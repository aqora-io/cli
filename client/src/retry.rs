use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use tower::retry::{Policy, Retry};
use tower::Layer;

use crate::async_util::{MaybeSend, MaybeSync};
use crate::http::Request;
use crate::sleep::{sleep, Sleep};

pub trait Backoff: Iterator<Item = Duration> + MaybeSend + MaybeSync {}
impl<T> Backoff for T where T: Iterator<Item = Duration> + MaybeSend + MaybeSync {}

pub trait BackoffBuilder: MaybeSend + MaybeSync {
    type Backoff: Backoff;
    fn build(&self) -> Self::Backoff;
}

impl<T> BackoffBuilder for Box<T>
where
    T: ?Sized + BackoffBuilder,
{
    type Backoff = T::Backoff;
    fn build(&self) -> Self::Backoff {
        T::build(self)
    }
}

#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    secs: f64,
    factor: f64,
    max_secs: Option<f64>,
    retries: usize,
    max_retries: Option<usize>,
}

impl Iterator for ExponentialBackoff {
    type Item = Duration;
    fn next(&mut self) -> Option<Self::Item> {
        if self
            .max_retries
            .is_some_and(|max_retries| self.retries >= max_retries)
        {
            return None;
        }
        let next = Duration::from_secs_f64(self.secs);
        self.secs *= self.factor;
        if let Some(max_secs) = self.max_secs {
            if self
                .secs
                .partial_cmp(&max_secs)
                .is_none_or(|ordering| ordering.is_gt())
            {
                self.secs = max_secs
            }
        }
        Some(next)
    }
}

#[derive(Debug, Clone)]
pub struct ExponentialBackoffBuilder {
    pub start_delay: Duration,
    pub factor: f64,
    pub max_delay: Option<Duration>,
    pub max_retries: Option<usize>,
}

impl Default for ExponentialBackoffBuilder {
    fn default() -> Self {
        Self {
            start_delay: Duration::from_secs(1),
            factor: 2.,
            max_delay: Some(Duration::from_secs(60)),
            max_retries: Some(5),
        }
    }
}

impl BackoffBuilder for ExponentialBackoffBuilder {
    type Backoff = ExponentialBackoff;
    fn build(&self) -> Self::Backoff {
        ExponentialBackoff {
            secs: self.start_delay.as_secs_f64(),
            factor: self.factor,
            max_secs: self.max_delay.map(|delay| delay.as_secs_f64()),
            retries: 0,
            max_retries: self.max_retries,
        }
    }
}

pub struct BackoffFn<F>(F);

impl<F, B> BackoffBuilder for BackoffFn<F>
where
    F: Fn() -> B + MaybeSend + MaybeSync,
    B: Backoff,
{
    type Backoff = B;
    fn build(&self) -> Self::Backoff {
        self.0()
    }
}

pub trait CloneBackoff: Backoff {
    fn clone_box(&self) -> Box<dyn CloneBackoff>;
}

impl<B> CloneBackoff for B
where
    B: Backoff + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn CloneBackoff> {
        Box::new(self.clone())
    }
}

pub struct BoxBackoff {
    inner: Box<dyn CloneBackoff>,
}

impl BoxBackoff {
    pub fn new<B>(inner: B) -> Self
    where
        B: Backoff + Clone + 'static,
    {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl Clone for BoxBackoff {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_box(),
        }
    }
}

impl Iterator for BoxBackoff {
    type Item = Duration;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub type BoxedBackoffBuilder = Box<dyn BackoffBuilder<Backoff = BoxBackoff>>;

pub trait BackoffBuilderExt {
    fn boxed(self) -> BoxedBackoffBuilder;
}

impl<B> BackoffBuilderExt for B
where
    B: BackoffBuilder + 'static,
    B::Backoff: Clone + 'static,
{
    fn boxed(self) -> BoxedBackoffBuilder {
        Box::new(BackoffFn(move || BoxBackoff::new(self.build())))
    }
}

pub trait RetryClassifier<Res, E> {
    fn should_retry(&self, result: &Result<Res, E>) -> bool;
}

impl<T, Res, E> RetryClassifier<Res, E> for Box<T>
where
    T: ?Sized + RetryClassifier<Res, E>,
{
    fn should_retry(&self, result: &Result<Res, E>) -> bool {
        T::should_retry(self, result)
    }
}

#[derive(Clone, Debug)]
pub struct RetryStatusCodeRange {
    range: RangeInclusive<u16>,
}

impl RetryStatusCodeRange {
    pub const fn new(range: RangeInclusive<u16>) -> Self {
        Self { range }
    }

    pub const fn for_client_and_server_errors() -> Self {
        RetryStatusCodeRange::new(400..=599)
    }
}

impl<B, E> RetryClassifier<http::Response<B>, E> for RetryStatusCodeRange {
    fn should_retry(&self, result: &Result<http::Response<B>, E>) -> bool {
        if let Ok(res) = result {
            self.range.contains(&res.status().as_u16())
        } else {
            true
        }
    }
}

pub struct BackoffPolicy<R, B> {
    retry_classifer: Arc<R>,
    backoff: B,
}

impl<R, B> Clone for BackoffPolicy<R, B>
where
    B: Clone,
{
    fn clone(&self) -> Self {
        Self {
            retry_classifer: self.retry_classifer.clone(),
            backoff: self.backoff.clone(),
        }
    }
}

impl<R, B> BackoffPolicy<R, B> {
    pub fn new(retry_classifer: R, backoff: B) -> Self {
        Self::new_arc(Arc::new(retry_classifer), backoff)
    }

    fn new_arc(retry_classifer: Arc<R>, backoff: B) -> Self {
        Self {
            backoff,
            retry_classifer,
        }
    }
}

impl<R, B, Res, E> Policy<Request, Res, E> for BackoffPolicy<R, B>
where
    B: Backoff,
    R: RetryClassifier<Res, E>,
{
    type Future = Sleep;
    fn retry(&mut self, _: &mut Request, res: &mut Result<Res, E>) -> Option<Self::Future> {
        if self.retry_classifer.should_retry(res) {
            self.backoff.next().map(sleep)
        } else {
            None
        }
    }
    fn clone_request(&mut self, req: &Request) -> Option<Request> {
        if let Some(body) = req.body().try_clone() {
            let mut builder = http::request::Builder::new()
                .method(req.method().clone())
                .uri(req.uri().clone())
                .version(req.version());
            *builder.headers_mut()? = req.headers().clone();
            *builder.extensions_mut()? = req.extensions().clone();
            builder.body(body).ok()
        } else {
            None
        }
    }
}

pub struct BackoffRetryLayer<R, B> {
    retry_classifier: Arc<R>,
    backoff_builder: Arc<B>,
}

impl<R, B> Clone for BackoffRetryLayer<R, B> {
    fn clone(&self) -> Self {
        Self {
            retry_classifier: self.retry_classifier.clone(),
            backoff_builder: self.backoff_builder.clone(),
        }
    }
}

impl<R, B> BackoffRetryLayer<R, B> {
    pub fn new(retry_classifier: R, backoff_builder: B) -> Self {
        Self {
            retry_classifier: Arc::new(retry_classifier),
            backoff_builder: Arc::new(backoff_builder),
        }
    }
}

impl<R, B, S> Layer<S> for BackoffRetryLayer<R, B>
where
    B: BackoffBuilder,
{
    type Service = Retry<BackoffPolicy<R, B::Backoff>, S>;
    fn layer(&self, inner: S) -> Self::Service {
        let backoff = self.backoff_builder.build();
        Retry::new(
            BackoffPolicy::new_arc(self.retry_classifier.clone(), backoff),
            inner,
        )
    }
}
