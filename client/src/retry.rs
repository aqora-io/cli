use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Request, Response};

use crate::async_util::{MaybeSend, MaybeSync};
use crate::middleware::{Middleware, MiddlewareError, Next};
use crate::sleep::sleep;

pub trait Backoff: Iterator<Item = Duration> + MaybeSend + MaybeSync {}
impl<T> Backoff for T where T: Iterator<Item = Duration> + MaybeSend + MaybeSync {}

pub trait BackoffBuilder: MaybeSend + MaybeSync {
    type Iter: Backoff;
    fn build(&self) -> Self::Iter;
}

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
    type Iter = ExponentialBackoff;
    fn build(&self) -> Self::Iter {
        ExponentialBackoff {
            secs: self.start_delay.as_secs_f64(),
            factor: self.factor,
            max_secs: self.max_delay.map(|delay| delay.as_secs_f64()),
            retries: 0,
            max_retries: self.max_retries,
        }
    }
}

pub struct RetryMiddleware<T>(T);

impl<T> RetryMiddleware<T> {
    pub fn new(backoff: T) -> Self {
        Self(backoff)
    }
}

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
impl<T> Middleware for RetryMiddleware<T>
where
    T: BackoffBuilder,
{
    async fn handle(&self, req: Request, next: Next<'_>) -> Result<Response, MiddlewareError> {
        let cloned = req.try_clone();
        match next.handle(req).await {
            Ok(res) => Ok(res),
            Err(err) => {
                let Some(req) = cloned else {
                    return Err(MiddlewareError::Middleware(
                        "Could not clone request to retry".into(),
                    ));
                };
                for delay in self.0.build() {
                    sleep(delay).await?;
                    if let Ok(res) = next.handle(req.try_clone().unwrap()).await {
                        return Ok(res);
                    }
                }
                Err(err)
            }
        }
    }
}
