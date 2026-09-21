//! A small key-value store held in one JSON document on the aqora object
//! store. Mutations apply locally at once and a background task flushes them
//! with conditional writes, rebasing on whatever someone else wrote in the
//! meantime; reads resolve the last fetched document plus the pending
//! mutations, refetching once the snapshot is older than `stale_after`.
#![cfg_attr(not(feature = "extension-module"), allow(dead_code))]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use aqora_client::retry::{BackoffBuilder, ExponentialBackoffBuilder};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{Map, Value};
use tokio::{sync::Notify, time::Instant};
use tokio_util::task::AbortOnDropHandle;

use crate::{
    error::{self, Result},
    store::{Fetched, Precondition, PutError, Store},
};

type Doc = Map<String, Value>;

/// The object a [`Kv`] lives in, abstracted so the engine is testable offline.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn fetch(&self, if_none_match: Option<&str>) -> Result<Fetched>;
    async fn put(&self, body: Bytes, precondition: Precondition) -> Result<String, PutError>;
}

pub struct StoreObject {
    pub store: Arc<Store>,
    pub key: String,
}

#[async_trait]
impl Transport for StoreObject {
    async fn fetch(&self, if_none_match: Option<&str>) -> Result<Fetched> {
        self.store.get_object(&self.key, if_none_match).await
    }

    async fn put(&self, body: Bytes, precondition: Precondition) -> Result<String, PutError> {
        self.store
            .put_object(&self.key, body, "application/json", precondition)
            .await
    }
}

#[derive(Debug, Clone)]
enum Op {
    Set(String, Value),
    Delete(String),
}

struct Snapshot {
    doc: Doc,
    etag: Option<String>,
    fetched_at: Instant,
}

#[derive(Default)]
struct State {
    remote: Option<Snapshot>,
    pending: Vec<Op>,
    closed: bool,
    /// Why the last batch gave up, until the next flush attempt starts.
    failed: Option<String>,
    /// Bumped by every mutation, so a batch can tell whether it tried them all.
    generation: u64,
}

impl State {
    fn resolve(&self) -> Doc {
        let mut doc = self
            .remote
            .as_ref()
            .map(|snapshot| snapshot.doc.clone())
            .unwrap_or_default();
        apply(&mut doc, &self.pending);
        doc
    }

    /// Keep `snapshot` unless something newer landed since `started`.
    fn store(&mut self, snapshot: Snapshot, started: Instant) {
        if self
            .remote
            .as_ref()
            .is_none_or(|current| current.fetched_at <= started)
        {
            self.remote = Some(snapshot);
        }
    }
}

enum FlushError {
    Conflict,
    /// Worth retrying with backoff.
    Transient(String),
    /// Retrying cannot help; wait for the next mutation or `flush`.
    Permanent(String),
}

impl From<error::Error> for FlushError {
    fn from(error: error::Error) -> Self {
        Self::Transient(error.description())
    }
}

impl From<PutError> for FlushError {
    fn from(error: PutError) -> Self {
        match error {
            PutError::Conflict => Self::Conflict,
            PutError::Other(error) => error.into(),
        }
    }
}

fn apply(doc: &mut Doc, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Set(key, value) => {
                doc.insert(key.clone(), value.clone());
            }
            Op::Delete(key) => {
                doc.remove(key);
            }
        }
    }
}

fn parse(body: &[u8]) -> Result<Doc> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Doc::new());
    }
    match serde_json::from_slice::<Value>(body)? {
        Value::Object(doc) => Ok(doc),
        other => Err(error::user(
            &format!("The KV document is not a JSON object but {other}"),
            "Point the KV at a different path or fix the object by hand",
        )),
    }
}

struct Inner {
    transport: Arc<dyn Transport>,
    state: Mutex<State>,
    /// Wakes the flusher.
    wake: Notify,
    /// Wakes `flush` waiters after every batch outcome.
    settled: Notify,
    /// Coalesces concurrent refetches.
    refetch: tokio::sync::Mutex<()>,
    stale_after: Duration,
}

pub struct Kv {
    inner: Arc<Inner>,
    _flusher: AbortOnDropHandle<()>,
}

impl Kv {
    pub fn new(
        transport: Arc<dyn Transport>,
        stale_after: Duration,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let inner = Arc::new(Inner {
            transport,
            state: Mutex::new(State::default()),
            wake: Notify::new(),
            settled: Notify::new(),
            refetch: tokio::sync::Mutex::new(()),
            stale_after,
        });
        let flusher = AbortOnDropHandle::new(runtime.spawn(Arc::clone(&inner).run()));
        Self {
            inner,
            _flusher: flusher,
        }
    }

    pub fn set(&self, key: &str, value: Value) -> Result<()> {
        self.push(Op::Set(key.to_owned(), value))
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        self.push(Op::Delete(key.to_owned()))
    }

    fn push(&self, op: Op) -> Result<()> {
        let mut state = self.inner.lock();
        if state.closed {
            return Err(error::user(
                "The KV is closed",
                "Create a new KV to keep writing",
            ));
        }
        state.pending.push(op);
        state.failed = None;
        state.generation += 1;
        drop(state);
        self.inner.wake.notify_one();
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.inner.ensure_fresh().await?;
        Ok(self.inner.lock().resolve().get(key).cloned())
    }

    pub async fn list(&self) -> Result<Vec<String>> {
        self.inner.ensure_fresh().await?;
        let mut keys: Vec<String> = self.inner.lock().resolve().keys().cloned().collect();
        keys.sort();
        Ok(keys)
    }

    /// Wait until every pending mutation is written, or the flusher gives up
    /// on them; they stay pending either way and a later call retries.
    pub async fn flush(&self) -> Result<()> {
        self.inner.lock().failed = None;
        self.inner.wake.notify_one();
        loop {
            let settled = self.inner.settled.notified();
            tokio::pin!(settled);
            settled.as_mut().enable();
            {
                let state = self.inner.lock();
                if state.pending.is_empty() {
                    return Ok(());
                }
                if let Some(message) = &state.failed {
                    return Err(error::system(
                        &format!("Flushing the KV failed: {message}"),
                        "Call flush() again to retry the pending writes",
                    ));
                }
            }
            settled.await;
        }
    }

    /// Flush, then refuse further mutations and stop the background task.
    pub async fn close(&self) -> Result<()> {
        self.inner.lock().closed = true;
        self.flush().await
    }
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn stale(&self) -> (bool, Option<String>) {
        let state = self.lock();
        match &state.remote {
            None => (true, None),
            Some(snapshot) => (
                snapshot.fetched_at.elapsed() > self.stale_after,
                snapshot.etag.clone(),
            ),
        }
    }

    async fn ensure_fresh(&self) -> Result<()> {
        if !self.stale().0 {
            return Ok(());
        }
        let _refetching = self.refetch.lock().await;
        let (stale, etag) = self.stale();
        if !stale {
            return Ok(());
        }
        let started = Instant::now();
        match self.transport.fetch(etag.as_deref()).await? {
            Fetched::Changed { body, etag } => {
                let doc = parse(&body)?;
                self.lock().store(
                    Snapshot {
                        doc,
                        etag: Some(etag),
                        fetched_at: Instant::now(),
                    },
                    started,
                );
            }
            Fetched::Missing => self.lock().store(
                Snapshot {
                    doc: Doc::new(),
                    etag: None,
                    fetched_at: Instant::now(),
                },
                started,
            ),
            Fetched::NotModified => {
                let mut state = self.lock();
                if let Some(snapshot) = state.remote.as_mut().filter(|s| s.etag == etag) {
                    snapshot.fetched_at = Instant::now();
                }
            }
        }
        Ok(())
    }

    async fn run(self: Arc<Self>) {
        loop {
            loop {
                let wake = self.wake.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                {
                    let state = self.lock();
                    if !state.pending.is_empty() && state.failed.is_none() {
                        break;
                    }
                    if state.closed && state.pending.is_empty() {
                        return;
                    }
                }
                wake.await;
            }
            self.flush_batch().await;
        }
    }

    /// Write the pending mutations on top of the latest document, retrying
    /// with backoff; conflicts refetch and rebase, the first one at once.
    async fn flush_batch(&self) {
        let mut backoff = ExponentialBackoffBuilder::default().build();
        let mut conflicts = 0;
        let mut attempted = self.lock().generation;
        loop {
            let message = match self.try_flush(&mut attempted).await {
                Ok(()) => {
                    self.settled.notify_waiters();
                    return;
                }
                Err(FlushError::Conflict) => {
                    self.lock().remote = None;
                    conflicts += 1;
                    if conflicts == 1 {
                        continue;
                    }
                    "conflict".to_owned()
                }
                Err(FlushError::Transient(message)) => message,
                Err(FlushError::Permanent(message)) => return self.give_up(message, attempted),
            };
            match backoff.next() {
                Some(delay) => {
                    tracing::debug!("KV flush failed ({message}), retrying in {delay:?}");
                    tokio::time::sleep(delay).await;
                }
                None => return self.give_up(message, attempted),
            }
        }
    }

    /// Leave the batch pending until a mutation or `flush` restarts it, unless
    /// a mutation already arrived after the last attempt: then the next batch
    /// starts at once so that it gets tried too.
    fn give_up(&self, message: String, attempted: u64) {
        tracing::warn!("KV flush gave up: {message}");
        let mut state = self.lock();
        if state.generation == attempted {
            state.failed = Some(message);
        }
        drop(state);
        self.settled.notify_waiters();
    }

    /// One attempt; `attempted` is set to the generation of mutations it covers.
    async fn try_flush(&self, attempted: &mut u64) -> Result<(), FlushError> {
        // Held through the commit so that a reader cannot fetch a newer
        // document in the meantime and have this attempt's result replace it.
        let _refetching = self.refetch.lock().await;
        let started = Instant::now();
        let base = {
            let state = self.lock();
            state
                .remote
                .as_ref()
                .map(|snapshot| (snapshot.doc.clone(), snapshot.etag.clone()))
        };
        let (base_doc, base_etag) = match base {
            Some(base) => base,
            None => {
                let (doc, etag) = match self.transport.fetch(None).await? {
                    Fetched::Changed { body, etag } => (
                        parse(&body).map_err(|error| FlushError::Permanent(error.description()))?,
                        Some(etag),
                    ),
                    Fetched::Missing | Fetched::NotModified => (Doc::new(), None),
                };
                self.lock().store(
                    Snapshot {
                        doc: doc.clone(),
                        etag: etag.clone(),
                        fetched_at: Instant::now(),
                    },
                    started,
                );
                (doc, etag)
            }
        };
        let ops = {
            let state = self.lock();
            *attempted = state.generation;
            state.pending.clone()
        };
        let mut doc = base_doc;
        apply(&mut doc, &ops);
        let body = Bytes::from(
            serde_json::to_vec(&doc).map_err(|error| FlushError::Permanent(error.to_string()))?,
        );
        let precondition = match base_etag {
            Some(etag) => Precondition::IfMatch(etag),
            None => Precondition::IfNoneMatchAny,
        };
        let etag = self.transport.put(body, precondition).await?;
        let mut state = self.lock();
        state.pending.drain(..ops.len());
        state.remote = Some(Snapshot {
            doc,
            etag: Some(etag),
            fetched_at: Instant::now(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct Remote {
        object: Option<(Vec<u8>, String)>,
        version: u64,
        fetches: Vec<Option<String>>,
        puts: Vec<Precondition>,
        conflicts: u32,
        failures: u32,
        put_failures: u32,
        put_delay: Duration,
    }

    #[derive(Default)]
    struct Fake(Mutex<Remote>);

    impl Fake {
        fn write(&self, doc: Value) {
            let mut remote = self.0.lock().unwrap();
            remote.version += 1;
            let etag = format!("\"v{}\"", remote.version);
            remote.object = Some((serde_json::to_vec(&doc).unwrap(), etag));
        }

        fn read(&self) -> Value {
            let remote = self.0.lock().unwrap();
            serde_json::from_slice(&remote.object.as_ref().unwrap().0).unwrap()
        }

        fn with(&self, f: impl FnOnce(&mut Remote)) {
            f(&mut self.0.lock().unwrap());
        }
    }

    #[async_trait]
    impl Transport for Fake {
        async fn fetch(&self, if_none_match: Option<&str>) -> Result<Fetched> {
            let mut remote = self.0.lock().unwrap();
            remote.fetches.push(if_none_match.map(str::to_owned));
            if remote.failures > 0 {
                remote.failures -= 1;
                return Err(error::system("fetch failed", ""));
            }
            Ok(match &remote.object {
                None => Fetched::Missing,
                Some((_, etag)) if Some(etag.as_str()) == if_none_match => Fetched::NotModified,
                Some((body, etag)) => Fetched::Changed {
                    body: Bytes::from(body.clone()),
                    etag: etag.clone(),
                },
            })
        }

        /// Applies (or fails) at once, then takes `put_delay` to answer, so
        /// the server-side effect precedes the client seeing the result.
        async fn put(&self, body: Bytes, precondition: Precondition) -> Result<String, PutError> {
            let (result, delay) = {
                let mut remote = self.0.lock().unwrap();
                remote.puts.push(precondition.clone());
                let result = if remote.failures > 0 || remote.put_failures > 0 {
                    if remote.failures > 0 {
                        remote.failures -= 1;
                    } else {
                        remote.put_failures -= 1;
                    }
                    Err(PutError::Other(error::system("put failed", "")))
                } else if remote.conflicts > 0 {
                    remote.conflicts -= 1;
                    Err(PutError::Conflict)
                } else {
                    let current = remote.object.as_ref().map(|(_, etag)| etag.as_str());
                    let ok = match &precondition {
                        Precondition::Any => true,
                        Precondition::IfNoneMatchAny => current.is_none(),
                        Precondition::IfMatch(etag) => current == Some(etag.as_str()),
                    };
                    if ok {
                        remote.version += 1;
                        let etag = format!("\"v{}\"", remote.version);
                        remote.object = Some((body.to_vec(), etag.clone()));
                        Ok(etag)
                    } else {
                        Err(PutError::Conflict)
                    }
                };
                (result, remote.put_delay)
            };
            tokio::time::sleep(delay).await;
            result
        }
    }

    fn kv(fake: &Arc<Fake>, stale_after: Duration) -> Kv {
        Kv::new(
            fake.clone() as Arc<dyn Transport>,
            stale_after,
            &tokio::runtime::Handle::current(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn writes_are_visible_before_and_after_flushing() {
        let fake = Arc::new(Fake::default());
        let kv = kv(&fake, Duration::from_secs(60));
        kv.set("a", json!(1)).unwrap();
        kv.set("b", json!({"x": [1, 2]})).unwrap();
        assert_eq!(kv.get("a").await.unwrap(), Some(json!(1)));
        kv.flush().await.unwrap();
        assert_eq!(fake.read(), json!({"a": 1, "b": {"x": [1, 2]}}));
        assert!(matches!(
            fake.0.lock().unwrap().puts[0],
            Precondition::IfNoneMatchAny
        ));
        kv.delete("a").unwrap();
        kv.set("c", json!("c")).unwrap();
        assert_eq!(kv.list().await.unwrap(), vec!["b", "c"]);
        kv.flush().await.unwrap();
        assert_eq!(fake.read(), json!({"b": {"x": [1, 2]}, "c": "c"}));
        assert!(
            matches!(&fake.0.lock().unwrap().puts[1], Precondition::IfMatch(etag) if etag == "\"v1\"")
        );
        assert_eq!(kv.get("a").await.unwrap(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn conflicts_rebase_on_the_latest_document() {
        let fake = Arc::new(Fake::default());
        fake.write(json!({"theirs": 1}));
        let kv = kv(&fake, Duration::from_secs(60));
        assert_eq!(kv.get("theirs").await.unwrap(), Some(json!(1)));
        fake.write(json!({"theirs": 2}));
        kv.set("mine", json!(true)).unwrap();
        let before = tokio::time::Instant::now();
        kv.flush().await.unwrap();
        assert_eq!(fake.read(), json!({"theirs": 2, "mine": true}));
        assert_eq!(fake.0.lock().unwrap().puts.len(), 2);
        assert_eq!(
            before.elapsed(),
            Duration::ZERO,
            "the first conflict rebases at once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_snapshots_are_refetched_conditionally() {
        let fake = Arc::new(Fake::default());
        fake.write(json!({"k": 1}));
        let kv = kv(&fake, Duration::ZERO);
        assert_eq!(kv.get("k").await.unwrap(), Some(json!(1)));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(kv.get("k").await.unwrap(), Some(json!(1)));
        assert_eq!(
            fake.0.lock().unwrap().fetches,
            vec![None, Some("\"v1\"".to_owned())]
        );
        fake.write(json!({"k": 2}));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(kv.get("k").await.unwrap(), Some(json!(2)));

        let fresh = kv_fresh(&fake).await;
        assert_eq!(fresh.get("k").await.unwrap(), Some(json!(2)));
        let before = fake.0.lock().unwrap().fetches.len();
        fresh.get("k").await.unwrap();
        assert_eq!(fake.0.lock().unwrap().fetches.len(), before);
    }

    async fn kv_fresh(fake: &Arc<Fake>) -> Kv {
        kv(fake, Duration::from_secs(3600))
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_retries_fail_flush_but_keep_the_writes() {
        let fake = Arc::new(Fake::default());
        fake.with(|remote| remote.failures = 100);
        let kv = kv(&fake, Duration::from_secs(60));
        kv.set("a", json!(1)).unwrap();
        let error = kv.flush().await.unwrap_err();
        assert!(error.message().contains("failed"), "{error}");
        assert!(fake.0.lock().unwrap().object.is_none());
        // Having given up, the flusher waits instead of hammering the store.
        let attempts = fake.0.lock().unwrap().fetches.len();
        tokio::time::advance(Duration::from_secs(600)).await;
        assert_eq!(fake.0.lock().unwrap().fetches.len(), attempts);
        fake.with(|remote| remote.failures = 0);
        kv.set("b", json!(2)).unwrap();
        kv.flush().await.unwrap();
        assert_eq!(fake.read(), json!({"a": 1, "b": 2}));
    }

    #[tokio::test(start_paused = true)]
    async fn a_mutation_during_the_last_attempt_restarts_the_batch() {
        let fake = Arc::new(Fake::default());
        // Puts take a second and fail: attempts run at 0-1, 2-3, 5-6, 10-11,
        // 19-20 and 36-37 seconds, after which the batch would give up with
        // `b` never tried.
        fake.with(|remote| {
            remote.put_delay = Duration::from_secs(1);
            remote.put_failures = 6;
        });
        let kv = Arc::new(kv(&fake, Duration::from_secs(60)));
        kv.set("a", json!(1)).unwrap();
        let flush = tokio::spawn({
            let kv = kv.clone();
            async move { kv.flush().await }
        });
        tokio::time::sleep(Duration::from_millis(36_500)).await;
        kv.set("b", json!(2)).unwrap();
        flush.await.unwrap().unwrap();
        assert_eq!(fake.read(), json!({"a": 1, "b": 2}));
    }

    #[tokio::test(start_paused = true)]
    async fn a_refetch_cannot_be_overwritten_by_an_older_flush() {
        let fake = Arc::new(Fake::default());
        fake.write(json!({}));
        fake.with(|remote| remote.put_delay = Duration::from_secs(1));
        let kv = Arc::new(kv(&fake, Duration::from_millis(300)));
        assert_eq!(kv.get("theirs").await.unwrap(), None);
        kv.set("a", json!(1)).unwrap();
        let flush = tokio::spawn({
            let kv = kv.clone();
            async move { kv.flush().await }
        });
        // The put has applied on the server but not answered yet; someone
        // else writes on top of it and a stale reader asks for the document.
        tokio::time::sleep(Duration::from_millis(500)).await;
        fake.write(json!({"a": 1, "theirs": true}));
        let during = kv.get("theirs").await.unwrap();
        flush.await.unwrap().unwrap();
        let after = kv.get("theirs").await.unwrap();
        assert!(
            !(during.is_some() && after.is_none()),
            "a read went backwards: {during:?} then {after:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_document_that_is_not_an_object_fails_without_retrying() {
        let fake = Arc::new(Fake::default());
        fake.write(json!([1, 2]));
        let kv = kv(&fake, Duration::from_secs(60));
        kv.set("a", json!(1)).unwrap();
        let error = kv.flush().await.unwrap_err();
        assert!(error.message().contains("not a JSON object"), "{error}");
        assert_eq!(fake.0.lock().unwrap().fetches.len(), 1);
        assert!(fake.0.lock().unwrap().puts.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn close_drains_and_rejects_further_writes() {
        let fake = Arc::new(Fake::default());
        let kv = kv(&fake, Duration::from_secs(60));
        kv.set("a", json!(1)).unwrap();
        kv.close().await.unwrap();
        assert_eq!(fake.read(), json!({"a": 1}));
        assert!(kv.set("b", json!(2)).is_err());
    }

    #[test]
    fn documents_must_be_objects() {
        assert_eq!(parse(b" ").unwrap(), Doc::new());
        assert_eq!(
            parse(br#"{"a":1}"#).unwrap(),
            json!({"a": 1}).as_object().cloned().unwrap()
        );
        assert!(parse(b"[1]").is_err());
    }
}
