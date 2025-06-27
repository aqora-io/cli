#[cfg(feature = "parquet-no-send")]
pub mod no_send_impl {
    pub use futures::future::LocalBoxFuture as BoxFuture;
    pub use futures::stream::LocalBoxStream as BoxStream;
    pub trait MaybeSend {}
    impl<T> MaybeSend for T {}
    pub fn boxed_fut<'a, F, T>(fut: F) -> BoxFuture<'a, T>
    where
        F: std::future::Future<Output = T> + 'a,
    {
        use futures::future::FutureExt;
        fut.boxed_local()
    }
    pub fn boxed_stream<'a, F, T>(fut: F) -> BoxStream<'a, T>
    where
        F: futures::stream::Stream<Item = T> + 'a,
    {
        use futures::stream::StreamExt;
        fut.boxed_local()
    }
}

#[cfg(not(feature = "parquet-no-send"))]
pub mod send_impl {
    pub use futures::future::BoxFuture;
    pub use futures::stream::BoxStream;
    pub use std::marker::Send as MaybeSend;
    pub fn boxed_fut<'a, F, T>(fut: F) -> BoxFuture<'a, T>
    where
        F: std::future::Future<Output = T> + Send + 'a,
    {
        use futures::future::FutureExt;
        fut.boxed()
    }
    pub fn boxed_stream<'a, F, T>(fut: F) -> BoxStream<'a, T>
    where
        F: futures::stream::Stream<Item = T> + Send + 'a,
    {
        use futures::stream::StreamExt;
        fut.boxed()
    }
}

#[cfg(feature = "parquet-no-send")]
pub use no_send_impl as parquet_async;
#[cfg(not(feature = "parquet-no-send"))]
pub use send_impl as parquet_async;
