#[cfg(not(feature = "threaded"))]
mod async_impl {
    pub trait MaybeSend {}
    impl<T: ?Sized> MaybeSend for T {}
    pub trait MaybeSync {}
    impl<T: ?Sized> MaybeSync for T {}
    pub type MaybeLocalBoxFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;
    pub type MaybeLocalBoxStream<'a, T> = futures::stream::LocalBoxStream<'a, T>;
}

#[cfg(feature = "threaded")]
mod async_impl {
    pub use std::marker::Send as MaybeSend;
    pub use std::marker::Sync as MaybeSync;
    pub type MaybeLocalBoxFuture<'a, T> = futures::future::BoxFuture<'a, T>;
    pub type MaybeLocalBoxStream<'a, T> = futures::stream::BoxStream<'a, T>;
}

pub use async_impl::*;

pub trait MaybeLocalFutureExt: std::future::Future {
    fn boxed_maybe_local<'a>(self) -> MaybeLocalBoxFuture<'a, Self::Output>
    where
        Self: Sized + MaybeSend + 'a,
    {
        Box::pin(self)
    }
}

impl<T> MaybeLocalFutureExt for T where T: std::future::Future {}

pub trait MaybeLocalStreamExt: futures::stream::Stream {
    fn boxed_maybe_local<'a>(self) -> MaybeLocalBoxStream<'a, Self::Item>
    where
        Self: Sized + MaybeSend + 'a,
    {
        Box::pin(self)
    }
}

impl<T> MaybeLocalStreamExt for T where T: futures::stream::Stream {}
