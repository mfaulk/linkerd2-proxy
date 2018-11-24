use bytes::IntoBuf;
use futures::{Async, Future, Poll, Stream};
use h2;
use http;
use std::collections::VecDeque;
use std::sync::Weak;
use tower_h2::Body as Payload;

use super::iface::{Register, Tap, TapBody, TapResponse};
use super::Inspect;
use proxy::http::HasH2Reason;
use svc;

/// A stack module that wraps services to record taps.
#[derive(Clone, Debug)]
pub struct Layer<R: Register> {
    registry: R,
}

/// Wraps services to record taps.
#[derive(Clone, Debug)]
pub struct Stack<R: Register, T> {
    registry: R,
    inner: T,
}

/// A middleware that records HTTP taps.
#[derive(Clone, Debug)]
pub struct Service<I, R, S, T> {
    tap_rx: R,
    taps: VecDeque<Weak<S>>,
    inner: T,
    inspect: I,
}

#[derive(Debug, Clone)]
pub struct ResponseFuture<F, T: TapResponse> {
    inner: F,
    taps: VecDeque<T>,
}

#[derive(Debug)]
pub struct Body<B: Payload, T: TapBody> {
    inner: B,
    taps: VecDeque<T>,
}

// === Layer ===

impl<R> Layer<R>
where
    R: Register + Clone,
{
    pub(super) fn new(registry: R) -> Self {
        Layer { registry }
    }
}

impl<R, T, M> svc::Layer<T, T, M> for Layer<R>
where
    T: Inspect + Clone,
    R: Register + Clone,
    M: svc::Stack<T>,
{
    type Value = <Stack<R, M> as svc::Stack<T>>::Value;
    type Error = M::Error;
    type Stack = Stack<R, M>;

    fn bind(&self, inner: M) -> Self::Stack {
        Stack {
            inner,
            registry: self.registry.clone(),
        }
    }
}

// === Stack ===

impl<R, T, M> svc::Stack<T> for Stack<R, M>
where
    T: Inspect + Clone,
    R: Register + Clone,
    M: svc::Stack<T>,
{
    type Value = Service<T, R::Taps, R::Tap, M::Value>;
    type Error = M::Error;

    fn make(&self, target: &T) -> Result<Self::Value, Self::Error> {
        let inner = self.inner.make(&target)?;
        let tap_rx = self.registry.clone().register();
        Ok(Service {
            inner,
            tap_rx,
            taps: VecDeque::default(),
            inspect: target.clone(),
        })
    }
}

// === Service ===

impl<I, R, S, T, A, B> svc::Service<http::Request<A>> for Service<I, R, S, T>
where
    I: Inspect,
    R: Stream<Item = Weak<S>>,
    S: Tap,
    T: svc::Service<http::Request<Body<A, S::TapRequestBody>>, Response = http::Response<B>>,
    T::Error: HasH2Reason,
    A: Payload,
    B: Payload,
{
    type Response = http::Response<Body<B, S::TapResponseBody>>;
    type Error = T::Error;
    type Future = ResponseFuture<T::Future, S::TapResponse>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        while let Ok(Async::Ready(Some(s))) = self.tap_rx.poll() {
            self.taps.push_back(s);
        }
        self.taps
            .retain(|t| t.upgrade().map(|t| t.can_tap_more()).unwrap_or(false));
        self.inner.poll_ready()
    }

    fn call(&mut self, req: http::Request<A>) -> Self::Future {
        let mut req_taps = VecDeque::with_capacity(self.taps.len());
        let mut rsp_taps = VecDeque::with_capacity(self.taps.len());
        for t in self.taps.iter().filter_map(Weak::upgrade) {
            if let Some((req_tap, rsp_tap)) = t.tap(&req, &self.inspect) {
                req_taps.push_back(req_tap);
                rsp_taps.push_back(rsp_tap);
            }
        }

        let req = {
            let (head, inner) = req.into_parts();
            let body = Body {
                inner,
                taps: req_taps,
            };
            http::Request::from_parts(head, body)
        };

        let inner = self.inner.call(req);
        ResponseFuture {
            inner,
            taps: rsp_taps,
        }
    }
}

impl<B, F, T> Future for ResponseFuture<F, T>
where
    B: Payload,
    F: Future<Item = http::Response<B>>,
    F::Error: HasH2Reason,
    T: TapResponse,
{
    type Item = http::Response<Body<B, T::TapBody>>;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let rsp = try_ready!(self.inner.poll().map_err(|e| self.err(e)));

        let taps = self.taps.drain(..).map(|t| t.tap(&rsp)).collect();
        let rsp = {
            let (head, inner) = rsp.into_parts();
            let mut body = Body { inner, taps };
            if body.is_end_stream() {
                body.eos(None);
            }
            http::Response::from_parts(head, body)
        };

        Ok(rsp.into())
    }
}

impl<B, F, T> ResponseFuture<F, T>
where
    B: Payload,
    F: Future<Item = http::Response<B>>,
    F::Error: HasH2Reason,
    T: TapResponse,
{
    fn err(&mut self, error: F::Error) -> F::Error {
        while let Some(tap) = self.taps.pop_front() {
            tap.fail(&error);
        }

        error
    }
}

// === Body ===

impl<B: Payload + Default, T: TapBody> Default for Body<B, T> {
    fn default() -> Self {
        Self {
            inner: B::default(),
            taps: VecDeque::default(),
        }
    }
}

impl<B: Payload, T: TapBody> Payload for Body<B, T> {
    type Data = <B::Data as IntoBuf>::Buf;

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, h2::Error> {
        let poll_frame = self.inner.poll_data().map_err(|e| self.err(e));
        let frame = try_ready!(poll_frame).map(|f| f.into_buf());
        self.data(frame.as_ref());
        Ok(Async::Ready(frame))
    }

    fn poll_trailers(&mut self) -> Poll<Option<http::HeaderMap>, h2::Error> {
        let trailers = try_ready!(self.inner.poll_trailers().map_err(|e| self.err(e)));
        self.eos(trailers.as_ref());
        Ok(Async::Ready(trailers))
    }
}

impl<B: Payload, T: TapBody> Body<B, T> {
    fn data(&mut self, frame: Option<&<B::Data as IntoBuf>::Buf>) {
        trace!("tapping data frame");
        if let Some(ref f) = frame {
            for ref mut tap in self.taps.iter_mut() {
                tap.data::<<B::Data as IntoBuf>::Buf>(f);
            }
        }

        if self.inner.is_end_stream() {
            self.eos(None);
        }
    }

    fn eos(&mut self, trailers: Option<&http::HeaderMap>) {
        trace!("tapping eos");
        for tap in self.taps.drain(..) {
            tap.eos(trailers);
        }
    }

    fn err(&mut self, error: h2::Error) -> h2::Error {
        trace!("tapping error: {}", error);
        for tap in self.taps.drain(..) {
            tap.fail(&error);
        }

        error
    }
}

impl<B: Payload, T: TapBody> Drop for Body<B, T> {
    fn drop(&mut self) {
        trace!("dropping tap body");
        // TODO this should be recorded as a cancelation if the stream didn't end.
        self.eos(None);
    }
}
