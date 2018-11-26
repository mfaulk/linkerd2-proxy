use bytes::IntoBuf;
use futures::{future, Async, Future, Poll, Stream};
use h2;
use http;
use std::collections::VecDeque;
use tower_h2::Body as Payload;

use super::iface::{Register, Tap, TapBody, TapRequest, TapResponse};
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
pub struct Service<I, R, T, S> {
    tap_rx: R,
    taps: VecDeque<T>,
    inner: S,
    inspect: I,
}

pub enum ResponseFuture<
    I: Inspect,
    T: Tap,
    A: Payload,
    S: svc::Service<http::Request<Body<A, T::TapRequestBody>>>,
> {
    Taps {
        taps: future::JoinAll<VecDeque<T::Future>>,
        inspect: I,
        request: Option<http::Request<A>>,
        service: S,
    },
    Call {
        taps: VecDeque<T::TapResponse>,
        call: S::Future,
    },
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
        Self { registry }
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

impl<I, R, S, T, A, B> svc::Service<http::Request<A>> for Service<I, R, T, S>
where
    I: Inspect + Clone,
    R: Stream<Item = T>,
    T: Tap,
    S: svc::Service<http::Request<Body<A, T::TapRequestBody>>, Response = http::Response<B>>
        + Clone,
    S::Error: HasH2Reason,
    A: Payload,
    B: Payload,
{
    type Response = http::Response<Body<B, T::TapResponseBody>>;
    type Error = S::Error;
    type Future = ResponseFuture<I, T, A, S>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        while let Ok(Async::Ready(Some(t))) = self.tap_rx.poll() {
            self.taps.push_back(t);
        }

        self.taps.retain(|t| t.can_tap_more());
        self.inner.poll_ready()
    }

    fn call(&mut self, req: http::Request<A>) -> Self::Future {
        let mut taps = VecDeque::with_capacity(self.taps.len());
        for t in self.taps.iter_mut() {
            if t.matches(&req, &self.inspect) {
                taps.push_back(t.tap());
            }
        }

        ResponseFuture::Taps {
            taps: future::join_all(taps),
            request: Some(req),
            service: self.inner.clone(),
            inspect: self.inspect.clone(),
        }
    }
}

impl<A, B, I, T, S> Future for ResponseFuture<I, T, A, S>
where
    A: Payload,
    B: Payload,
    I: Inspect,
    T: Tap,
    S: svc::Service<http::Request<Body<A, T::TapRequestBody>>, Response = http::Response<B>>,
    S::Error: HasH2Reason,
{
    type Item = http::Response<Body<B, T::TapResponseBody>>;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            *self = match self {
                ResponseFuture::Taps {
                    request,
                    service,
                    taps,
                    inspect,
                } => {
                    let taps = match taps.poll() {
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Ok(Async::Ready(taps)) => taps,
                        Err(_) => Vec::new(),
                    };

                    let req = request.take().expect("request must be set");

                    let mut req_taps = VecDeque::with_capacity(taps.len());
                    let mut rsp_taps = VecDeque::with_capacity(taps.len());
                    for tap in taps.into_iter().filter_map(|t| t) {
                        let (req, rsp) = tap.open(&req, inspect);
                        req_taps.push_back(req);
                        rsp_taps.push_back(rsp);
                    }

                    let req = {
                        let (head, inner) = req.into_parts();
                        let body = Body {
                            inner,
                            taps: req_taps,
                        };
                        http::Request::from_parts(head, body)
                    };

                    let call = service.call(req);

                    ResponseFuture::Call {
                        call,
                        taps: rsp_taps,
                    }
                }
                ResponseFuture::Call { call, taps } => {
                    return match call.poll() {
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Ok(Async::Ready(rsp)) => {
                            let taps = taps.drain(..).map(|t| t.tap(&rsp)).collect();
                            let (head, inner) = rsp.into_parts();
                            let mut body = Body { inner, taps };
                            if body.is_end_stream() {
                                body.eos(None);
                            }
                            Ok(Async::Ready(http::Response::from_parts(head, body)))
                        }
                        Err(e) => {
                            for tap in taps.drain(..) {
                                tap.fail(&e);
                            }
                            Err(e)
                        }
                    };
                }
            };
        }
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
        for tap in self.taps.drain(..) {
            tap.eos(trailers);
        }
    }

    fn err(&mut self, error: h2::Error) -> h2::Error {
        for tap in self.taps.drain(..) {
            tap.fail(&error);
        }

        error
    }
}

impl<B: Payload, T: TapBody> Drop for Body<B, T> {
    fn drop(&mut self) {
        // TODO this should be recorded as a cancelation if the stream didn't end.
        self.eos(None);
    }
}
