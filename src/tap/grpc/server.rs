use bytes::Buf;
use futures::sync::{mpsc, oneshot};
use futures::{future, Async, Future, Poll, Stream};
use http::HeaderMap;
use never::Never;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tokio_timer::clock;
use tower_grpc::{self as grpc, Response};
use tower_h2::Body as Payload;

use api::{http_types, pb_duration, tap as api};

use super::match_::Match;
use proxy::http::HasH2Reason;
use tap::{iface, Inspect};

// Buffer ~100 req/rsp pairs' worth of events per tap request.
const TAP_BUFFER_CAPACITY: usize = 100;

// Buffer ~100 req/rsp pairs' worth of events per tap request.
const PER_REQUEST_BUFFER_CAPACITY: usize = 400;

#[derive(Clone, Debug)]
pub struct Server<T> {
    subscribe: T,
    base_id: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct ResponseStream {
    dispatch: Option<Dispatch>,
    events_rx: mpsc::Receiver<api::TapEvent>,
}

#[derive(Debug)]
struct Dispatch {
    base_id: u32,
    count: usize,
    limit: usize,
    taps_rx: mpsc::Receiver<oneshot::Sender<TapTx>>,
    events_tx: mpsc::Sender<api::TapEvent>,
    _match: Arc<Match>,
}

#[derive(Clone, Debug)]
struct TapTx {
    id: api::tap_event::http::StreamId,
    tx: mpsc::Sender<api::TapEvent>,
}

#[derive(Clone, Debug)]
pub struct Tap {
    match_: Weak<Match>,
    taps_tx: mpsc::Sender<oneshot::Sender<TapTx>>,
}

#[derive(Debug)]
pub struct TapFuture(Fut);

#[derive(Debug)]
enum Fut {
    Init {
        request_init_at: Instant,
        taps_tx: mpsc::Sender<oneshot::Sender<TapTx>>
    },
    Pending {
        request_init_at: Instant,
        rx: oneshot::Receiver<TapTx>,
    },
}

#[derive(Debug)]
pub struct TapRequest {
    request_init_at: Instant,
    tap: TapTx,
}

#[derive(Debug)]
pub struct TapResponse {
    base_event: api::TapEvent,
    request_init_at: Instant,
    tap: TapTx,
}

#[derive(Debug)]
pub struct TapRequestBody {
    base_event: api::TapEvent,
    tap: TapTx,
}

#[derive(Debug)]
pub struct TapResponseBody {
    base_event: api::TapEvent,
    request_init_at: Instant,
    response_init_at: Instant,
    response_bytes: usize,
    tap: TapTx,
}

impl<T: iface::Subscribe<Tap>> Server<T> {
    pub(in tap) fn new(subscribe: T) -> Self {
        let base_id = Arc::new(0.into());
        Self { base_id, subscribe }
    }

    fn invalid_arg(event: http::header::HeaderValue) -> grpc::Error {
        let status = grpc::Status::with_code(grpc::Code::InvalidArgument);
        let mut headers = HeaderMap::new();
        headers.insert("grpc-message", event);
        grpc::Error::Grpc(status, headers)
    }
}

impl<T> api::server::Tap for Server<T>
where
    T: iface::Subscribe<Tap> + Clone,
{
    type ObserveStream = ResponseStream;
    type ObserveFuture = future::FutureResult<Response<Self::ObserveStream>, grpc::Error>;

    fn observe(&mut self, req: grpc::Request<api::ObserveRequest>) -> Self::ObserveFuture {
        let req = req.into_inner();

        let limit = req.limit as usize;
        if limit == 0 {
            let v = http::header::HeaderValue::from_static("limit must be positive");
            return future::err(Self::invalid_arg(v));
        };
        trace!("tap: limit={}", limit);

        // Read the match logic into a type we can use to evaluate against
        // requests. This match will be shared (weakly) by all registered
        // services to match requests. The response stream strongly holds the
        // match until the response is complete. This way, services never
        // evaluate matches for taps that have been completed or canceled.
        let match_ = match Match::try_new(req.match_) {
            Ok(m) => Arc::new(m),
            Err(e) => {
                warn!("invalid tap request: {} ", e);
                let v = format!("{}", e)
                    .parse()
                    .unwrap_or_else(|_| http::header::HeaderValue::from_static("invalid message"));
                return future::err(Self::invalid_arg(v));
            }
        };

        // Wrapping is okay. This is realy just to disambiguate events within a
        // single tap session (i.e. that may consist of several tap requests).
        let base_id = self.base_id.fetch_add(1, Ordering::AcqRel) as u32;
        debug!("tap; id={}; match={:?}", base_id, match_);

        // The taps channel is used by services to acquire a `TapTx` for
        // `Dispatch`, i.e. ensuring that no more than the requested number of
        // taps are executed.
        //
        // The read side of this channel (held by `dispatch`) is dropped by the
        // `ResponseStream` once the `limit` has been reached. This is dropped
        // with the strong reference to `match_` so that services can determine
        // when a Tap should be dropped.
        //
        // The response stream continues to process events for open streams
        // until all streams have been completed.
        let (taps_tx, taps_rx) = mpsc::channel(TAP_BUFFER_CAPACITY);

        // This tap is cloned onto each
        let tap = Tap::new(Arc::downgrade(&match_), taps_tx);
        self.subscribe.subscribe(tap);

        // The events channel is used to emit tap events to the response stream.
        //
        // At most `limit` copies of `events_tx` are dispatched to `taps_rx`
        // requests. Each tapped request's sender is dropped when the response
        // completes, so the event stream closes gracefully when all tapped
        // requests are completed without additional coordination.
        let (events_tx, events_rx) = mpsc::channel(PER_REQUEST_BUFFER_CAPACITY);

        // Reads up to `limit` requests from from `taps_rx` and satisfies them
        // with a cpoy of `events_tx`.
        let dispatch = Dispatch {
            base_id,
            count: 0,
            limit,
            taps_rx,
            events_tx,
            _match: match_,
        };

        let rsp = ResponseStream {
            dispatch: Some(dispatch),
            events_rx,
        };

        future::ok(Response::new(rsp))
    }
}

impl Stream for ResponseStream {
    type Item = api::TapEvent;
    type Error = grpc::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Drop the dispatch future once it completes so that services do not do
        // any more matching against this tap.
        self.dispatch = self.dispatch.take().and_then(|mut d| match d.poll() {
            Ok(Async::NotReady) => Some(d),
            Ok(Async::Ready(())) | Err(_) => None,
        });

        let poll: Poll<Option<Self::Item>, Self::Error> =
            self.events_rx.poll().or_else(|_| Ok(None.into()));
        let event = try_ready!(poll);
        trace!("ResponseStream::poll: event={:?}", event);
        Ok(event.into())
    }
}

impl Future for Dispatch {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), Self::Error> {
        while let Some(tx) = try_ready!(self.taps_rx.poll().map_err(|_| ())) {
            debug_assert!(self.count < self.limit - 1);

            self.count += 1;
            let tap = TapTx {
                id: api::tap_event::http::StreamId {
                    base: self.base_id,
                    stream: self.count as u64,
                },
                tx: self.events_tx.clone(),
            };
            let _ = tx.send(tap);

            if self.count == self.limit - 1 {
                return Ok(Async::Ready(()));
            }
        }

        Ok(Async::Ready(()))
    }
}

impl Tap {
    fn new(match_: Weak<Match>, taps_tx: mpsc::Sender<oneshot::Sender<TapTx>>) -> Self {
        Self { match_, taps_tx }
    }

    fn base_event<B, I: Inspect>(req: &http::Request<B>, inspect: &I) -> api::TapEvent {
        api::TapEvent {
            proxy_direction: if inspect.is_outbound(req) {
                api::tap_event::ProxyDirection::Outbound.into()
            } else {
                api::tap_event::ProxyDirection::Inbound.into()
            },
            source: inspect.src_addr(req).as_ref().map(|a| a.into()),
            source_meta: {
                let mut m = api::tap_event::EndpointMeta::default();
                let tls = format!("{}", inspect.src_tls(req));
                m.labels.insert("tls".to_owned(), tls);
                Some(m)
            },
            destination: inspect.dst_addr(req).as_ref().map(|a| a.into()),
            destination_meta: inspect.dst_labels(req).map(|labels| {
                let mut m = api::tap_event::EndpointMeta::default();
                m.labels.extend(labels.clone());
                let tls = format!("{}", inspect.dst_tls(req));
                m.labels.insert("tls".to_owned(), tls);
                m
            }),
            event: None,
        }
    }
}

impl iface::Tap for Tap {
    type TapRequest = TapRequest;
    type TapRequestBody = TapRequestBody;
    type TapResponse = TapResponse;
    type TapResponseBody = TapResponseBody;
    type Future = TapFuture;

    fn can_tap_more(&self) -> bool {
        self.match_.upgrade().is_some()
    }

    fn matches<B: Payload, I: Inspect>(
        &self,
        req: &http::Request<B>,
        inspect: &I,
    ) -> bool {
        self.match_.upgrade().map(|m| m.matches(req, inspect)).unwrap_or(false)
    }

    fn tap(&mut self) -> Self::Future {
        TapFuture(Fut::Init {
            request_init_at: clock::now(),
            taps_tx: self.taps_tx.clone(),
        })
    }
}

impl Future for TapFuture {
    type Item = Option<TapRequest>;
    type Error = Never;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            self.0 = match self.0 {
                Fut::Init { ref request_init_at, ref mut taps_tx } => {
                    match taps_tx.poll_ready() {
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Ok(Async::Ready(())) => {}
                        Err(_) => return Ok(Async::Ready(None)),
                    }
                    let (tx, rx) = oneshot::channel();

                    // If this fails, polling `rx` will fail below.
                    let _ = taps_tx.try_send(tx);

                    Fut::Pending {
                        request_init_at: *request_init_at,
                        rx,
                    }
                }
                Fut::Pending { ref request_init_at, ref mut rx } => {
                    let tap = match rx.poll() {
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Ok(Async::Ready(tap)) => Some(TapRequest {
                            request_init_at: *request_init_at,
                            tap,
                        }),
                        Err(_) => None,
                    };
                    return Ok(Async::Ready(tap));
                }
            }
        }
    }
}

impl iface::TapRequest for TapRequest {
    type TapBody = TapRequestBody;
    type TapResponse = TapResponse;
    type TapResponseBody = TapResponseBody;

    fn open<B: Payload, I: Inspect>(
        mut self,
        req: &http::Request<B>,
        inspect: &I,
     ) -> (TapRequestBody, TapResponse) {
        // All of the events emitted from tap have a common set of metadata.
        // Build this once, without an `event`, so that it can be used to build
        // each HTTP event.
        let base_event = Tap::base_event(req, inspect);

        let init = api::tap_event::http::RequestInit {
            id: Some(self.tap.id.clone()),
            method: Some(req.method().into()),
            scheme: req.uri().scheme_part().map(http_types::Scheme::from),
            authority: inspect.authority(req).unwrap_or_default().to_owned(),
            path: req.uri().path().into(),
        };
;
        let event = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(api::tap_event::http::Event::RequestInit(init)),
            })),
            ..base_event.clone()
        };
        let _ = self.tap.tx.try_send(event);

        let req = TapRequestBody {
            tap: self.tap.clone(),
            base_event: base_event.clone(),
        };
        let rsp = TapResponse {
            tap: self.tap,
            base_event,
            request_init_at: self.request_init_at,
        };
        (req, rsp)
    }
}
impl iface::TapResponse for TapResponse {
    type TapBody = TapResponseBody;

    fn tap<B: Payload>(mut self, rsp: &http::Response<B>) -> TapResponseBody {
        let response_init_at = clock::now();
        let init = api::tap_event::http::Event::ResponseInit(api::tap_event::http::ResponseInit {
            id: Some(self.tap.id.clone()),
            since_request_init: Some(pb_duration(response_init_at - self.request_init_at)),
            http_status: rsp.status().as_u16().into(),
        });

        let event = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(init),
            })),
            ..self.base_event.clone()
        };
        let _ = self.tap.tx.try_send(event);

        TapResponseBody {
            base_event: self.base_event,
            request_init_at: self.request_init_at,
            response_init_at,
            response_bytes: 0,
            tap: self.tap,
        }
    }

    fn fail<E: HasH2Reason>(mut self, err: &E) {
        let response_end_at = clock::now();
        let reason = err.h2_reason();
        let end = api::tap_event::http::Event::ResponseEnd(api::tap_event::http::ResponseEnd {
            id: Some(self.tap.id.clone()),
            since_request_init: Some(pb_duration(response_end_at - self.request_init_at)),
            since_response_init: None,
            response_bytes: 0,
            eos: Some(api::Eos {
                end: reason.map(|r| api::eos::End::ResetErrorCode(r.into())),
            }),
        });

        let event = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(end),
            })),
            ..self.base_event
        };
        let _ = self.tap.tx.try_send(event);
    }
}

impl iface::TapBody for TapRequestBody {
    fn data<B: Buf>(&mut self, _: &B) {}

    fn eos(self, _: Option<&http::HeaderMap>) {}

    fn fail(self, _: &h2::Error) {}
}

impl iface::TapBody for TapResponseBody {
    fn data<B: Buf>(&mut self, data: &B) {
        self.response_bytes += data.remaining();
    }

    fn eos(self, trls: Option<&http::HeaderMap>) {
        let end = trls
            .and_then(|t| t.get("grpc-status"))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .map(api::eos::End::GrpcStatusCode);

        self.send(end);
    }

    fn fail(self, e: &h2::Error) {
        let end = e.reason().map(|r| api::eos::End::ResetErrorCode(r.into()));
        self.send(end);
    }
}

impl TapResponseBody {
    fn send(mut self, end: Option<api::eos::End>) {
        let response_end_at = clock::now();

        let end = api::tap_event::http::Event::ResponseEnd(api::tap_event::http::ResponseEnd {
            id: Some(self.tap.id.clone()),
            since_request_init: Some(pb_duration(response_end_at - self.request_init_at)),
            since_response_init: Some(pb_duration(response_end_at - self.response_init_at)),
            response_bytes: self.response_bytes as u64,
            eos: Some(api::Eos { end }),
        });

        let event = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(end),
            })),
            ..self.base_event
        };
        let _ = self.tap.tx.try_send(event);
    }
}
