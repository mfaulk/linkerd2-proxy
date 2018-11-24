use bytes::Buf;
use futures::{future, sync::mpsc, Poll, Stream};
use http::HeaderMap;
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

// Buffer ~10 req/rsp pairs' worth of events.
const PER_REQUEST_BUFFER_CAPACITY: usize = 40;

#[derive(Clone, Debug)]
pub struct Server<T> {
    subscribe: T,
    base_id: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct ResponseStream {
    rx: mpsc::Receiver<api::TapEvent>,
    _handle: Arc<()>,
}

#[derive(Debug)]
pub struct Tap {
    tx: mpsc::Sender<api::TapEvent>,
    match_: Match,
    base_id: u32,
    count: AtomicUsize,
    limit: usize,
    response_handle: Weak<()>,
}

#[derive(Debug)]
pub struct TapResponse {
    base_event: api::TapEvent,
    id: api::tap_event::http::StreamId,
    request_init_at: Instant,
    tx: mpsc::Sender<api::TapEvent>,
}

#[derive(Debug)]
pub struct TapRequestBody {
    base_event: api::TapEvent,
    id: api::tap_event::http::StreamId,
    tx: mpsc::Sender<api::TapEvent>,
}

#[derive(Debug)]
pub struct TapResponseBody {
    base_event: api::TapEvent,
    id: api::tap_event::http::StreamId,
    request_init_at: Instant,
    response_init_at: Instant,
    response_bytes: usize,
    tx: mpsc::Sender<api::TapEvent>,
}

impl<T: iface::Subscribe<Tap>> Server<T> {
    pub(in tap) fn new(subscribe: T) -> Self {
        let base_id = Arc::new(0.into());
        Self { base_id, subscribe }
    }

    fn invalid_arg(msg: http::header::HeaderValue) -> grpc::Error {
        let status = grpc::Status::with_code(grpc::Code::InvalidArgument);
        let mut headers = HeaderMap::new();
        headers.insert("grpc-message", msg);
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

        let limit = match req.limit as usize {
            0 => {
                let v = http::header::HeaderValue::from_static("limit must be positive");
                return future::err(Self::invalid_arg(v));
            }
            n if n == ::std::usize::MAX => {
                let v = http::header::HeaderValue::from_static("limit is too large");
                return future::err(Self::invalid_arg(v));
            }
            n => n,
        };

        let match_ = match Match::try_new(req.match_) {
            Ok(m) => m,
            Err(e) => {
                let v = format!("{}", e)
                    .parse()
                    .or_else(|_| "invalid message".parse())
                    .unwrap();
                return future::err(Self::invalid_arg(v));
            }
        };

        // Wrapping is okay. This is realy just to disambiguate events within a
        // single tap session (i.e. that may consist of several tap requests).
        let base_id = self.base_id.fetch_add(1, Ordering::AcqRel) as u32;
        info!("tap: id={}; match={:?}", base_id, match_);

        let (tx, rx) = mpsc::channel(PER_REQUEST_BUFFER_CAPACITY);
        let _handle = Arc::new(());
        let tap = Tap::new(base_id, tx, match_, limit, Arc::downgrade(&_handle));
        self.subscribe.subscribe(tap);
        future::ok(Response::new(ResponseStream { rx, _handle }))
    }
}

impl Stream for ResponseStream {
    type Item = api::TapEvent;
    type Error = grpc::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll().or_else(|_| Ok(None.into()))
    }
}

impl Tap {
    fn new(
        base_id: u32,
        tx: mpsc::Sender<api::TapEvent>,
        match_: Match,
        limit: usize,
        response_handle: Weak<()>,
    ) -> Self {
        Self {
            tx,
            match_,
            base_id,
            limit,
            count: 0.into(),
            response_handle,
        }
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
    type TapRequestBody = TapRequestBody;
    type TapResponse = TapResponse;
    type TapResponseBody = TapResponseBody;

    fn can_tap_more(&self) -> bool {
        self.response_handle.upgrade().is_some() && self.count.load(Ordering::Acquire) < self.limit
    }

    fn tap<B: Payload, I: Inspect>(
        &self,
        req: &http::Request<B>,
        inspect: &I,
    ) -> Option<(TapRequestBody, TapResponse)> {
        let request_init_at = clock::now();

        if !self.match_.matches(&req, inspect) {
            return None;
        }

        let n = self.count.fetch_add(1, Ordering::AcqRel);
        if n >= self.limit {
            return None;
        }

        // All of the events emitted from tap have a common set of metadata.
        // Build this once, without an `event`, so that it can be used to build
        // each HTTP event.
        let base_event = Self::base_event(req, inspect);

        let id = api::tap_event::http::StreamId {
            base: self.base_id,
            stream: n as u64,
        };

        let msg = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(api::tap_event::http::Event::RequestInit(
                    api::tap_event::http::RequestInit {
                        id: Some(id.clone()),
                        method: Some(req.method().into()),
                        scheme: req.uri().scheme_part().map(http_types::Scheme::from),
                        authority: inspect.authority(req).unwrap_or_default().to_owned(),
                        path: req.uri().path().into(),
                    },
                )),
            })),
            ..base_event.clone()
        };

        let mut tx = self.tx.clone();
        tx.try_send(msg).ok()?;

        let req = TapRequestBody {
            id: id.clone(),
            tx: tx.clone(),
            base_event: base_event.clone(),
        };
        let rsp = TapResponse {
            id,
            tx,
            base_event,
            request_init_at,
        };
        Some((req, rsp))
    }
}

impl iface::TapResponse for TapResponse {
    type TapBody = TapResponseBody;

    fn tap<B: Payload>(mut self, rsp: &http::Response<B>) -> TapResponseBody {
        let response_init_at = clock::now();
        let msg = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(api::tap_event::http::Event::ResponseInit(
                    api::tap_event::http::ResponseInit {
                        id: Some(self.id.clone()),
                        since_request_init: Some(pb_duration(
                            response_init_at - self.request_init_at,
                        )),
                        http_status: rsp.status().as_u16().into(),
                    },
                )),
            })),
            ..self.base_event.clone()
        };
        let _ = self.tx.try_send(msg);

        TapResponseBody {
            base_event: self.base_event,
            id: self.id,
            request_init_at: self.request_init_at,
            response_init_at,
            response_bytes: 0,
            tx: self.tx,
        }
    }

    fn fail<E: HasH2Reason>(mut self, e: &E) {
        let response_end_at = clock::now();
        let end = e
            .h2_reason()
            .map(|r| api::eos::End::ResetErrorCode(r.into()));
        let msg = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(api::tap_event::http::Event::ResponseEnd(
                    api::tap_event::http::ResponseEnd {
                        id: Some(self.id.clone()),
                        since_request_init: Some(pb_duration(
                            response_end_at - self.request_init_at,
                        )),
                        since_response_init: None,
                        response_bytes: 0,
                        eos: Some(api::Eos { end }),
                    },
                )),
            })),
            ..self.base_event
        };

        let _ = self.tx.try_send(msg);
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

        self.send_end(end);
    }

    fn fail(self, e: &h2::Error) {
        let end = e.reason().map(|r| api::eos::End::ResetErrorCode(r.into()));
        self.send_end(end);
    }
}

impl TapResponseBody {
    fn send_end(mut self, end: Option<api::eos::End>) {
        let response_end_at = clock::now();
        let msg = api::TapEvent {
            event: Some(api::tap_event::Event::Http(api::tap_event::Http {
                event: Some(api::tap_event::http::Event::ResponseEnd(
                    api::tap_event::http::ResponseEnd {
                        id: Some(self.id.clone()),
                        since_request_init: Some(pb_duration(
                            response_end_at - self.request_init_at,
                        )),
                        since_response_init: Some(pb_duration(
                            response_end_at - self.response_init_at,
                        )),
                        response_bytes: self.response_bytes as u64,
                        eos: Some(api::Eos { end }),
                    },
                )),
            })),
            ..self.base_event
        };

        let _ = self.tx.try_send(msg);
    }
}
