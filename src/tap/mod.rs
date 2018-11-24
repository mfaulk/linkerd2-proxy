use http;
use indexmap::IndexMap;
use std::net;

use transport::tls;

mod daemon;
mod grpc;
mod service;

pub type Layer = service::Layer<daemon::Register<grpc::Tap>>;
pub type Server = grpc::Server<daemon::Subscribe<grpc::Tap>>;
pub type Daemon = daemon::Daemon<grpc::Tap>;

/// Build the tap subsystem.
///
///
pub fn new() -> (Layer, Server, Daemon) {
    let (daemon, register, subscribe) = daemon::new();
    let layer = Layer::new(register);
    let server = Server::new(subscribe);
    (layer, server, daemon)
}

///
pub trait Inspect {
    fn src_addr<B>(&self, req: &http::Request<B>) -> Option<net::SocketAddr>;
    fn src_tls<B>(&self, req: &http::Request<B>) -> tls::Status;

    fn dst_addr<B>(&self, req: &http::Request<B>) -> Option<net::SocketAddr>;
    fn dst_labels<B>(&self, req: &http::Request<B>) -> Option<&IndexMap<String, String>>;
    fn dst_tls<B>(&self, req: &http::Request<B>) -> tls::Status;

    fn is_outbound<B>(&self, req: &http::Request<B>) -> bool;

    fn is_inbound<B>(&self, req: &http::Request<B>) -> bool {
        !self.is_outbound(req)
    }

    fn authority<B>(&self, req: &http::Request<B>) -> Option<String> {
        req.uri().authority_part().map(|a| a.as_str().to_owned()).or_else(|| {
            req.headers()
                .get(http::header::HOST)
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_owned())
        })
    }
}

/// The internal interface used between Layer, Server, and Daemon.
///
/// This module is necessary to seal the traits, which must be public
/// forLayer/Server/Daemon to
mod iface {
    use bytes::Buf;
    use futures::Stream;
    use http;
    use std::sync::Weak;
    use tower_h2::Body as Payload;

    use proxy::http::HasH2Reason;

    pub trait Register {
        type Tap: Tap;
        type Taps: Stream<Item = Weak<Self::Tap>>;

        fn register(&mut self) -> Self::Taps;
    }

    pub trait Subscribe<T: Tap> {
        fn subscribe(&mut self, tap: Weak<T>);
    }

    pub trait Tap {
        type TapRequestBody: TapBody;
        type TapResponse: TapResponse<TapBody = Self::TapResponseBody>;
        type TapResponseBody: TapBody;

        fn can_tap_more(&self) -> bool {
            true
        }

        fn tap<B: Payload, I: super::Inspect>(
            &self,
            req: &http::Request<B>,
            inspect: &I,
        ) -> Option<(Self::TapRequestBody, Self::TapResponse)>;
    }

    pub trait TapBody {
        fn data<B: Buf>(&mut self, data: &B);

        fn eos(self, headers: Option<&http::HeaderMap>);

        fn fail(self, error: &h2::Error);
    }

    pub trait TapResponse {
        type TapBody: TapBody;

        fn tap<B: Payload>(self, rsp: &http::Response<B>) -> Self::TapBody;

        fn fail<E: HasH2Reason>(self, error: &E);
    }
}
