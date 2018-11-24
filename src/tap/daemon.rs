use futures::{Async, Future, Poll, Stream};
use futures::sync::mpsc;
use never::Never;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use super::iface::Tap;

const SERVICE_CAPACITY: usize = 10_000;

const TAP_CAPACITY: usize = 1_000;

pub fn new<T>() -> (Daemon<T>, Register<T>, Subscribe<T>) {
    let (svc_tx, svc_rx) = mpsc::channel(SERVICE_CAPACITY);
    let (tap_tx, tap_rx) = mpsc::channel(TAP_CAPACITY);

    let daemon = Daemon {
        svc_rx,
        svcs: VecDeque::default(),

        tap_rx,
        taps: VecDeque::default(),
    };

    (daemon, Register(svc_tx), Subscribe(tap_tx))
}

#[must_use = "daemon must be polled"]
#[derive(Debug)]
pub struct Daemon<T> {
    svc_rx: mpsc::Receiver<mpsc::Sender<Weak<T>>>,
    svcs: VecDeque<mpsc::Sender<Weak<T>>>,

    tap_rx: mpsc::Receiver<T>,
    taps: VecDeque<Arc<T>>,
}

#[derive(Debug)]
pub struct Register<T>(mpsc::Sender<mpsc::Sender<Weak<T>>>);

#[derive(Debug)]
pub struct Subscribe<T>(mpsc::Sender<T>);

impl<T: Tap> Future for Daemon<T> {
    type Item = ();
    type Error = Never;

    fn poll(&mut self) -> Poll<(), Never> {
        // Drop taps that are no longer active (i.e. the response stream has
        // been droped).
        let tap_count = self.taps.len();
        self.taps.retain(|t| t.can_tap_more());
        trace!("retained {} of {} taps", self.taps.len(), tap_count);

        // Connect newly-created services to active taps.
        while let Ok(Async::Ready(Some(mut svc))) = self.svc_rx.poll() {
            trace!("registering a service");

            // Notify the service of all active taps. If there's an error, the
            // registration is dropped.
            let mut is_ok = true;
            for tap in &self.taps {
                if is_ok {
                    is_ok = svc.try_send(Arc::downgrade(tap)).is_ok();
                }
            }

            if is_ok {
                self.svcs.push_back(svc);
                trace!("service registered");
            }
        }

        // Connect newly-created taps to existing services.
        while let Ok(Async::Ready(Some(t))) = self.tap_rx.poll() {
            trace!("subscribing a tap");
            if !t.can_tap_more() {
                continue;
            }
            let tap = Arc::new(t);

            // Notify services of the new tap. If the tap can't be sent to a
            // given service, it's assumed that the service has been dropped, so
            // it is removed from the registry.
            for idx in (0..self.svcs.len()).rev() {
                if self.svcs[idx].try_send(Arc::downgrade(&tap)).is_err() {
                    trace!("removing a service");
                    self.svcs.swap_remove_back(idx);
                }
            }

            self.taps.push_back(tap);
            trace!("tap subscribed");
        }

        Ok(Async::NotReady)
    }
}

impl<T: Tap> Clone for Register<T> {
    fn clone(&self) -> Self {
        Register(self.0.clone())
    }
}

impl<T: Tap> super::iface::Register for Register<T> {
    type Tap = T;
    type Taps = mpsc::Receiver<Weak<T>>;

    fn register(&mut self) -> Self::Taps {
        let (tx, rx) = mpsc::channel(TAP_CAPACITY);
        if let Err(_) = self.0.try_send(tx) {
            debug!("failed to register service");
        }
        rx
    }
}

impl<T: Tap> Clone for Subscribe<T> {
    fn clone(&self) -> Self {
        Subscribe(self.0.clone())
    }
}

impl<T: Tap> super::iface::Subscribe<T> for Subscribe<T> {
    fn subscribe(&mut self, tap: T) {
        if let Err(_) = self.0.try_send(tap) {
            debug!("failed to subscribe tap");
        }
    }
}
