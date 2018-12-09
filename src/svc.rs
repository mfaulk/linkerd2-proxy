pub extern crate linkerd2_stack as stack;
extern crate tower_service;

pub use self::tower_service::Service;
pub use tower_util::MakeService;

pub use self::stack::{
    shared,
    stack_per_request,
    watch,
    Either,
    Layer,
    Stack,
};
