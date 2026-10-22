pub(crate) type BoxedTask = Box<dyn FnOnce() + Send + 'static>;
pub(crate) struct IndexedTaskResult<R>(pub usize, pub R);
