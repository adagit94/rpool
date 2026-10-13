pub type BoxedTask = Box<dyn FnOnce() + Send + 'static>;
pub struct IndexedTaskResult<R>(pub usize, pub R);


