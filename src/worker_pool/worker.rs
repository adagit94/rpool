use std::{
    sync::{Arc, Mutex, mpsc::Receiver},
    thread::{self as th, JoinHandle},
};

use crate::worker_pool::tasks::BoxedTask;

type AtomicTaskReceiver = Arc<Mutex<Receiver<BoxedTask>>>;

pub(crate) struct Worker();

impl Worker {
    pub(crate) fn new(task: AtomicTaskReceiver) -> JoinHandle<fn()> {
        return th::spawn(move || {
            loop {
                let mg = task.lock().unwrap();
                let t = mg.recv();

                drop(mg);

                match t {
                    Ok(t) => {
                        t();
                    }
                    Err(err) => {
                        println!("Receive of a task failed:\n{err}");
                    }
                }
            }
        });
    }
}
