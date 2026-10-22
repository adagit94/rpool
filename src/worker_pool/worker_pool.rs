use std::{
    panic::{UnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, SyncSender, channel as chan, sync_channel as sync_chan},
    },
    thread::available_parallelism,
};

use crate::{
    worker_pool::tasks::{BoxedTask, IndexedTaskResult},
    worker_pool::worker::Worker,
};

type TaskQueue = Sender<BoxedTask>;
type ResultErr = ();

pub struct WorkerPool {
    queue: TaskQueue,
}

impl WorkerPool {
    pub fn new(mut workers_count: usize) -> WorkerPool {
        if workers_count == 0 {
            workers_count = available_parallelism().unwrap().get();

            if workers_count > 1 {
                workers_count -= 1;
            }
        }

        let (s, r) = chan::<BoxedTask>();
        let rec = Arc::new(Mutex::new(r));

        for _ in 0..workers_count {
            Worker::new(rec.clone());
        }

        return WorkerPool { queue: s };
    }

    fn box_task(&self, task: impl FnOnce() + Send + UnwindSafe + 'static) -> BoxedTask {
        return Box::new(move || {
            match catch_unwind(task) {
                Ok(_) => {}
                Err(_err) => {
                    println!("Task execution failed.");
                }
            };
        });
    }

    fn box_task_with_result<R: Send + 'static>(
        &self,
        task: impl FnOnce() -> R + Send + UnwindSafe + 'static,
        sender: SyncSender<Result<R, ResultErr>>,
    ) -> BoxedTask {
        return Box::new(move || {
            match catch_unwind(task) {
                Ok(r) => match sender.send(Ok(r)) {
                    Err(err) => {
                        println!("Send of task result failed:\n{err}");
                    }
                    _ => {}
                },

                Err(_err) => {
                    println!("Task execution failed.");

                    match sender.send(Err(())) {
                        Err(err) => {
                            println!("Send of task error failed:\n{err}");
                        }
                        _ => {}
                    }
                }
            };
        });
    }

    fn send_task(&self, task: BoxedTask) -> bool {
        match self.queue.send(task) {
            Ok(_) => {
                return true;
            }

            Err(err) => {
                println!("send_task: Send failed. No worker available:\n{err}");
                return false;
            }
        }
    }
}

pub trait WorkerPoolTrait {
    fn schedule(&self, task: impl FnOnce() + Send + UnwindSafe + 'static);
    fn schedule_receive<R: Send + 'static>(
        &self,
        task: impl FnOnce() -> R + Send + UnwindSafe + 'static,
    ) -> Result<Receiver<Result<R, ResultErr>>, ResultErr>;
    fn schedule_many_receive_ordered<R: Send + 'static>(
        &self,
        tasks: Vec<impl FnOnce() -> R + Send + UnwindSafe + 'static>,
    ) -> Result<Vec<R>, ResultErr>;
}

impl WorkerPoolTrait for WorkerPool {
    fn schedule(&self, task: impl FnOnce() + Send + UnwindSafe + 'static) {
        self.send_task(self.box_task(task));
    }

    fn schedule_receive<R: Send + 'static>(
        &self,
        task: impl FnOnce() -> R + Send + UnwindSafe + 'static,
    ) -> Result<Receiver<Result<R, ResultErr>>, ResultErr> {
        let (s, r) = sync_chan::<Result<R, ResultErr>>(0);

        if !self.send_task(self.box_task_with_result(task, s)) {
            return Err(());
        }

        return Ok(r);
    }

    fn schedule_many_receive_ordered<R: Send + 'static>(
        &self,
        tasks: Vec<impl FnOnce() -> R + Send + UnwindSafe + 'static>,
    ) -> Result<Vec<R>, ResultErr> {
        let tasks_count = tasks.len();
        let (s, r) = sync_chan::<Result<IndexedTaskResult<R>, ResultErr>>(tasks_count);
        let mut indexed_results: Vec<IndexedTaskResult<R>> = Vec::with_capacity(tasks_count);

        for (i, t) in tasks.into_iter().enumerate() {
            if !self
                .send_task(self.box_task_with_result(move || IndexedTaskResult(i, t()), s.clone()))
            {
                return Err(());
            }
        }

        for _ in 0..tasks_count {
            match r.recv() {
                Ok(res) => match res {
                    Ok(res) => {
                        indexed_results.push(res);
                    }
                    Err(err) => {
                        return Err(err);
                    }
                },
                Err(err) => {
                    println!(
                        "schedule_many_receive_ordered: Receive from the worker thread failed:\n{err}"
                    );

                    return Err(());
                }
            }
        }

        indexed_results.sort_by_key(|r| r.0);
        return Ok(indexed_results.into_iter().map(|r| r.1).collect());
    }
}
