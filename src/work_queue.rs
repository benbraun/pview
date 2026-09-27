//! Bounded work queues keep hub I/O out of the SSE dispatcher.
use futures_util::future::BoxFuture;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq)]
pub enum WorkKind {
    Ordered,
    Replace(String),
    Stop(String),
}

struct Job {
    kind: WorkKind,
    run: BoxFuture<'static, ()>,
}

#[derive(Clone)]
pub struct WorkQueue {
    jobs: Arc<Mutex<VecDeque<Job>>>,
    ready: Arc<Notify>,
    capacity: usize,
}

impl WorkQueue {
    pub fn new(capacity: usize) -> Self {
        let queue = Self {
            jobs: Arc::new(Mutex::new(VecDeque::<Job>::new())),
            ready: Arc::new(Notify::new()),
            capacity,
        };
        let worker = queue.clone();
        tokio::spawn(async move {
            loop {
                worker.ready.notified().await;
                loop {
                    let job = worker.jobs.lock().unwrap().pop_front();
                    match job {
                        Some(job) => job.run.await,
                        None => break,
                    }
                }
            }
        });
        queue
    }

    pub fn submit(
        &self,
        kind: WorkKind,
        run: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        match &kind {
            WorkKind::Replace(key) => {
                // Never move a replacement across a scene, STOP or other ordered operation.
                if let Some(job) = jobs
                    .iter_mut()
                    .rev()
                    .take_while(|j| matches!(j.kind, WorkKind::Replace(_)))
                    .find(|j| j.kind == WorkKind::Replace(key.clone()))
                {
                    job.run = Box::pin(run);
                    return Ok(());
                }
            }
            WorkKind::Stop(key) => {
                jobs.retain(|j| !matches!(&j.kind, WorkKind::Replace(k) if k == key || k == &format!("{key}_top")));
            }
            WorkKind::Ordered => {}
        }
        anyhow::ensure!(
            jobs.len() < self.capacity,
            "hub work queue is full; command rejected"
        );
        let job = Job {
            kind,
            run: Box::pin(run),
        };
        if matches!(job.kind, WorkKind::Stop(_)) {
            jobs.push_front(job);
        } else {
            jobs.push_back(job);
        }
        drop(jobs);
        self.ready.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn slow_work_does_not_block_enqueue_and_stop_supersedes_pending_moves() {
        let queue = WorkQueue::new(4);
        let (release, wait) = tokio::sync::oneshot::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        queue
            .submit(WorkKind::Ordered, async move {
                started.send(()).unwrap();
                wait.await.unwrap();
            })
            .unwrap();
        ready.await.unwrap();
        let seen = Arc::new(Mutex::new(vec![]));
        for (kind, value) in [
            (WorkKind::Replace("a".into()), 1),
            (WorkKind::Replace("a".into()), 2),
            (WorkKind::Replace("b".into()), 3),
            (WorkKind::Stop("a".into()), 4),
        ] {
            let seen = seen.clone();
            queue
                .submit(kind, async move {
                    seen.lock().unwrap().push(value);
                })
                .unwrap();
        }
        let (done, finished) = tokio::sync::oneshot::channel();
        queue
            .submit(WorkKind::Ordered, async move {
                done.send(()).unwrap();
            })
            .unwrap();
        release.send(()).unwrap();
        finished.await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![4, 3]);
    }

    #[tokio::test]
    async fn ordered_work_is_a_replacement_barrier_and_capacity_is_bounded() {
        let queue = WorkQueue::new(3);
        let (release, wait) = tokio::sync::oneshot::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        queue
            .submit(WorkKind::Ordered, async move {
                started.send(()).unwrap();
                let _ = wait.await;
            })
            .unwrap();
        ready.await.unwrap();
        queue
            .submit(WorkKind::Replace("a".into()), async {})
            .unwrap();
        queue.submit(WorkKind::Ordered, async {}).unwrap();
        queue
            .submit(WorkKind::Replace("a".into()), async {})
            .unwrap();
        assert!(queue.submit(WorkKind::Ordered, async {}).is_err());
        release.send(()).unwrap();
    }
}
