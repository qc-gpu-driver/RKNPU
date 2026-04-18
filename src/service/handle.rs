use crate::RknpuQueueTaskId;

use super::{CompletedSubmit, RknpuRuntime, RknpuService, RknpuServiceError};

/// Owned handle to one in-flight or completed async submit.
///
/// Returned by [`RknpuService::submit_async`]. Dropping the handle before
/// completion detaches it — the scheduler continues draining the submit to
/// terminal state, but the result is discarded.
pub struct SubmitHandle<P: RknpuRuntime> {
    service: RknpuService<P>,
    task_id: Option<RknpuQueueTaskId>,
}

impl<P: RknpuRuntime> SubmitHandle<P> {
    pub(super) fn new(service: RknpuService<P>, task_id: RknpuQueueTaskId) -> Self {
        Self {
            service,
            task_id: Some(task_id),
        }
    }

    /// The queue task id assigned to this submit.
    pub fn task_id(&self) -> Option<RknpuQueueTaskId> {
        self.task_id
    }

    /// Check for completion without blocking.
    ///
    /// Returns `Some` exactly once when the submit is terminal. Returns `None`
    /// while still in-flight or after the result has already been taken.
    pub fn poll(&mut self) -> Option<CompletedSubmit> {
        let task_id = self.task_id?;
        match self.service.take_terminal_submit(task_id) {
            Ok(result) => {
                self.task_id = None;
                Some(result)
            }
            Err(_) => None,
        }
    }

    /// Block until the submit is terminal and return the result.
    pub fn wait(mut self) -> Result<CompletedSubmit, RknpuServiceError> {
        let task_id = self.task_id.take().ok_or(RknpuServiceError::InvalidData)?;
        // wait_for_submit calls abort_wait internally on interrupt, so no
        // double-cleanup needed from Drop when this returns Err.
        self.service.wait_for_submit(task_id)?;
        self.service.take_terminal_submit(task_id)
    }
}

impl<P: RknpuRuntime> Drop for SubmitHandle<P> {
    fn drop(&mut self) {
        if let Some(task_id) = self.task_id.take() {
            self.service.detach_waiter(task_id);
        }
    }
}
