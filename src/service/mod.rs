//! High-level RKNPU service layer.
//!
//! This module sits above the low-level MMIO/GEM/task code and below OS
//! device-node adapters. It owns:
//!
//! - blocking-submit scheduling
//! - per-submit waiter management
//! - RKNPU-specific ioctl payload handling
//! - small trait boundaries for OS services such as userspace copy, sleeping,
//!   and worker spawning

use alloc::sync::Arc;

mod error;
mod handle;
mod ioctl;
mod platform;
mod scheduler;

pub use error::RknpuServiceError;
pub use handle::SubmitHandle;
pub use ioctl::{RknpuCmd, RknpuUserAction};
pub use platform::{
    RknpuDeviceAccess, RknpuRuntime, RknpuSchedulerRuntime, RknpuSubmitWaiter, RknpuUserMemory,
    RknpuWorkerListener, RknpuWorkerSignal,
};
pub use scheduler::CompletedSubmit;

use scheduler::RknpuScheduler;

/// Shared high-level RKNPU service instance.
///
/// The service is intentionally not a crate-global singleton. Each embedding
/// OS or test harness can construct and own an instance with its own platform
/// adapter.
pub struct RknpuService<P: RknpuRuntime> {
    inner: Arc<RknpuServiceInner<P>>,
}

struct RknpuServiceInner<P: RknpuRuntime> {
    platform: P,
    scheduler: RknpuScheduler<P>,
}

impl<P: RknpuRuntime> Clone for RknpuService<P> {
    /// Clone the shared service handle by cloning the inner `Arc`.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P: RknpuRuntime> RknpuService<P> {
    /// Build a new service around one platform adapter.
    pub fn new(platform: P) -> Self {
        let scheduler = RknpuScheduler::new(&platform);
        Self {
            inner: Arc::new(RknpuServiceInner {
                platform,
                scheduler,
            }),
        }
    }
}

#[cfg(test)]
mod tests;
