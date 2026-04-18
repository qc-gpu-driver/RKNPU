use alloc::{vec, vec::Vec};
use core::{
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use super::{
    RknpuCmd, RknpuDeviceAccess, RknpuSchedulerRuntime, RknpuService, RknpuServiceError,
    RknpuSubmitWaiter, RknpuUserMemory, RknpuWorkerListener, RknpuWorkerSignal,
};
use crate::{
    Rknpu, RknpuAction, RknpuConfig, RknpuError, RknpuTask, RknpuType,
    ioctrl::{RknpuMemCreate, RknpuMemDestroy, RknpuMemMap, RknpuSubmit},
};
use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
};

const FAKE_MMIO_LEN: usize = 0x10000;

/// Test platform that supplies mock MMIO, user copies, waiters, and workers.
#[derive(Clone)]
struct MockPlatform {
    device: Arc<MockDevice>,
    spawn_count: Arc<AtomicUsize>,
    interrupt_wait: Arc<AtomicBool>,
}

/// Shared mock hardware object guarded the same way an OS adapter would guard it.
struct MockDevice {
    _mmios: Vec<Vec<u8>>,
    npu: Mutex<Rknpu>,
}

impl MockPlatform {
    /// Build a mock RK3588 service platform backed by three fake MMIO regions.
    fn new() -> Self {
        let mut mmios = vec![vec![0_u8; FAKE_MMIO_LEN]; 3];
        let base_addrs = mmios
            .iter_mut()
            .map(|mmio| NonNull::new(mmio.as_mut_ptr()).unwrap())
            .collect::<Vec<_>>();
        let config = RknpuConfig {
            rknpu_type: RknpuType::Rk3588,
        };

        Self {
            device: Arc::new(MockDevice {
                _mmios: mmios,
                npu: Mutex::new(Rknpu::new(&base_addrs, config)),
            }),
            spawn_count: Arc::new(AtomicUsize::new(0)),
            interrupt_wait: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Publish a completion status into one fake core's IRQ register.
    fn publish_completion(&self, core_slot: usize, irq_status: u32) {
        let dev = self.device.npu.lock().unwrap();
        dev.base[core_slot]
            .irq_status
            .store(irq_status, Ordering::Release);
    }

    /// Return how many scheduler workers the platform spawned.
    fn spawn_count(&self) -> usize {
        self.spawn_count.load(Ordering::SeqCst)
    }
}

impl RknpuDeviceAccess for MockPlatform {
    /// Run a closure with exclusive access to the mock NPU driver.
    fn with_device<T, F>(&self, f: F) -> Result<T, RknpuServiceError>
    where
        F: FnOnce(&mut Rknpu) -> Result<T, RknpuError>,
    {
        let mut dev = self.device.npu.lock().unwrap();
        f(&mut dev).map_err(RknpuServiceError::from)
    }
}

impl RknpuUserMemory for MockPlatform {
    /// Copy bytes from the caller-provided test pointer into a destination buffer.
    fn copy_from_user(
        &self,
        dst: *mut u8,
        src: *const u8,
        size: usize,
    ) -> Result<(), RknpuServiceError> {
        unsafe {
            core::ptr::copy_nonoverlapping(src, dst, size);
        }
        Ok(())
    }

    /// Copy bytes from a source buffer into the caller-provided test pointer.
    fn copy_to_user(
        &self,
        dst: *mut u8,
        src: *const u8,
        size: usize,
    ) -> Result<(), RknpuServiceError> {
        unsafe {
            core::ptr::copy_nonoverlapping(src, dst, size);
        }
        Ok(())
    }
}

/// Condvar-backed waiter used to model one blocking submit ioctl.
struct MockWaiter {
    done: Mutex<bool>,
    cv: Condvar,
    interrupt: bool,
}

impl RknpuSubmitWaiter for MockWaiter {
    /// Block until completion unless the test configured an interrupted wait.
    fn wait(&self) -> Result<(), RknpuServiceError> {
        if self.interrupt {
            return Err(RknpuServiceError::Interrupted);
        }

        let mut done = self.done.lock().unwrap();
        while !*done {
            done = self.cv.wait(done).unwrap();
        }
        Ok(())
    }

    /// Mark the waiter as complete and wake every blocked test thread.
    fn complete(&self) {
        let mut done = self.done.lock().unwrap();
        *done = true;
        self.cv.notify_all();
    }
}

/// Cloneable wake-up handle shared between the service and worker thread.
#[derive(Clone)]
struct MockWorkerSignal {
    inner: Arc<MockWorkerSignalInner>,
}

/// Generation counter used to avoid lost wake-ups in the mock signal.
struct MockWorkerSignalInner {
    generation: Mutex<u64>,
    cv: Condvar,
}

/// Prepared listener that waits for the signal generation to change.
struct MockWorkerListenerState {
    inner: Arc<MockWorkerSignalInner>,
    generation: u64,
}

impl RknpuWorkerListener for MockWorkerListenerState {
    /// Sleep until the mock worker signal advances to a later generation.
    fn wait(self) {
        let mut generation_guard = self.inner.generation.lock().unwrap();
        while *generation_guard == self.generation {
            generation_guard = self.inner.cv.wait(generation_guard).unwrap();
        }
    }
}

impl RknpuWorkerSignal for MockWorkerSignal {
    type Listener = MockWorkerListenerState;

    /// Capture the current generation before the worker re-checks work.
    fn listen(&self) -> Self::Listener {
        let generation = *self.inner.generation.lock().unwrap();
        MockWorkerListenerState {
            inner: self.inner.clone(),
            generation,
        }
    }

    /// Advance the generation and wake one sleeping worker.
    fn notify_one(&self) {
        let mut generation = self.inner.generation.lock().unwrap();
        *generation = generation.saturating_add(1);
        self.inner.cv.notify_one();
    }
}

impl RknpuSchedulerRuntime for MockPlatform {
    type Waiter = MockWaiter;
    type WorkerSignal = MockWorkerSignal;

    /// Create a waiter whose interrupt behavior matches the current test flag.
    fn new_waiter(&self) -> Self::Waiter {
        MockWaiter {
            done: Mutex::new(false),
            cv: Condvar::new(),
            interrupt: self.interrupt_wait.load(Ordering::Acquire),
        }
    }

    /// Create the shared worker signal for one service instance.
    fn new_worker_signal(&self) -> Self::WorkerSignal {
        MockWorkerSignal {
            inner: Arc::new(MockWorkerSignalInner {
                generation: Mutex::new(0),
                cv: Condvar::new(),
            }),
        }
    }

    /// Spawn the scheduler worker and count the spawn for singleton checks.
    fn spawn_worker<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn_count.fetch_add(1, Ordering::SeqCst);
        thread::spawn(f);
    }

    /// Yield the host thread while the mock scheduler waits for progress.
    fn yield_now(&self) {
        thread::yield_now();
    }
}

/// Poll a condition for a short bounded interval.
fn wait_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(2));
    }
    panic!("condition not reached before timeout");
}

/// Build the smallest valid one-task submit backed by an owned task vector.
fn build_single_task_submit(int_mask: u32) -> (RknpuSubmit, Vec<RknpuTask>) {
    let mut tasks = vec![RknpuTask {
        int_mask,
        ..RknpuTask::default()
    }];
    let mut submit = RknpuSubmit::default();
    submit.task_number = 1;
    submit.task_base_addr = 0x2000;
    submit.task_obj_addr = tasks.as_mut_ptr() as u64;
    submit.core_mask = 0x1;
    submit.subcore_task[0].task_start = 0;
    submit.subcore_task[0].task_number = 1;
    (submit, tasks)
}

#[test]
fn submit_ioctl_copies_back_terminal_state() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    let submitter = {
        let service = service.clone();
        thread::spawn(move || {
            let (mut submit, mut tasks) = build_single_task_submit(0x100);
            submit.task_obj_addr = tasks.as_mut_ptr() as u64;
            let result =
                service.driver_ioctl(RknpuCmd::Submit, (&mut submit as *mut RknpuSubmit) as usize);
            (result, submit, tasks)
        })
    };

    wait_until(|| service.has_inflight_dispatches());
    platform.publish_completion(0, 0x100);

    let (result, submit, tasks) = submitter.join().unwrap();
    assert_eq!(result.unwrap(), 0);
    assert_eq!(submit.task_counter, 1);
    assert_eq!({tasks[0].int_status}, 0x100);
    assert_eq!(platform.spawn_count(), 1);
}

#[test]
fn submit_ioctl_reports_terminal_task_error() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    let submitter = {
        let service = service.clone();
        thread::spawn(move || {
            let (mut submit, mut tasks) = build_single_task_submit(0x100);
            submit.task_obj_addr = tasks.as_mut_ptr() as u64;
            let result =
                service.driver_ioctl(RknpuCmd::Submit, (&mut submit as *mut RknpuSubmit) as usize);
            (result, submit, tasks)
        })
    };

    wait_until(|| service.has_inflight_dispatches());
    platform.publish_completion(0, 0x200);

    let (result, submit, tasks) = submitter.join().unwrap();
    assert_eq!(
        result,
        Err(RknpuServiceError::Driver(RknpuError::TaskError))
    );
    assert_eq!(submit.task_counter, 1);
    assert_eq!({tasks[0].int_status}, 0);
}

#[test]
fn worker_spawns_only_once_across_multiple_submits() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    for _ in 0..2 {
        let submitter = {
            let service = service.clone();
            thread::spawn(move || {
                let (mut submit, mut tasks) = build_single_task_submit(0x100);
                submit.task_obj_addr = tasks.as_mut_ptr() as u64;
                let result = service
                    .driver_ioctl(RknpuCmd::Submit, (&mut submit as *mut RknpuSubmit) as usize);
                (result, submit, tasks)
            })
        };

        wait_until(|| service.has_inflight_dispatches());
        platform.publish_completion(0, 0x100);
        let (result, submit, tasks) = submitter.join().unwrap();
        assert_eq!(result.unwrap(), 0);
        assert_eq!(submit.task_counter, 1);
        assert_eq!({tasks[0].int_status}, 0x100);
    }

    assert_eq!(platform.spawn_count(), 1);
}

#[test]
fn action_ioctl_roundtrip() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    let mut action = super::RknpuUserAction {
        flags: RknpuAction::GetDrvVersion as u32,
        value: 0,
    };
    assert_eq!(
        service
            .driver_ioctl(
                RknpuCmd::Action,
                (&mut action as *mut super::RknpuUserAction) as usize,
            )
            .unwrap(),
        0
    );
    assert_ne!(action.value, 0);
}

/// Exercise MEM_CREATE/MEM_MAP/MEM_DESTROY only in environments where the DMA
/// allocator runtime is initialized.
#[test]
#[ignore = "requires dma-api runtime initialization"]
fn mem_ioctls_roundtrip_requires_dma_runtime() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    let mut mem_create = RknpuMemCreate {
        size: 0x1000,
        ..RknpuMemCreate::default()
    };
    assert_eq!(
        service
            .driver_ioctl(
                RknpuCmd::MemCreate,
                (&mut mem_create as *mut RknpuMemCreate) as usize,
            )
            .unwrap(),
        0
    );
    assert_ne!(mem_create.handle, 0);
    assert_ne!(mem_create.obj_addr, 0);

    let mut mem_map = RknpuMemMap {
        handle: mem_create.handle,
        ..RknpuMemMap::default()
    };
    assert_eq!(
        service
            .driver_ioctl(
                RknpuCmd::MemMap,
                (&mut mem_map as *mut RknpuMemMap) as usize,
            )
            .unwrap(),
        0
    );
    assert_eq!(mem_map.offset, (mem_create.handle as u64) << 12);

    let mut mem_destroy = RknpuMemDestroy {
        handle: mem_create.handle,
        obj_addr: mem_create.obj_addr,
        ..RknpuMemDestroy::default()
    };
    assert_eq!(
        service
            .driver_ioctl(
                RknpuCmd::MemDestroy,
                (&mut mem_destroy as *mut RknpuMemDestroy) as usize,
            )
            .unwrap(),
        0
    );

    let exists_after_destroy = platform
        .with_device(|dev| Ok(dev.get_phys_addr_and_size(mem_create.handle).is_some()))
        .unwrap();
    assert!(!exists_after_destroy);
}

#[test]
fn action_ioctl_rejects_unknown_opcode() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform);

    let mut action = super::RknpuUserAction {
        flags: u32::MAX,
        value: 0,
    };

    assert_eq!(
        service.driver_ioctl(
            RknpuCmd::Action,
            (&mut action as *mut super::RknpuUserAction) as usize,
        ),
        Err(RknpuServiceError::BadIoctl)
    );
}

// --- Scheduler invariant guard tests (AC-1) ---

/// Running submits are dispatched before ready submits of higher priority.
#[test]
fn running_submit_dispatched_before_higher_priority_ready() {
    use crate::{RknpuQueuedSubmit, ioctrl::RknpuSubmit};

    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    // Submit A: lower priority (higher number), two lanes — one per core.
    let mut submit_a = RknpuSubmit::default();
    submit_a.task_number = 2;
    submit_a.task_base_addr = 0x1000;
    submit_a.core_mask = 0x3;
    submit_a.priority = 10;
    submit_a.subcore_task[0].task_start = 0;
    submit_a.subcore_task[0].task_number = 1;
    submit_a.subcore_task[1].task_start = 1;
    submit_a.subcore_task[1].task_number = 1;
    let tasks_a = vec![
        RknpuTask { int_mask: 0x100, ..RknpuTask::default() },
        RknpuTask { int_mask: 0x100, ..RknpuTask::default() },
    ];

    // Submit B: higher priority (lower number), one lane on core 0.
    let mut submit_b = RknpuSubmit::default();
    submit_b.task_number = 1;
    submit_b.task_base_addr = 0x2000;
    submit_b.core_mask = 0x1;
    submit_b.priority = -10;
    submit_b.subcore_task[0].task_start = 0;
    submit_b.subcore_task[0].task_number = 1;
    let tasks_b = vec![RknpuTask { int_mask: 0x100, ..RknpuTask::default() }];

    // Enqueue A first so it enters running state on both cores.
    let id_a = service
        .enqueue_submit(RknpuQueuedSubmit::new(submit_a, tasks_a))
        .unwrap();
    wait_until(|| service.core_binding_count() == 2);

    // Now enqueue B (higher priority but ready, not running).
    let _id_b = service
        .enqueue_submit(RknpuQueuedSubmit::new(submit_b, tasks_b))
        .unwrap();

    // Running submit A should continue to occupy both cores before B is considered.
    assert_eq!(service.core_binding_count(), 2);

    // Drain A from both cores.
    platform.publish_completion(0, 0x100);
    platform.publish_completion(1, 0x100);

    // Drain A.
    service.wait_for_submit(id_a).unwrap();
    let _ = service.take_terminal_submit(id_a).unwrap();
}

/// A lane with lane_isrun=true must not be dispatched again on a second core.
#[test]
fn running_lane_not_dispatched_twice() {
    use crate::{RknpuQueuedSubmit, ioctrl::RknpuSubmit};

    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    // One submit with a single lane (slot 0) covering one task.
    let mut submit = RknpuSubmit::default();
    submit.task_number = 1;
    submit.task_base_addr = 0x1000;
    submit.core_mask = 0x3; // allow both cores
    submit.subcore_task[0].task_start = 0;
    submit.subcore_task[0].task_number = 1;
    let tasks = vec![RknpuTask { int_mask: 0x100, ..RknpuTask::default() }];

    let id = service
        .enqueue_submit(RknpuQueuedSubmit::new(submit, tasks))
        .unwrap();

    // Wait for the single lane to be dispatched on one core.
    wait_until(|| service.has_inflight_dispatches());

    // The same lane must not appear on a second core — only one core binding.
    assert_eq!(service.core_binding_count(), 1, "single-lane submit must not bind more than one core");

    platform.publish_completion(0, 0x100);
    service.wait_for_submit(id).unwrap();
    let _ = service.take_terminal_submit(id).unwrap();
}

/// A faulted submit drains all running lanes before becoming terminal.
#[test]
fn faulted_submit_drains_running_lanes_before_terminal() {
    use crate::{RknpuQueuedSubmit, ioctrl::RknpuSubmit};

    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    // Two-lane submit: lane 0 on core 0, lane 1 on core 1.
    let mut submit = RknpuSubmit::default();
    submit.task_number = 2;
    submit.task_base_addr = 0x1000;
    submit.core_mask = 0x3;
    submit.subcore_task[0].task_start = 0;
    submit.subcore_task[0].task_number = 1;
    submit.subcore_task[1].task_start = 1;
    submit.subcore_task[1].task_number = 1;
    let tasks = vec![
        RknpuTask { int_mask: 0x100, ..RknpuTask::default() },
        RknpuTask { int_mask: 0x100, ..RknpuTask::default() },
    ];

    let id = service
        .enqueue_submit(RknpuQueuedSubmit::new(submit, tasks))
        .unwrap();

    // Wait until both lanes are in-flight.
    wait_until(|| service.core_binding_count() == 2);

    // Fault lane 0 (irq_status != int_mask → task error).
    platform.publish_completion(0, 0x200);

    // Submit must NOT be terminal yet — lane 1 is still running.
    thread::sleep(Duration::from_millis(20));
    assert!(
        !service.is_submit_terminal(id),
        "submit must not be terminal while lane 1 is still running"
    );

    // Complete lane 1 normally.
    platform.publish_completion(1, 0x100);

    // Now the submit should become terminal (with an error).
    service.wait_for_submit(id).unwrap();
    let completed = service.take_terminal_submit(id).unwrap();
    assert!(completed.last_error.is_some(), "faulted submit must carry an error");
}

// --- Async API tests (AC-4) ---

/// submit_async returns immediately; poll returns None while in-flight, Some after completion.
#[test]
fn submit_async_poll_returns_none_then_some() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    let (submit, tasks) = build_single_task_submit(0x100);
    let mut handle = service
        .submit_async(crate::RknpuQueuedSubmit::new(submit, tasks))
        .unwrap();

    wait_until(|| service.has_inflight_dispatches());
    assert!(handle.poll().is_none(), "must be None while in-flight");

    platform.publish_completion(0, 0x100);
    wait_until(|| service.is_submit_terminal(handle.task_id().unwrap()));

    let result = handle.poll();
    assert!(result.is_some(), "must be Some after completion");
    assert!(handle.poll().is_none(), "exactly-once: second poll must return None");
}

/// submit_async propagates task errors through the handle.
#[test]
fn submit_async_wait_propagates_fault() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    let (submit, tasks) = build_single_task_submit(0x100);
    let handle = service
        .submit_async(crate::RknpuQueuedSubmit::new(submit, tasks))
        .unwrap();

    wait_until(|| service.has_inflight_dispatches());
    platform.publish_completion(0, 0x200); // wrong status → task error

    let result = handle.wait().unwrap();
    assert!(result.last_error.is_some(), "fault must propagate through wait");
}

/// Dropping an async handle detaches the waiter but must not abort remaining work.
#[test]
fn submit_async_handle_drop_before_completion_does_not_leak() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform.clone());

    let mut submit = RknpuSubmit::default();
    submit.task_number = 2;
    submit.task_base_addr = 0x3000;
    submit.core_mask = 0x1; // single core forces sequential dispatch
    submit.subcore_task[0].task_start = 0;
    submit.subcore_task[0].task_number = 2;
    let tasks = vec![
        RknpuTask {
            int_mask: 0x100,
            ..RknpuTask::default()
        },
        RknpuTask {
            int_mask: 0x100,
            ..RknpuTask::default()
        },
    ];
    let handle = service
        .submit_async(crate::RknpuQueuedSubmit::new(submit, tasks))
        .unwrap();
    let task_id = handle.task_id().unwrap();

    wait_until(|| service.has_inflight_dispatches());
    drop(handle); // detach before completion

    // Complete task 0. Task 1 must still be dispatched after handle drop.
    platform.publish_completion(0, 0x100);
    thread::sleep(Duration::from_millis(20));
    assert!(
        service.has_inflight_dispatches(),
        "dropping async handle must not abort remaining tasks in the submit"
    );

    // Complete task 1 and let the worker drain the detached submit.
    platform.publish_completion(0, 0x100);
    wait_until(|| !service.has_inflight_dispatches());
    assert!(!service.is_submit_terminal(task_id), "complete entry cleaned up after drop");
}

// --- Legacy removal tests (AC-3) ---

/// Removed 0x40–0x45 ioctl aliases must be rejected.
#[test]
fn submit_ioctl_rejects_removed_alias_range() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform);
    for alias in 0x40u32..=0x45 {
        assert!(
            RknpuCmd::try_from(alias).is_err(),
            "alias {:#x} must not decode to a valid command",
            alias
        );
    }
    let _ = service; // ensure service compiles
}

/// handle_submit_ioctl must reject a submit with task_base_addr == 0.
#[test]
fn submit_ioctl_rejects_zero_task_base_addr() {
    let platform = MockPlatform::new();
    let service = RknpuService::new(platform);

    let mut submit = RknpuSubmit::default();
    submit.task_number = 1;
    submit.task_base_addr = 0; // zero — must be rejected
    submit.task_obj_addr = 1;  // non-zero so the first guard passes

    let result = service.driver_ioctl(
        RknpuCmd::Submit,
        (&mut submit as *mut RknpuSubmit) as usize,
    );
    assert_eq!(result, Err(RknpuServiceError::InvalidData));
}
