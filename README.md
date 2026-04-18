# `rknpu`

1. 低层驱动 `Rknpu`：负责 MMIO、GEM/DMA、寄存器编程、单次 task dispatch、IRQ completion 回收。
2. 高层 service `RknpuService<P>`：负责 RKNPU 专用 ioctl、blocking submit 调度、waiter/worker 协作，以及平台能力适配。

## 当前能力

- RK3588 三核 NPU 的 MMIO 驱动
- 基于 `rknpu-regs` 的类型安全寄存器访问
- GEM/DMA 缓冲区创建、销毁、同步和 mmap offset 查询
- `RknpuTask` / `RknpuSubmit` ABI 结构
- 单 core / 单 task 的 `submit_ioctrl_step()` dispatch 原语
- IRQ handler 发布 raw per-core completion
- service 层 blocking submit 调度器
- RKNPU 专用 ioctl 分发：`Submit`、`MemCreate`、`MemMap`、`MemDestroy`、`MemSync`、`Action`

## crate 结构

```text
src/
├── lib.rs               顶层低层驱动入口 `Rknpu`
├── ioctrl.rs            ioctl ABI 结构与单步 dispatch 原语
├── gem.rs               DMA/GEM 缓冲区管理
├── job.rs               作业模式与提交相关结构
├── registers/           寄存器访问包装
├── task/                task 构建与调度数据结构
├── status/              状态码与状态寄存器定义
└── service/
    ├── mod.rs           `RknpuService<P>` 高层入口
    ├── ioctl.rs         RKNPU 专用 ioctl 分发
    ├── scheduler.rs     blocking submit 调度器
    ├── platform.rs      OS trait 边界
    ├── error.rs         service 层错误类型
    └── tests.rs         service 层单测
```

## 推荐使用方式：接入 `RknpuService<P>`

正常接入 OS 时，建议走 `RknpuService<P>`。这样 RKNPU 专用 ioctl、submit 调度和 worker/waiter 逻辑都留在 crate 内，OS 侧只做设备节点、通用 DRM 分发、mmap 和平台 trait 实现。

```

### 1. 创建低层 `Rknpu`

平台先从设备树或板级配置里拿到 NPU 三个 core 的 MMIO 区间，把它们映射成内核虚拟地址，然后创建 `Rknpu`：

```rust
use core::ptr::NonNull;
use rknpu::{Rknpu, RknpuConfig, RknpuType};

let base_addrs: [NonNull<u8>; 3] = [
    /* core0 mapped mmio */,
    /* core1 mapped mmio */,
    /* core2 mapped mmio */,
];

let config = RknpuConfig {
    rknpu_type: RknpuType::Rk3588,
};

let mut npu = Rknpu::new(&base_addrs, config);
npu.open()?;
```

`base_addrs` 必须是已经映射好的 MMIO 虚拟地址，不是裸物理地址。

### 2. 注册 IRQ handler

service 调度器依赖 IRQ handler 把每个 core 的完成状态发布到驱动内部的原子变量里。平台 IRQ 框架里需要为每个 core 注册一个 handler：

```rust
let irq_handler0 = npu.new_irq_handler(0);

register_irq(core0_irq, move || {
    let _observed_status = irq_handler0.handle();
    /* 平台 IRQ 框架返回 handled */
});
```

`RknpuIrqHandler::handle()` 只读取并清除硬件中断，然后把 raw status 发布到 core 的 completion shadow。后续由 scheduler 在普通线程上下文里调用 `harvest_completed_dispatches()` 回收。

如果直接使用低层 `Rknpu::submit()`，还可以通过 `set_wait_fn()` 切换到 WFI 等中断等待模式：

```rust
npu.set_wait_fn(wait_for_irqs);
```

service 层的 blocking submit 不靠 `set_wait_fn()` 阻塞调用线程，而是靠每个 submit 自己的 waiter。

### 3. 实现平台 trait

`RknpuService<P>` 要求平台类型实现 `RknpuPlatform`。这个组合 trait 来自下面几类能力：

| trait | 平台需要提供什么 |
| --- | --- |
| `RknpuDeviceAccess` | 用 `with_device()` 临时借用底层 `Rknpu` |
| `RknpuUserMemory` | `copy_from_user()` / `copy_to_user()` |
| `RknpuSubmitWaiter` | 单个 submit 的阻塞等待和完成唤醒 |
| `RknpuWorkerSignal` | 全局 worker 的 sleep/wake 信号 |
| `RknpuSchedulerRuntime` | 创建 waiter/signal，启动 worker，提供 `yield_now()` |

一个平台适配层通常长这样：

```rust
use rknpu::{
    Rknpu, RknpuError,
    service::{
        RknpuDeviceAccess, RknpuSchedulerRuntime, RknpuServiceError,
        RknpuUserMemory,
    },
};

struct Platform {
    /* 这里放 Rknpu 的锁、用户态拷贝能力、任务系统入口等 */
}

impl RknpuDeviceAccess for Platform {
    fn with_device<T, F>(&self, f: F) -> Result<T, RknpuServiceError>
    where
        F: FnOnce(&mut Rknpu) -> Result<T, RknpuError>,
    {
        let mut npu = self.lock_rknpu();
        f(&mut npu).map_err(RknpuServiceError::from)
    }
}

impl RknpuUserMemory for Platform {
    fn copy_from_user(
        &self,
        dst: *mut u8,
        src: *const u8,
        size: usize,
    ) -> Result<(), RknpuServiceError> {
        self.user_copy_from(dst, src, size)
    }

    fn copy_to_user(
        &self,
        dst: *mut u8,
        src: *const u8,
        size: usize,
    ) -> Result<(), RknpuServiceError> {
        self.user_copy_to(dst, src, size)
    }
}
```

还需要实现 `RknpuSchedulerRuntime`，把 `new_waiter()`、`new_worker_signal()`、`spawn_worker()` 和 `yield_now()` 接到本 OS 的 wait queue、event 和任务系统上。StarryOS 当前就是这条接线方式。

### 4. 创建设备 service

平台 trait 实现好以后，就可以创建 service：

```rust
use rknpu::service::RknpuService;

let service = RknpuService::new(platform);
```

`RknpuService` 本身不是 crate 内全局单例。外层 OS 可以按自己的设备模型决定放在哪里，比如放进 `/dev/rknpu` 对应的设备对象里，或者放进全局设备表里。

### 5. 在 ioctl 分发中转发 RKNPU 命令

OS 侧收到 ioctl 后，先从 ioctl `nr` 字段解析出 RKNPU 命令，再转给 service。这里的 `nr` 是已经解码出的 command number，不是完整的 encoded ioctl 值。

```rust
use core::convert::TryFrom;
use rknpu::service::{RknpuCmd, RknpuServiceError};

fn rknpu_ioctl(service: &RknpuService<Platform>, nr: u32, arg: usize) -> Result<usize, OsError> {
    let op = RknpuCmd::try_from(nr).map_err(|_| OsError::InvalidIoctl)?;
    service
        .driver_ioctl(op, arg)
        .map_err(map_rknpu_service_error)
}
```

`arg` 是用户态 ABI 结构的地址。service 内部会通过 `RknpuUserMemory` 拷入/拷出，不会直接解引用用户地址。

### 6. 用户态典型调用顺序

用户态或 runtime 的常见流程如下：

```text
1. ioctl(MemCreate, RknpuMemCreate)
   分配输入、输出、权重、regcmd、task array 等 DMA buffer。

2. ioctl(MemMap, RknpuMemMap) + mmap()
   先拿到 mmap offset，再由 OS 的 VFS mmap 路径完成映射。

3. 写入输入/权重/regcmd/task array
   CPU 侧填充 buffer 内容，RknpuTask[] 地址写入 RknpuSubmit.task_obj_addr。

4. ioctl(MemSync, RknpuMemSync)
   对非一致性 DMA 平台用于 cache 同步；当前一致性内存实现里是 no-op。

5. ioctl(Submit, RknpuSubmit)
   service 拷入 submit header 和 RknpuTask[]，入队并阻塞等待完成。

6. 返回后读取输出 buffer
   Submit 会回写 RknpuSubmit.task_counter 和每个 RknpuTask.int_status。

7. ioctl(MemDestroy, RknpuMemDestroy)
   释放不再使用的 GEM/DMA buffer。
```

`Submit` 是阻塞语义，但内部不是调用线程自己一路跑完。调用线程睡在 per-submit waiter 上，后台 worker 负责 dispatch、harvest 和后续补发。

## 低层直接使用方式

如果不需要 service 层，也可以直接使用低层 `Rknpu`：

```rust
use rknpu::{Rknpu, RknpuAction, RknpuMemCreate};

let mut npu = Rknpu::new(&base_addrs, config);
npu.open()?;

let hw_version = npu.get_hw_version();
let drv_version = npu.action(RknpuAction::GetDrvVersion, 0)?;

let mut create = RknpuMemCreate {
    size: 4096,
    ..Default::default()
};
npu.create(&mut create)?;

/* 填充 DMA buffer 和 task 描述符 */

npu.destroy(create.handle);
```

低层路径更适合 bare-test 或 bring-up。正常 OS 设备节点建议走 `RknpuService<P>`，因为它已经处理了用户态拷贝、blocking submit、waiter 和 worker 调度。

## Submit 使用要点

`RknpuSubmit` 是 `Submit` ioctl 的 header。几个字段需要特别注意：

| 字段 | 用途 |
| --- | --- |
| `flags` | 作业模式标志，对应 `JobMode` 位 |
| `timeout` | submit 等待超时，单位毫秒 |
| `task_number` | `RknpuTask[]` 总数量，必须非零 |
| `task_counter` | 输出字段，返回实际完成 task 数 |
| `priority` | 调度优先级，数值越小越优先 |
| `task_obj_addr` | 用户态可访问的 `RknpuTask[]` CPU 地址，必须非零 |
| `task_base_addr` | `RknpuTask[]` 的 DMA 地址，旧用户态可能传 0，当前会保持兼容 |
| `core_mask` | 允许使用的 NPU core bitmask，常见值为 `0x1/0x2/0x4/0x7` |
| `subcore_task` | 每个 lane 的 task 范围，非空项决定多核/多 lane 切分 |

如果 `subcore_task[]` 全为空，队列层会把它归一化成 lane0 执行 `[0, task_number)`。如果用户显式填写了非空 lane，只会使用这些非空范围。completion 返回后，service 会把最终 `RknpuTask[]` 拷回用户态，用户可以检查每个 task 的 `int_status`。

## 支持的 ioctl/ioctrl 接口

service 层公开入口是：

```rust
pub fn driver_ioctl(&self, op: RknpuCmd, arg: usize) -> Result<usize, RknpuServiceError>
```

`RknpuCmd::try_from(nr)` 当前接受两组 command number：`0x00..0x05` 和兼容别名 `0x40..0x45`。

| 命令 | nr | payload | 方向 | 说明 |
| --- | --- | --- | --- | --- |
| `Action` | `0x00` / `0x40` | `RknpuUserAction` | in/out | 管理/查询类操作 |
| `Submit` | `0x01` / `0x41` | `RknpuSubmit` + `RknpuTask[]` | in/out | 阻塞式/非阻塞式（取决于wait函数） 提交任务 |
| `MemCreate` | `0x02` / `0x42` | `RknpuMemCreate` | in/out | 创建 GEM/DMA buffer |
| `MemMap` | `0x03` / `0x43` | `RknpuMemMap` | in/out | 查询 mmap offset |
| `MemDestroy` | `0x04` / `0x44` | `RknpuMemDestroy` | in | 销毁 GEM/DMA buffer |
| `MemSync` | `0x05` / `0x45` | `RknpuMemSync` | in | 同步 DMA buffer 区间 |

### `Action`

payload：

```rust
#[repr(C)]
pub struct RknpuUserAction {
    pub flags: RknpuAction,
    pub value: u32,
}
```

`flags` 是 action opcode，`value` 既可以是输入参数，也可以是输出结果。当前实现情况如下：

| action | opcode | 当前行为 |
| --- | --- | --- |
| `GetHwVersion` | `0` | 返回硬件版本 |
| `GetDrvVersion` | `1` | 返回 crate 内部驱动版本 |
| `GetFreq` / `SetFreq` | `2` / `3` | 需要平台 clock 集成，当前返回 `NotSupported` |
| `GetVolt` / `SetVolt` | `4` / `5` | 需要平台 regulator 集成，当前返回 `NotSupported` |
| `ActReset` | `6` | best-effort soft reset，清 core IRQ 状态和 completion shadow |
| `GetBwPriority` / `SetBwPriority` | `7` / `8` | 需要带宽寄存器窗口，当前返回 `NotSupported` |
| `GetBwExpect` / `SetBwExpect` | `9` / `10` | 需要带宽寄存器窗口，当前返回 `NotSupported` |
| `GetBwTw` / `SetBwTw` | `11` / `12` | 需要带宽寄存器窗口，当前返回 `NotSupported` |
| `ActClrTotalRwAmount` | `13` | 清读写统计 |
| `GetDtWrAmount` | `14` | 返回 data write 统计 |
| `GetDtRdAmount` | `15` | 返回 data read 统计 |
| `GetWtRdAmount` | `16` | 返回 weight read 统计 |
| `GetTotalRwAmount` | `17` | 返回总读写统计 |
| `GetIommuEn` | `18` | 返回当前 IOMMU enable shadow |
| `SetProcNice` | `19` | bare-metal/内核上下文下无实际 nice，当前接受并返回 0 |
| `PowerOn` / `PowerOff` | `20` / `21` | 需要平台 PM 集成，当前返回 `NotSupported` |
| `GetTotalSramSize` | `22` | 返回 variant data 里的 NBUF/SRAM 大小 |
| `GetFreeSramSize` | `23` | 当前返回同样的 NBUF/SRAM 大小 |
| `GetIommuDomainId` | `24` | 返回当前逻辑 domain id shadow |
| `SetIommuDomainId` | `25` | 设置逻辑 domain id，合法范围是 `0..16` |

### `Submit`

payload header 是 `RknpuSubmit`，task array 由 `submit.task_obj_addr` 指向：

```text
arg ─────────────► RknpuSubmit
task_obj_addr ───► RknpuTask[task_number]
```

处理流程：

1. 从用户态拷入 `RknpuSubmit`。
2. 校验 `task_number != 0` 且 `task_obj_addr != 0`。
3. 按 `task_number * sizeof(RknpuTask)` 拷入 task array。
4. 构造队列 submit 并唤醒 worker。
5. 当前调用线程睡在 per-submit waiter 上。
6. worker 按 core/lane streaming dispatch，并在 IRQ completion 后继续补发。
7. submit terminal 后，回写 `RknpuTask[]` 和 `RknpuSubmit`。

返回后需要关注：

| 输出 | 含义 |
| --- | --- |
| `RknpuSubmit.task_counter` | 成功推进的 task 数量 |
| `RknpuSubmit.hw_elapse_time` | 当前终态回写的硬件耗时字段 |
| `RknpuTask.int_status` | 每个 task 的完成中断状态 |

如果 terminal 时记录了 `last_error`，service 会把它转换成 `RknpuServiceError::Driver(err)` 返回。

### `MemCreate`

payload：

```rust
#[repr(C)]
pub struct RknpuMemCreate {
    pub handle: u32,
    pub flags: u32,
    pub size: u64,
    pub obj_addr: u64,
    pub dma_addr: u64,
    pub sram_size: u64,
    pub iommu_domain_id: i32,
    pub core_mask: u32,
}
```

输入主要是 `size`、`flags`、`iommu_domain_id`、`core_mask`。返回时填充：

| 字段 | 含义 |
| --- | --- |
| `handle` | 后续 ioctl 使用的 GEM handle |
| `obj_addr` | CPU 侧可访问地址 |
| `dma_addr` | NPU/DMA 侧访问地址 |
| `sram_size` | 实际分配大小 |

当前 GEM 实现使用 `dma-api::DVec` 分配 bidirectional DMA buffer。

### `MemMap`

payload：

```rust
#[repr(C)]
pub struct RknpuMemMap {
    pub handle: u32,
    pub reserved: u32,
    pub offset: u64,
}
```

`MemMap` 只做 handle 校验和 offset 生成。当前 offset 规则是：

```text
offset = handle << 12
```

真正把 buffer 映射到用户态，还需要 OS/VFS 的 `mmap()` 路径根据这个 offset 找回对应 GEM 对象。

### `MemDestroy`

payload：

```rust
#[repr(C)]
pub struct RknpuMemDestroy {
    pub handle: u32,
    pub reserved: u32,
    pub obj_addr: u64,
}
```

销毁以 `handle` 为准，`obj_addr` 只保留 ABI 兼容。当前实现里，如果 handle 不存在，会打印 warning 并返回成功；如果存在，就从 GEM pool 中移除。

### `MemSync`

payload：

```rust
#[repr(C)]
pub struct RknpuMemSync {
    pub flags: u32,
    pub reserved: u32,
    pub obj_addr: u64,
    pub offset: u64,
    pub size: u64,
}
```

这个接口用于 CPU 和 NPU 之间的缓存一致性同步。当前 GEM 后端使用一致性 DMA 内存，所以 `GemPool::sync()` 是 no-op。后续如果接入非一致性 DMA 平台，这里需要根据 `flags / obj_addr / offset / size` 做 flush 或 invalidate。

## 测试

### 主机侧单测

仓库里包含 service/scheduler 相关单测，主要位于：

- `src/service/tests.rs`
- `src/task/taskqueen.rs`
- `src/ioctrl.rs`
- `src/lib.rs`

这些测试覆盖 submit copy-back、terminal error、worker 单例、memory/action ioctl 分发等路径。

当前 crate 的 `.cargo/config.toml` 默认 target 是 `aarch64-unknown-none-softfloat`，测试配置偏板端 bring-up。因此主机侧单测不是 README 推荐的默认一键入口。

### 板端 bare-test

板端测试入口：

```bash
cargo install ostool
cargo test --test test -- tests --show-output --uboot
```

这个测试会在板端完成 DTB 设备发现、MMIO 映射、电源域开启、IRQ 注册和基本 matmul 提交验证。

## 当前限制

- `GetFreq / SetFreq`、`GetVolt / SetVolt`、`PowerOn / PowerOff` 还需要平台 clock、regulator、PM 集成。
- 带宽控制类 action 需要额外的 bandwidth register window，目前返回 `NotSupported`。
- `MemSync` 当前是 no-op，因为现有 DMA 分配路径使用一致性内存。
- service 对外仍是 blocking submit，内部由 worker 异步推进；如果要做真正异步 userspace API，还需要新增上层语义。
- 真实硬件稳定性和长时间运行场景还需要继续补测试。
# RKNPU
