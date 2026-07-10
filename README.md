# TDA596 Distributed Systems Labs (MIT 6.5840)

Go implementations of the MIT 6.5840 (Distributed Systems) lab series, done as part of Chalmers' **TDA596 Distributed Systems** course.

## Labs implemented

- **`mr/`** — MapReduce: a coordinator/worker framework that runs map and reduce tasks over a set of input files, with crash/timeout handling and re-assignment of failed tasks
- **`raft/`** — Raft consensus: leader election, log replication, and persistence
- **`kvraft/`** — A fault-tolerant key/value store built on top of Raft
- **`shardctrler/`** + **`shardkv/`** — A sharded key/value store with a separate shard configuration service and dynamic shard migration between replica groups
- **`porcupine/`** — Linearizability checker used by the test suites to verify correctness under concurrent operations
- **`labrpc/`**, **`labgob/`** — Supporting RPC and encoding libraries provided by the course framework

## Run

```bash
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
go run mrcoordinator.go pg-*.txt &
go run mrworker.go wc.so
```

Each lab also ships its own test suite, e.g.:

```bash
cd src/raft && go test
cd src/kvraft && go test
cd src/shardkv && go test
```

## Submission

The `Makefile` packages a lab for submission to Gradescope:

```bash
make lab1     # MapReduce
make lab2a    # Raft leader election
make lab3a    # KVRaft
make lab4a    # ShardCtrler / ShardKV
```

<details>
<summary><b>🇨🇳 中文版本（点击展开）</b></summary>

<br>

# TDA596 分布式系统课程实验（MIT 6.5840）

MIT 6.5840（分布式系统）系列实验的 Go 语言实现，作为查尔姆斯理工大学 **TDA596 分布式系统** 课程作业完成。

## 已实现的实验

- **`mr/`** — MapReduce：一个协调者/工作节点框架，负责在多个输入文件上调度 map 与 reduce 任务，支持任务超时检测与失败任务的重新分配
- **`raft/`** — Raft 共识算法：领导者选举、日志复制与持久化
- **`kvraft/`** — 基于 Raft 构建的容错键值存储
- **`shardctrler/`** + **`shardkv/`** — 分片键值存储，包含独立的分片配置服务，支持副本组之间的动态分片迁移
- **`porcupine/`** — 线性一致性检查器，测试套件用它来验证并发操作下的正确性
- **`labrpc/`**、**`labgob/`** — 课程框架提供的 RPC 与编码支持库

## 运行

```bash
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
go run mrcoordinator.go pg-*.txt &
go run mrworker.go wc.so
```

每个实验也自带测试套件，例如：

```bash
cd src/raft && go test
cd src/kvraft && go test
cd src/shardkv && go test
```

## 提交

`Makefile` 用于把某个实验打包提交到 Gradescope：

```bash
make lab1     # MapReduce
make lab2a    # Raft 领导者选举
make lab3a    # KVRaft
make lab4a    # ShardCtrler / ShardKV
```

</details>
