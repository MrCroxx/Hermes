# Hermes

*本项目为本科毕业设计项目，仅供交流学习使用，****强烈不建议****在任何生产环境下使用。论文内容请通过邮件联系[本人](mailto://mrcroxx@outlook.com)索取。*

## Introduction

Hermes是一个基于Multi-Raft算法的多数据流并发采集系统，适用于工业机器人云平台数据采集场景。

在测试中，Hermes能在模拟的 6 个树莓派成本的服务器组成的集群中能够并发采集多达 145 个工业机器人产生的 250Hz 的数据流，且能保证集群大多数节点可用时数据流采集服务可用，可以在 400ms 以内完成故障节点的恢复与数据同步，从而实现对云平台服务短时间不可用的容错处理。

## Architecture

![Hermes 架构设计图](/static/arch.png "Hermes 架构设计图")

Hermes系统由三个子系统组成：数据流采集系统Hermes Worker Cluster、数据生产者客户端Hermes Producer、数据消费者客户端Hermes Consumer。其中，Hermes Worker Cluster为Hermes的核心系统。

![Hermes 重要类与结构](/static/class.png "Hermes 重要类与接口")

## Usage

Hermes Worker Cluster中的Pod已经实现好，用户仅需启动时指定配置文件即可使用。

Hermes Producer是生产者库，其为用户提供了低层的逐条推送的接口和高层的自动分批推送接口。用户可以通过这两个接口实现自己的生产者。

Hermes Consumer是消费者样例，用户仅需参考该样例实现自己的消费者即可。

Hermes提供了管理脚本样例hermesm，用户可通过修改本项目根目录下的hermesm-example.sh中的配置来使用。其可在一台机器上操作集群，包括集群的编译、自动化配置、部署、启动、停止、监控等。

Hermes会根据配置生成Hermes WebUI，用户可在Hermes WebUI中监控集群、创建Zone、转移Zone Leader、监控数据采集、检查集群配置等。

## Demo

[Demo视频](https://www.bilibili.com/video/BV1zt4y1i7Ar/)


![Hermse WebUI Overview](/static/ui-overview.png "Hermse WebUI Overview")

## Learn More

更多详情详见本人毕业论文，论文内容请通过邮件联系[本人](mailto://mrcroxx@outlook.com)索取。