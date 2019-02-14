![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/DataX-logo.jpg)



# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。



# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。



# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/alibaba/DataX/wiki/DataX-Introduction)



# Quick Start

##### Download [DataX下载地址](http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz)

##### 请点击：[Quick Start](https://github.com/alibaba/DataX/wiki/Quick-Start)
* [配置示例：从MySQL读取数据 写入ODPS](https://github.com/alibaba/DataX/wiki/Quick-Start)
* [配置定时任务](https://github.com/alibaba/DataX/wiki/%E9%85%8D%E7%BD%AE%E5%AE%9A%E6%97%B6%E4%BB%BB%E5%8A%A1%EF%BC%88Linux%E7%8E%AF%E5%A2%83%EF%BC%89)
* [动态传入参数](https://github.com/alibaba/DataX/wiki/%E5%8A%A8%E6%80%81%E4%BC%A0%E5%85%A5%E5%8F%82%E6%95%B0)



# Support Data Channels

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入，目前支持数据如下图，详情请点击：[DataX数据源参考指南](https://github.com/alibaba/DataX/wiki/DataX-all-data-channels)

| 类型           | 数据源        | Reader(读) | Writer(写) |
| ------------ | ---------- | :-------: | :-------: |
| RDBMS 关系型数据库 | Mysql      |     √     |     √     |
|              | Oracle     |     √     |     √     |
|              | SqlServer  |     √     |     √     |
|              | Postgresql |     √     |     √     |
|              | 达梦         |     √     |     √     |
| 阿里云数仓数据存储    | ODPS       |     √     |     √     |
|              | ADS        |           |     √     |
|              | OSS        |     √     |     √     |
|              | OCS        |     √     |     √     |
| NoSQL数据存储    | OTS        |     √     |     √     |
|              | Hbase0.94  |     √     |     √     |
|              | Hbase1.1   |     √     |     √     |
|              | MongoDB    |     √     |     √     |
| 无结构化数据存储     | TxtFile    |     √     |     √     |
|              | FTP        |     √     |     √     |
|              | HDFS       |     √     |     √     |


# 我要开发新的插件
请点击：[DataX插件开发宝典](https://github.com/alibaba/DataX/wiki/DataX%E6%8F%92%E4%BB%B6%E5%BC%80%E5%8F%91%E5%AE%9D%E5%85%B8)

# 项目成员

核心Contributions:  光戈、一斅、祁然、云时

感谢天烬、巴真对DataX做出的贡献。

# License

This software is free to use under the Apache License [Apache license](https://github.com/alibaba/DataX/blob/master/license.txt).

# 
请及时提出issue给我们。请前往：[DataxIssue](https://github.com/alibaba/DataX/issues)

```
长期招聘 联系邮箱：hanfa.shf@alibaba-inc.com
【JAVA开发职位】
职位名称：JAVA资深开发工程师/专家/高级专家
工作年限 : 2年以上
学历要求 : 本科（如果能力靠谱，这些都不是条件）
期望层级 : P6/P7/P8

岗位描述：
    1. 负责阿里云大数据平台（数加）的开发设计。 
    2. 负责面向政企客户的大数据相关产品开发；
    3. 利用大规模机器学习算法挖掘数据之间的联系，探索数据挖掘技术在实际场景中的产品应用 ；
    4. 一站式大数据开发平台
    5. 大数据任务调度引擎
    6. 任务执行引擎
    7. 任务监控告警
    8. 海量异构数据同步

岗位要求：
    1. 拥有3年以上JAVA Web开发经验；
    2. 熟悉Java的基础技术体系。包括JVM、类装载、线程、并发、IO资源管理、网络；
    3. 熟练使用常用Java技术框架、对新技术框架有敏锐感知能力；深刻理解面向对象、设计原则、封装抽象；
    4. 熟悉HTML/HTML5和JavaScript；熟悉SQL语言；
    5. 执行力强，具有优秀的团队合作精神、敬业精神；
    6. 深刻理解设计模式及应用场景者加分；
    7. 具有较强的问题分析和处理能力、比较强的动手能力，对技术有强烈追求者优先考虑；
    8. 对高并发、高稳定可用性、高性能、大数据处理有过实际项目及产品经验者优先考虑；
    9. 有大数据产品、云产品、中间件技术解决方案者优先考虑。
````
钉钉用户请扫描以下二维码进行讨论：

![DataX-OpenSource-Dingding](https://raw.githubusercontent.com/alibaba/DataX/master/images/datax-opensource-dingding.png)


