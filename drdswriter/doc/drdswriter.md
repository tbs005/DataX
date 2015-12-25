# DataX DRDSWriter


---


## 1 快速介绍

DRDSWriter 插件实现了写入数据到 DRDS 的目的表的功能。在底层实现上， DRDSWriter 通过 JDBC 连接远程 DRDS 数据库的 Proxy，并执行相应的 replace into ... 的 sql 语句将数据写入 DRDS，特别注意执行的 Sql 语句是 replace into，为了避免数据重复写入，需要你的表具备主键或者唯一性索引(Unique Key)。

DRDSWriter 面向ETL开发工程师，他们使用 DRDSWriter 从数仓导入数据到 DRDS。同时 DRDSWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

DRDSWriter 通过 DataX 框架获取 Reader 生成的协议数据，通过 `replace into...`(没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用新行替换原有行所有字段) 的语句写入数据到 DRDS。DRDSWriter 累积一定数据，提交给 DRDS 的 Proxy，该 Proxy 内部决定数据是写入一张还是多张表以及多张表写入时如何路由数据。
<br />

    注意：整个任务至少需要具备 replace into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 DRDS 导入的数据。

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19880808,
                                "type": "long"
                            },
                            {
                                "value": "1988-08-08 08:08:08",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1000
                    }
                },
                "writer": {
                    "name": "DRDSWriter",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "root",
                        "password": "root",
                        "column": [
                            "id",
                            "name"
                        ],
                        "preSql": [
                            "delete from test"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://127.0.0.1:3306/datax?useUnicode=true&characterEncoding=gbk",
                                "table": [
                                    "test"
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```


### 3.2 参数说明

* **jdbcUrl**

	* 描述：目的数据库的 JDBC 连接信息。作业运行时，DataX 会在你提供的 jdbcUrl 后面追加如下属性：yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true

               注意：1、在一个数据库上只能配置一个 jdbcUrl 值
               		2、一个DRDS 写入任务仅能配置一个 jdbcUrl
                    3、jdbcUrl按照Mysql/DRDS官方规范，并可以填写连接附加控制信息，比如想指定连接编码为 gbk ，则在 jdbcUrl 后面追加属性 useUnicode=true&characterEncoding=gbk。具体请参看 Mysql/DRDS官方文档或者咨询对应 DBA。


 	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：目的数据库的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：目的数据库的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。 只能配置一个DRDS 的表名称。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["*"]

			**column配置项必须指定，不能留空！**


               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

	* 必选：是 <br />

	* 默认值：否 <br />

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。比如你想在导入数据前清空数据表中的数据，那么可以配置为:`"preSql":["delete from yourTableName"]` <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：默认为 replace，目前仅支持 replace，可以不配置。 <br />

	* 必选：否 <br />

	* 默认值：replace <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与DRDS的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：<br />

### 3.3 类型转换

类似 MysqlWriter ，目前 DRDSWriter 支持大部分 Mysql 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 DRDSWriter 针对 Mysql 类型转换列表:


| DataX 内部类型| Mysql 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint, year|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext    |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |



## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

```
CREATE TABLE `t_job` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '实例id',
  `project_id` bigint(20) NOT NULL COMMENT '所属资源组，外键',
  `pipeline_name` varchar(128) DEFAULT NULL COMMENT '所属资源组，以后变为非null',
  `execute_name` varchar(512) DEFAULT NULL COMMENT '执行脚本',
  `context` text NOT NULL COMMENT 'job的配置信息',
  `trace_id` varchar(128) DEFAULT NULL COMMENT '外界标志',
  `submit_user` varchar(128) DEFAULT NULL COMMENT '提交的用户',
  `submit_time` datetime DEFAULT NULL COMMENT '提交时间',
  `submit_ip` varchar(64) DEFAULT NULL COMMENT '实例提交的客户端ip',
  `start_time` datetime DEFAULT NULL COMMENT '开始执行时间',
  `end_user` varchar(128) DEFAULT NULL COMMENT '结束的用户',
  `end_time` datetime DEFAULT NULL COMMENT '结束时间',
  `log_url` varchar(4096) DEFAULT NULL COMMENT '实例运行log的地址',
  `execute_id` varchar(256) DEFAULT NULL COMMENT '执行id',
  `execute_ip` varchar(64) DEFAULT NULL COMMENT '执行所在机器ip',
  `state` tinyint(3) unsigned DEFAULT '255' COMMENT '实例状态，0-success，1-submit，2-init，3-run，4-fail，5-kill，255-unknown',
  `stage` float DEFAULT '0' COMMENT '实例运行进度',
  `total_records` bigint(20) unsigned DEFAULT '0' COMMENT '实例总条数',
  `total_bytes` bigint(20) unsigned DEFAULT '0' COMMENT '实例总bytes数',
  `speed_records` bigint(20) unsigned DEFAULT '0' COMMENT '实例运行速度',
  `speed_bytes` bigint(20) unsigned DEFAULT '0' COMMENT '实例运行bytes速度',
  `error_records` bigint(20) unsigned DEFAULT '0' COMMENT '实例错误总条数',
  `error_bytes` bigint(20) unsigned DEFAULT '0' COMMENT '实例错误总bytes数',
  `error_message` text COMMENT '实例错误信息',
  PRIMARY KEY (`id`),
  KEY `idx_project` (`project_id`),
  KEY `idx_pipeline_name` (`pipeline_name`),
  KEY `idx_trace_id` (`trace_id`),
  KEY `idx_submit_time` (`submit_time`)
) ENGINE=InnoDB AUTO_INCREMENT=645299 DEFAULT CHARSET=utf8 COMMENT='实例的信息表'
```

单行记录类似于：

```
           id: 100605
   project_id: 112
pipeline_name: jcs_project_128105
 execute_name: NULL
      context: {"configuration":{"reader":{"parameter":{"*password":"NTdjMknTWnAlCCyaG3EVQg==","column":"`pugId`,`gmtCreated`,`gmtModified`,`gmtAuthTime`,`gmtLeaveTime`,`merchantId`,`token`,`mac`,`os`,`oauth`,`oauthInfo`","database":"witown","error-limit":"1","ip":"rdsyi7biayi7bia.mysql.rds.aliyuncs.com","port":"3306","table":"wi_pug_106","username":"rnd","where":""},"plugin":"mysql"},"writer":{"parameter":{"*access-key":"+oN7h69a9T64z1fas0CNDWmVeSsIF4i5a8s8HA5HNjo=","access-id":"IlUQ8E7i3CFFbGax","error-limit":"1","partition":"","project":"witown_rds","table":"wi_pug_106"},"plugin":"odps"}},"type":"datax"}
     trace_id: NULL
  submit_user: 128105
  submit_time: 2014-12-12 11:36:27
    submit_ip: 127.0.0.1
   start_time: 2014-12-12 11:36:27
     end_user: NULL
     end_time: 2014-12-12 11:36:41
      log_url: /20141212/cdp/11-36-27/hwdbvm3nju4qcgadwgurv445/
   execute_id: T3_0000184404
   execute_ip: oxsgateway04.cloud.et1
        state: 4
        stage: 0
total_records: 544
  total_bytes: 42819
speed_records: 0
  speed_bytes: 0
error_records: 0
  error_bytes: NULL
error_message: Code:[OdpsWriter-01], Description:[您配置的值不合法.].  - 数据源读取的列数是:11 大于目的端的列数是:10 , DataX 不允许这种行为, 请检查您的配置.
```

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 24核 Intel(R) Xeon(R) CPU E5-2430 0 @ 2.20GHz
	2. mem: 96GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* DRDS数据库机器参数为:
        1. 两个实例，每个实例上8个分库，每个分库上一张分表，共计16个分库16张分表
	2. mem: 1200M
	3. 磁盘: 40960M
	4. mysql类型: MySQL5.6
        5. 最大连接数: 300
        6. 最大IOPS: 600

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError


### 4.2 测试报告

压测基于jdbcUrl里添加了选项`rewriteBatchedStatements=true`<br />

#### 4.2.1 replace测试报告

| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DRDS QPS| 备注 |
|--------|--------| --------|--------|--------|--------|--------|--------|
| 1 | 128 | 260 | 0.39 | 0.34 | 0.03 | 2 | |
| 1 | 512 | 256 | 0.36 | 0.38 | 0.03 | 1 | |
| 1 | 1024 | 256 | 0.35 | max:0.81 min:0.002 | 0.36 | 0 | 波动很大 |
| 4 | 128 | 1011 | 1.22 | 1.75 | 0.14 | 7 | |
| 4 | 512 | 1024 | 1.27 | 1.84 | 0.06 | 1 | |
| 4 | 1024 | 1024 | 1.3 | max:2.82 min:0.76 | 0.23 | 1 | |
| 8 | 128 | 1190 | 1.6 | 2.26 | 0.15 | 6 | |
| 8 | 512 |  |  |  |  |  | IndexOutOfBoundsException报错 |

初步定位报错是drds那边通过sql去获取表元信息的时候，返回结果为空

#### 4.2.2 insert ignore测试报告

| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DRDS QPS| 备注 |
|--------|--------| --------|--------|--------|--------|--------|--------|
| 1 | 128 | 2444 | 3.43 | 3.41 | 0.13 | 12 | |
| 1 | 512 | 2099 | 3.00 | 2.99 | 0.13 | 3 | |
| 1 | 1024 | 2048 | 2.91 | 3.06 | 0.17 | 1 | |
| 4 | 128 | 7923 | 8.75 | 10.7 | 0.17 | 26 | |
| 4 | 512 | 8662 | 10.05 | 11.92 | 0.17 | 7 | |
| 4 | 1024 | 8211 | 10.20 | 12.74 | 0.29 | 3 | |
| 8 | 128 | | | | | | 抛错IndexOutOfBoundsException |

#### 4.2.3 单条小数据数据量较小情况(insert ignore插入模式)
```
建表语句
CREATE TABLE `tddl_users` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL,
  `gmt_modified` datetime NOT NULL,
  `name` varchar(200) NOT NULL,
  `address` varchar(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=gbk;

数据值
sun,aaa,2015-04-21 00:00:00,2015-04-21 01:00:00,kkk
```

| 通道数|  批量提交行数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡流出流量(MB/s)|DataX机器运行负载|DRDS QPS| 备注 |
|--------|--------| --------|--------|--------|--------|--------|--------|
| 1 | 1 | 108 | 0.005 | 0.042 | 0.08 | 110 | |
| 1 | 32 | 2611 | 0.117 | 0.188 | 0.20 | 42 | |
| 1 | 128 | 8012 | 0.36 | 0.531 | 0.4 | 37 | |
| 1 | 512 | 16025 | 0.718 | 1.12 | 0.15 | 27 | |
| 1 | 1024 | 19046 | 0.85 | 1.2 | 0.30 | 13 | |
| 1 | 40960 | 22595 | 1.01 | 1.31 | 1.27 | 1 | |

#### 4.2.4 性能小节

单条数据量较大情况下：

1. batchSize的增加对性能影响不大，且太多会导致网络波动太大，建议batchSize设置为128

2. 并发对写入性能影响很大，但到了8个并发就会抛错IndexOutOfBoundsException，`感觉是datax内部代码有问题`

3. insert ignore的性能比replace好很多，用户可以根据业务场景来选取insert ignore

4. 以上性能均是在rewriteBatchedStatements=true情况下测试的，需要datax把该选项加入到默认配置中

单条数据量较小情况下

5. batchSize设置大一点影响还是蛮大的

## 5 约束限制


## FAQ

***

**Q: DRDSWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
