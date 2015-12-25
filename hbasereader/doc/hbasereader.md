
# HbaseReader 插件文档


___



## 1 快速介绍

HbaseReader 插件实现了从 Hbase 读取数据。在底层实现上，HbaseReader 通过 HBase 的 Java 客户端连接远程 HBase 服务，并通过 Scan 方式读取数据。典型示例如下：


	Scan scan = new Scan();
	scan.setStartRow(startKey);
	scan.setStopRow(endKey);

	ResultScanner resultScanner = table.getScanner(scan);
	for(Result r:resultScanner){
    	System.out.println(new String(r.getRow()));
    	for(KeyValue kv:r.raw()){
       		System.out.println(new String(kv.getValue()));
    	}
	}


HbaseReader 需要特别注意如下几点：

1、HbaseReader 中有一个必填配置项是：hbaseConfig，需要你联系 HBase PE，将hbase-site.xml 中与连接 HBase 相关的配置项提取出来，以 json 格式填入。

2、HbaseReader 中的 mode 配置项，必须填写且值只能为：normal 或者 multiVersion。当值为 normal 时，会把 HBase 中的表，当成普通二维表进行读取；当值为 multiVersion 时，会把每一个 cell 中的值，读成 DataX 中的一个 Record，Record 中的格式是：

| 第0列    | 第1列            | 第2列    | 第3列 |
| --------| ---------------- |-----     |-----  |
| rowKey  | column:qualifier| timestamp | value |


## 2 实现原理

简而言之，HbaseReader 通过 HBase 的 Java 客户端，通过 HTable, Scan, ResultScanner 等 API，读取你指定 rowkey 范围内的数据，并将读取的数据使用 DataX 自定义的数据类型拼装为抽象的数据集，并传递给下游 Writer 处理。



## 3 功能说明

### 3.1 配置样例

* 配置一个从 HBase 抽取数据到本地的作业:（normal 模式）

```
{
  "job": {
    "setting": {
      "speed": {
        //设置传输速度，单位为byte/s，DataX运行会尽可能达到该速度但是不超过它.
        "byte": 1048576
      }
      //出错限制
      "errorLimit": {
        //出错的record条数上限，当大于该值即报错。
        "record": 0,
        //出错的record百分比上限 1.0表示100%，0.02表示2%
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "hbasereader",
          "parameter": {
            "hbaseConfig": "hbase-site 文件中与连接相关的配置项，以 json 格式填写",
            "table": "hbase_test_table",
            "encoding": "utf8",
            "mode": "normal",
            "column": [
              {
                "name": "rowkey",
                "type": "string"
              },
              {
                "name": "fb:comm_result_code",
                "type": "string"
              },
              {
                "name": "fb:exchange_amount",
                "type": "string"
              },
              {
                "name": "fb:exchange_status",
                "type": "string"
              }
            ],
            "range": {
              "startRowkey": "",
              "endRowkey": ""
            },
            "isBinaryRowkey": true
          }
        },
        "writer": {
          //writer类型
          "name": "streamwriter",
          //是否打印内容
          "parameter": {
            "print": true
          }
        }
      }
    ]
  }
}

```

* 配置一个从 HBase 抽取数据到本地的作业:（ multiVersion 模式）

```

TODO

```


### 3.2 参数说明

* **hbaseConfig**

	* 描述：每个HBase集群提供给DataX客户端连接 的配置信息存放在hbase-site.xml，请联系你的HBase DBA提供配置信息，并转换为JSON格式填写如下：{"key1":"value1","key2":"value2"}。比如：{"hbase.zookeeper.quorum":"????","hbase.zookeeper.property.clientPort":"????"} 这样的形式。注意：如果是手写json，那么需要把双引号 转义为\"

	* 必选：是 <br />

	* 默认值：无 <br />

* **mode**

	* 描述：normal/multiVersion。。。toto <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：要读取的 hbase 表名（大小写敏感） <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **encoding**

	* 描述：编码方式，UTF-8 或是 GBK，用于对二进制存储的 HBase byte[] 转为 String 时 <br />

	* 必选：否 <br />

	* 默认值：UTF-8 <br />


* **column**

	* 描述：TODO。

	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

	  支持常量配置，用户需要按照如下语法格式:
	  ["id", "\`table\`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
	  TODO。

	* 必选：是 <br />

	* 默认值：无 <br />

* **startRowkey**

	* 描述：TODO

	  TODO

	* 必选：否 <br />

	* 默认值：空 <br />

* **endRowkey**

	* 描述：TODO<br />。

          TODO。

	* 必选：否 <br />

	* 默认值：无 <br />

* **isBinaryRowkey**

	* 描述： <br />

	 `当用户配置querySql时，MysqlReader直接忽略table、column、where条件的配置`，querySql优先级大于table、column、where选项。

	* 必选：否 <br />

	* 默认值：无 <br />


### 3.3 类型转换(TODO)

目前 HbaseReader 支持大部分 HBase 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 HbaseReader 针对 HBase 类型转换列表:


| DataX 内部类型| HBase 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext    |
| Date     |date, datetime, timestamp, time, year    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。
* `bit DataX属于未定义行为`。

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

	TODO


单行记录类似于：

	biz_order_id: 888888888
   	   key_value: ;orderIds:20148888888,2014888888813800;
  	  gmt_create: 2011-09-24 11:07:20
	gmt_modified: 2011-10-24 17:56:34
	attribute_cc: 1
  	  value_type: 3
    	buyer_id: 8888888
   	   seller_id: 1

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 24核 Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz
	2. mem: 48GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* Mysql数据库机器参数为:
	1. cpu: 32核 Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
	2. mem: 256GB
	3. net: 千兆双网卡
	4. disc: BTWL419303E2800RGN  INTEL SSDSC2BB800G4   D2010370

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError


### 4.2 测试报告

#### 4.2.1 单表测试报告


| 通道数| 是否按照主键切分| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡进入流量(MB/s)|DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------|--------| --------|--------|--------|--------|--------|--------|
|1| 否 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|1| 是 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|4| 否 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|4| 是 | 329733 | 32.60 | 58| 0.8 | 60| 0.76 |
|8| 否 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|8| 是 | 549556 | 54.33 | 115| 1.46 | 120| 0.78 |

说明：

1. 这里的单表，主键类型为 bigint(20),范围为：190247559466810-570722244711460，从主键范围划分看，数据分布均匀。
2. 对单表如果没有安装主键切分，那么配置通道个数不会提升速度，效果与1个通道一样。


#### 4.2.2 分表测试报告(2个分库，每个分库16张分表，共计32张分表)


| 通道数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡进入流量(MB/s)|DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------| --------|--------|--------|--------|--------|--------|
|1| 202241 | 20.06 | 31.5| 1.0 | 32 | 1.1 |
|4| 726358 | 72.04 | 123.9 | 3.1 | 132 | 3.6 |
|8|1074405 | 106.56| 197 | 5.5 | 205| 5.1|
|16| 1227892 | 121.79 | 229.2 | 8.1 | 233 | 7.3 |

## 5 约束限制

### 5.1 ?

主备同步问题指Mysql使用主从灾备，备库从主库不间断通过binlog恢复数据。由于主备数据同步存在一定的时间差，特别在于某些特定情况，例如网络延迟等问题，导致备库同步恢复的数据与主库有较大差别，导致从备库同步的数据不是一份当前时间的完整镜像。

针对这个问题，我们提供了preSql功能，该功能待补充。

### 5.2 ?

Mysql在数据存储划分中属于RDBMS系统，对外可以提供强一致性数据查询接口。例如当一次同步任务启动运行过程中，当该库存在其他数据写入方写入数据时，MysqlReader完全不会获取到写入更新数据，这是由于数据库本身的快照特性决定的。关于数据库快照特性，请参看[MVCC Wikipedia](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)

上述是在MysqlReader单线程模型下数据同步一致性的特性，由于MysqlReader可以根据用户配置信息使用了并发数据抽取，因此不能严格保证数据一致性：当MysqlReader根据splitPk进行数据切分后，会先后启动多个并发任务完成数据同步。由于多个并发任务相互之间不属于同一个读事务，同时多个并发任务存在时间间隔。因此这份数据并不是`完整的`、`一致的`数据快照信息。

针对多线程的一致性快照需求，在技术上目前无法实现，只能从工程角度解决，工程化的方式存在取舍，我们提供几个解决思路给用户，用户可以自行选择：

1. 使用单线程同步，即不再进行数据切片。缺点是速度比较慢，但是能够很好保证一致性。

2. 关闭其他数据写入方，保证当前数据为静态数据，例如，锁表、关闭备库同步等等。缺点是可能影响在线业务。

### 5.3 ?

Mysql本身的编码设置非常灵活，包括指定编码到库、表、字段级别，甚至可以均不同编码。优先级从高到低为字段、表、库、实例。我们不推荐数据库用户设置如此混乱的编码，最好在库级别就统一到UTF-8。

MysqlReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此MysqlReader不需用户指定编码，可以自动获取编码并转码。

对于Mysql底层写入编码和其设定的编码不一致的混乱情况，MysqlReader对此无法识别，对此也无法提供解决方案，对于这类情况，`导出有可能为乱码`。

### 5.4 ?

MysqlReader使用JDBC SELECT语句完成数据抽取工作，因此可以使用SELECT...WHERE...进行增量数据抽取，方式有多种：

* 数据库在线应用写入数据库时，填充modify字段为更改时间戳，包括新增、更新、删除(逻辑删)。对于这类应用，MysqlReader只需要WHERE条件跟上一同步阶段时间戳即可。
* 对于新增流水型数据，MysqlReader可以WHERE条件后跟上一阶段最大自增ID即可。

对于业务上无字段区分新增、修改数据情况，MysqlReader也无法进行增量数据同步，只能同步全量数据。


## 6 FAQ

***

**Q: ??同步报错，报错信息为XXX**

 A: 网络或者权限问题，请使用 HBase shell 命令行测试：

    TODO

如果上述命令也报错，那可以证实是环境问题，请联系你的 PE。
