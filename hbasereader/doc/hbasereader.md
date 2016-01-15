
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

	* 描述：TODO

	* 必选：是 <br />

	* 默认值：无 <br />

* **startRowkey**

	* 描述：TODO

	* 必选：否 <br />

	* 默认值：空 <br />

* **endRowkey**

	* 描述：TODO<br />。

	* 必选：否 <br />

	* 默认值：无 <br />

* **isBinaryRowkey**

	* 描述： <br />

	* 必选：否 <br />

	* 默认值：无 <br />

