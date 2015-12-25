# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、SQL Server、Oracle、PostgreSQL、HDFS、Hive、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。

# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

# System Requirements

- Linux
- [JDK(1.6以上，推荐1.6) ](http://www.oracle.com/technetwork/cn/java/javase/downloads/index.html) 
- [Python(推荐Python2.6.X) ](https://www.python.org/downloads/)
- [Apache Maven 3.x](https://maven.apache.org/download.cgi) (Compile DataX)

# Quick Start

* 工具部署
  
  * 方法一、直接下载DataX工具包：[DataX](https://github.com/alibaba/DataX)
    
    下载后解压至本地某个目录，进入bin目录，即可运行同步作业：
    
    ``` shell
    $ cd  {YOUR_DATAX_HOME}/bin
    $ python datax.py {YOUR_JOB.json}
    ```
    
  * 方法二、下载DataX源码，自己编译：[DataX源码](https://github.com/alibaba/DataX)
    
    (1)、下载DataX源码：
    
    ``` shell
    $ git clone git@github.com:alibaba/DataX.git
    ```
    
    (2)、通过maven打包：
    
    ``` shell
    $ cd  {DataX_source_code_home}
    $ mvn -U clean package assembly:assembly -Dmaven.test.skip=true
    ```
    
    打包成功，日志显示如下：
    
    ``` 
    [INFO] BUILD SUCCESS
    [INFO] -----------------------------------------------------------------
    [INFO] Total time: 08:12 min
    [INFO] Finished at: 2015-12-13T16:26:48+08:00
    [INFO] Final Memory: 133M/960M
    [INFO] -----------------------------------------------------------------
    ```
    
    打包成功后的DataX包位于 {DataX_source_code_home}/target/datax/datax/ ，结构如下：
    
    ``` shell
    $ cd  {DataX_source_code_home}
    $ ls ./target/datax/datax/
    bin		conf		job		lib		log		log_perf	plugin		script		tmp
    ```


* 配置示例：从stream读取数据并打印到控制台
  
  * 第一步、创建创业的配置文件（json格式）
    
    可以通过命令查看配置模板： python datax.py -r {YOUR_READER} -w {YOUR_WRITER}
    
    ``` shell
    $ cd  {YOUR_DATAX_HOME}/bin
    $  python datax.py -r streamreader -w streamwriter
    DataX (UNKNOWN_DATAX_VERSION), From Alibaba !
    Copyright (C) 2010-2015, Alibaba Group. All Rights Reserved.
    Please refer to the streamreader document:
        https://github.com/alibaba/DataX/blob/master/streamreader/doc/streamreader.md 
    
    Please refer to the streamwriter document:
         https://github.com/alibaba/DataX/blob/master/streamwriter/doc/streamwriter.md 
     
    Please save the following configuration as a json file and  use
         python {DATAX_HOME}/bin/datax.py {JSON_FILE_NAME}.json 
    to run the job.
    
    {
        "job": {
            "content": [
                {
                    "reader": {
                        "name": "streamreader", 
                        "parameter": {
                            "column": [], 
                            "sliceRecordCount": ""
                        }
                    }, 
                    "writer": {
                        "name": "streamwriter", 
                        "parameter": {
                            "encoding": "", 
                            "print": true
                        }
                    }
                }
            ], 
            "setting": {
                "speed": {
                    "channel": ""
                }
            }
        }
    }
    ```
    
    根据模板配置json如下：
    
    ``` json
    #stream2stream.json
    {
      "job": {
        "content": [
          {
            "reader": {
              "name": "streamreader",
              "parameter": {
                "sliceRecordCount": 10,
                "column": [
                  {
                    "type": "long",
                    "value": "10"
                  },
                  {
                    "type": "string",
                    "value": "hello，你好，世界-DataX"
                  }
                ]
              }
            },
            "writer": {
              "name": "streamwriter",
              "parameter": {
                "encoding": "UTF-8",
                "print": true
              }
            }
          }
        ],
        "setting": {
          "speed": {
            "channel": 5
           }
        }
      }
    }
    ```
    
  * 第二步：启动DataX
    
    ``` shell
    $ cd {YOUR_DATAX_DIR_BIN}
    $ python datax.py ./stream2stream.json 
    ```
    
    同步结束，显示日志如下：
    
    ``` shell
    ...
    2015-12-17 11:20:25.263 [job-0] INFO  JobContainer - 
    任务启动时刻                    : 2015-12-17 11:20:15
    任务结束时刻                    : 2015-12-17 11:20:25
    任务总计耗时                    :                 10s
    任务平均流量                    :              205B/s
    记录写入速度                    :              5rec/s
    读出记录总数                    :                  50
    读写失败总数                    :                   0
    ```

# Support Data Channels

目前DataX支持的数据源有:

### Reader

> **Reader实现了从数据存储系统批量抽取数据，并转换为DataX标准数据交换协议，DataX任意Reader能与DataX任意Writer实现无缝对接，达到任意异构数据互通之目的。**

**RDBMS 关系型数据库**

* [MysqlReader](https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md): 使用JDBC批量抽取Mysql数据集。
* [OracleReader](https://github.com/alibaba/DataX/blob/master/oraclereader/doc/oraclereader.md): 使用JDBC批量抽取Oracle数据集。
* [SqlServerReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-sqlserverreader): 使用JDBC批量抽取SqlServer数据集
* [PostgresqlReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-pgreader): 使用JDBC批量抽取PostgreSQL数据集
* [DrdsReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-drdsreader): 针对公有云上DRDS的批量数据抽取工具。

**数仓数据存储**

* [ODPSReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-odpsreader): 使用ODPS Tunnel SDK批量抽取ODPS数据。

**NoSQL数据存储**

* [OTSReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-otsreader): 针对公有云上OTS的批量数据抽取工具。
* [HBaseReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-hbasereader): 针对 HBase 0.94版本的在线数据抽取工具

**无结构化数据存储**

* [TxtFileReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-txtfilereader): 读取(递归/过滤)本地文件。
* [FtpReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-ftpreader): 读取(递归/过滤)远程ftp文件。
* [HdfsReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-hdfsreader): 针对Hdfs文件系统中textfile和orcfile文件批量数据抽取工具。 
* [OssReader](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-ossreader): 针对公有云OSS产品的批量数据抽取工具。
* StreamReader

### Writer

----

> **Writer实现了从DataX标准数据交换协议，翻译为具体的数据存储类型并写入目的数据存储。DataX任意Writer能与DataX任意Reader实现无缝对接，达到任意异构数据互通之目的。**

----

**RDBMS 关系型数据库**

* [MysqlWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-mysqlwriter): 使用JDBC(Insert,Replace方式)写入Mysql数据库
* [OracleWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-oraclewriter): 使用JDBC(Insert方式)写入Oracle数据库
* [PostgresqlWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-pgwriter): 使用JDBC(Insert方式)写入PostgreSQL数据库
* [SqlServerWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-sqlserverwriter): 使用JDBC(Insert方式)写入sqlserver数据库
* [DrdsWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-drdswriter): 使用JDBC(Replace方式)写入Drds数据库

**数仓数据存储**

* [ODPSWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-odpswriter): 使用ODPS Tunnel SDK向ODPS写入数据。
* [ADSWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-adswriter): 使用ODPS中转将数据导入ADS。

**NoSQL数据存储**

* [OTSWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-otswriter): 使用OTS SDK向OTS Public模型的表中导入数据。
* [OCSWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-ocswriter)
* [MongoDBReader](mongo_db_reader)：MongoDBReader
* [MongoDBWriter](mongo_db_writer)：MongoDBWriter

**无结构化数据存储**

* [TxtFileWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-txtfilewriter): 提供写入本地文件功能。
* [OssWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-osswriter): 使用OSS SDK写入OSS数据。
* [HdfsWriter](http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-hdfswriter): 提供向Hdfs文件系统中写入textfile文件和orcfile文件功能。
* StreamWriter



# Contact us

Google Groups: [DataX-user](https://github.com/alibaba/DataX)

QQ群:??



