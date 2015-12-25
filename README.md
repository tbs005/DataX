# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。



# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。



# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/alibaba/DataX/wiki/DataX-Introduction)



# Quick Start

##### 请点击：[Quick Start](https://github.com/alibaba/DataX/wiki/Quick-Start)



# Support Data Channels

目前DataX支持的数据源有:

### Reader

> **Reader实现了从数据存储系统批量抽取数据，并转换为DataX标准数据交换协议，DataX任意Reader能与DataX任意Writer实现无缝对接，达到任意异构数据互通之目的。**

**RDBMS 关系型数据库**

- [MysqlReader](https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md): 使用JDBC批量抽取Mysql数据集。
- [OracleReader](https://github.com/alibaba/DataX/blob/master/oraclereader/doc/oraclereader.md): 使用JDBC批量抽取Oracle数据集。
- [SqlServerReader](https://github.com/alibaba/DataX/blob/master/sqlserverreader/doc/sqlserverreader.md): 使用JDBC批量抽取SqlServer数据集
- [PostgresqlReader](https://github.com/alibaba/DataX/blob/master/postgresqlreader/doc/postgresqlreader.md): 使用JDBC批量抽取PostgreSQL数据集
- [DrdsReader](https://github.com/alibaba/DataX/blob/master/drdsreader/doc/drdsreader.md): 针对公有云上DRDS的批量数据抽取工具。

**数仓数据存储**

- [ODPSReader](https://github.com/alibaba/DataX/blob/master/odpsreader/doc/odpsreader.md): 使用ODPS Tunnel SDK批量抽取ODPS数据。

**NoSQL数据存储**

- [OTSReader](https://github.com/alibaba/DataX/blob/master/otsreader/doc/otsreader.md): 针对公有云上OTS的批量数据抽取工具。
- [HBaseReader](https://github.com/alibaba/DataX/blob/master/hbasereader/doc/hbasereader.md): 针对 HBase 0.94版本的在线数据抽取工具
- [MongoDBReader](https://github.com/alibaba/DataX/blob/master/mongodbreader/doc/mongodbreader.md)：MongoDBReader

**无结构化数据存储**

- [TxtFileReader](https://github.com/alibaba/DataX/blob/master/txtfilereader/doc/txtfilereader.md): 读取(递归/过滤)本地文件。
- [FtpReader](https://github.com/alibaba/DataX/blob/master/ftpreader/doc/ftpreader.md): 读取(递归/过滤)远程ftp文件。
- [HdfsReader](https://github.com/alibaba/DataX/blob/master/hdfsreader/doc/hdfsreader.md): 针对Hdfs文件系统中textfile和orcfile文件批量数据抽取工具。 
- [OssReader](https://github.com/alibaba/DataX/blob/master/ossreader/doc/ossreader.md): 针对公有云OSS产品的批量数据抽取工具。
- StreamReader

### Writer

------

> **Writer实现了从DataX标准数据交换协议，翻译为具体的数据存储类型并写入目的数据存储。DataX任意Writer能与DataX任意Reader实现无缝对接，达到任意异构数据互通之目的。**

------

**RDBMS 关系型数据库**

- [MysqlWriter](https://github.com/alibaba/DataX/blob/master/mysqlwriter/doc/mysqlwriter.md): 使用JDBC(Insert,Replace方式)写入Mysql数据库
- [OracleWriter](https://github.com/alibaba/DataX/blob/master/oraclewriter/doc/oraclewriter.md): 使用JDBC(Insert方式)写入Oracle数据库
- [PostgresqlWriter](https://github.com/alibaba/DataX/blob/master/postgresqlwriter/doc/postgresqlwriter.md): 使用JDBC(Insert方式)写入PostgreSQL数据库
- [SqlServerWriter](https://github.com/alibaba/DataX/blob/master/sqlserverwriter/doc/sqlserverwriter.md): 使用JDBC(Insert方式)写入sqlserver数据库
- [DrdsWriter](https://github.com/alibaba/DataX/blob/master/drdswriter/doc/drdswriter.md): 使用JDBC(Replace方式)写入Drds数据库

**数仓数据存储**

- [ODPSWriter](https://github.com/alibaba/DataX/blob/master/odpswriter/doc/odpswriter.md): 使用ODPS Tunnel SDK向ODPS写入数据。
- [ADSWriter](https://github.com/alibaba/DataX/blob/master/adswriter/doc/adswriter.md): 使用ODPS中转将数据导入ADS。

**NoSQL数据存储**

- [OTSWriter](https://github.com/alibaba/DataX/blob/master/otswriter/doc/otswriter.md): 使用OTS SDK向OTS Public模型的表中导入数据。
- [OCSWriter](https://github.com/alibaba/DataX/blob/master/ocswriter/doc/ocswriter.md)
- [MongoDBWriter](https://github.com/alibaba/DataX/blob/master/mongodbwriter/doc/mongodbwriter.md)：MongoDBWriter

**无结构化数据存储**

- [TxtFileWriter](https://github.com/alibaba/DataX/blob/master/txtfilewriter/doc/txtfilewriter.md): 提供写入本地文件功能。
- [OssWriter](https://github.com/alibaba/DataX/blob/master/osswriter/doc/osswriter.md): 使用OSS SDK写入OSS数据。
- [HdfsWriter](https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md): 提供向Hdfs文件系统中写入textfile文件和orcfile文件功能。
- StreamWriter



# Contact us

请及时提出issue给我们。请前往：[DataxIssue](https://github.com/alibaba/DataX/issues)

