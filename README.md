# benchmarksql_for_mysql

	This variant is forked from a modified version of benchmarksql(4.1). 

## What is this

	Compare to the original , we works on objectss belows:

1. Add support for MySQL protocol

2. Replace complex statements( i.e. JOIN ) with simple CRUD statement so that a MyCat with weak parser and optimizer can be the backend of the benchmark

3. Some little optimize or even refactor

## How-to run

	Please refer to the HOW-TO-RUN.txt.

## Special thanks

[jTPCC](http://jtpcc.sourceforge.net)
	A Java-based implementation of TPC-C. Everything starts from here

[benchmarksql](https://sourceforge.net/projects/benchmarksql)
	A successor of jTPCC with a really good arhitecture. We folked our project from this.

[MyCat](https://github.com/MyCATApache/Mycat-Server)
	A Java-based mysql protocol router.

## Contributors

	Hailong Li
	Weihua Peng
	Yue Zhong(John)
