---
id: cube
filename: cube
title: LSM Tree OLAP Cube Module
prev: cloud/aggregation.html
next: cloud/dataflow.html
nav-menu: cloud
layout: cloud
description: Asynchronous and distributed multidimensional LSM Tree OLAP Cube database with transaction semantics.
keywords: olap,multidimentional olap,asynchronous database,distributed database,distributed storage,spring alternative
---
LSM Tree OLAP Cube is a log-structured merge-tree database designed for processing massive partial aggregations of 
raw data and forms a multidimensional OLAP (online analytical processing). It utilizes Cloud-OT and Cloud-FS technologies. 
LSM Tree database is truly asynchronous and distributed, with full support of transaction semantics. Dimension here can be 
treated as categories while measures represent values.

You can add OLAP Cube module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-cube</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

`Cube` class represents an OLAP cube. It provides methods for loading and querying data along with functionality for 
aggregations management.
 
## This module on [GitHub repository](https://github.com/softindex/datakernel/tree/master/cloud-lsmt-cube)