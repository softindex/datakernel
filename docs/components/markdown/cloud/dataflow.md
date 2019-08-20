---
id: dataflow
filename: dataflow
title: Dataflow Module
prev: cloud/cube.html
next: cloud/crdt.html
nav-menu: cloud
layout: cloud
---
Dataflow is a distributed stream-based batch processing engine for Big Data applications.
You can write tasks to be executed on a dataset. The task is then compiled into execution graphs and passed as 
JSON commands to corresponding worker servers to be executed.

You can add Dataflow module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-dataflow</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

* Data graph consists of nodes that correspond to certain operations (e.g. Download, Filter, Map, Reduce, Sort, etc).
* User can define custom predicates, mapping functions, reducers to be executed on datasets.
* Nodes in a data graph have inputs and outputs which are identified by a unique StreamId. This allows inter-partition 
data computation.
* Since nodes are stateless by themselves, computation is similar to passing data items through a pipeline, applying certain 
operations during the process.

## This module on [GitHub repository](https://github.com/softindex/datakernel/tree/master/cloud-dataflow)