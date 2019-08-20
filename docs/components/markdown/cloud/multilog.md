---
id: multilog
filename: multilog
title: Multilog Module
prev: cloud/crdt.html
next: cloud/etl.html
nav-menu: cloud
layout: cloud
---

* Utilizes FS module to work with logs stored on different partitions.
* Log data is transferred using Datastream module which is perfect for large amount of lightweight items (just like logs).
* Uses LZ4 compression algorithm which is fast and allows to save storage space.

You can add Multilog module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-multilog</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

`Multilog` interface manages persistence of logs and `MultilogImpl` is an implementation of the interface. It has 
the following core operations:
* *create()*
* *write()*
* *read()*

## This module on [GitHub repository](https://github.com/softindex/datakernel/tree/master/cloud-multilog)