---
id: etl
filename: etl
title: ETL Module
prev: /cloud/multilog.html
nav-menu: cloud
layout: cloud

---
ETL is a near real-time async data processing system. Unlike traditional ETL (extraction-transformation-loading) systems, 
this module processes massive parallel [FS](/docs/cloud/fs.html) streams of 
data with its distributed worker servers and then commits partially processed results into commit graph of 
[OT](/docs/cloud/ot.html). These commits are continuously merged into a single 
coherent result using merge and conflict resolution strategies provided by OT. 

You can add ETL module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-etl</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

* ETL is used as a part of [LSM Tree OLAP Cube](/docs/cloud/cube.html).

## This module on [GitHub repository](https://github.com/softindex/datakernel/tree/master/cloud-etl)