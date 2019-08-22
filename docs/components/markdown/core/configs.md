---
id: configs
filename: configs
title: Configs Module
prev: core/workerpool.html
next: core/http.html
nav-menu: core
layout: core
description: Efficiently manage your application configurations and properties values with DataKernel Configs
keywords: configs,application configs,save configs,application configuration,java,java framework,java for beginners
---
## Features
**Configs** are a useful extension for properties file. Main features:

* using a set of standard converters
* specifying default value for property
* saving all properties that were used into file

You can add Boot module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-boot</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}


## Example

{% include note.html content="To run the example, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the example, build the project." %}

An example of providing configs to your application:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/boot/src/main/java/ConfigModuleExample.java tag:EXAMPLE %}
{% endhighlight %}
