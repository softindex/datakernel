---
id: uikernel
filename: uikernel
title: UIKernel Module
prev: core/launcher.html
next: cloud
nav-menu: core
layout: core

---
UIKernel Module is integration with [UIKernel.io](http://uikernel.io/) JS frontend library: JSON serializers, 
grid model, basic servlets.

You can add UIKernel module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-uikernel</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

**UiKernelServlets** contains API for interacting with UIKernel tables and includes such operations 
as *read*, *get*, *create*, *update*, *delete*.