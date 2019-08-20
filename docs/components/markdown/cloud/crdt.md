---
id: crdt
filename: crdt
title: CRDT Module
prev: cloud/dataflow.html
next: cloud/multilog.html
nav-menu: cloud
layout: cloud
description: Fast and efficient Java CRDT (conflict-free replicated data type) implementation.
keywords: crdt,conflict-free replicated data type,java,scalable storage,eventually consistent,collaborative editing,key-value-storage
---
CRDT module was designed to create collaborative editing applications with CRDT (conflict-free replicated data type) 
approach. Well suitable for some simple solutions (for example, a scalable eventually consistent key-value storage with 
CRDT conflict resolutions).

You can add CRDT module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-crdt</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

### Example

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project.
<br> These examples are located at <b>datakernel -> examples -> cloud -> crdt</b>." %}

In this example, we have two replicas - independent nodes which store different information.

First replica stores:
{% highlight bash %}
first = [#1, #2, #3, #4]
second = ["#3", "#4", "#5", "#6"]
{% endhighlight %}

Second replica stores:
{% highlight bash %}
first = [#3, #4, #5, #6]
second = [#2, #4, <removed> #5, <removed> #6]
{% endhighlight %}

Then we merge replicas with CRDT approach and receive a result:
{% highlight bash %}
first = [#1, #2, #3, #4, #5, #6]
second = [#2, #3, #4]
{% endhighlight %}

In the example `LWWSet` (Last Write Wins) is utilized. It implements `Set` interface and is basically a 
`Map<E, Timestamp>`. Timestamp allows to merge `LWWSet`s by choosing the most relevant versions in case of conflicts.