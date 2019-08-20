---
id: datastream
filename: datastream
title: Datastream Module
prev: core/bytebuf.html
next: core/csp.html
nav-menu: core
layout: core
description: Asynchronous reactive Java streams with extremely low overhead.
keywords: datastream,reactive streams,async reactive streams,java,java framework
---

Datastream Module is useful for intra- and inter-server communication and asynchronous data processing.
It is an important building block for other DataKernel modules.


DataStream is:
* Modern implementation of async reactive streams (unlike streams in Java 8 and traditional thread-based blocking streams)
* Asynchronous with extremely efficient congestion control, to handle natural imbalance in speed of data sources
* Composable stream operations (mappers, reducers, filters, sorters, mergers/splitters, compression, serialization)
* Stream-based network and file I/O on top of [Eventloop module](/docs/core/eventloop.html)
* Compatibility with [CSP module](/docs/core/csp.html)


Datastream has a lot in common with [CSP](/docs/core/csp.html) module. 
Although they both were designed for I/O processing, there are several important distinctions:

| | Datastream | CSP |
| --- | --- | --- |
| **Overhead:** | Extremely low: stream can be started with 1 virtual call, short-circuit evaluation optimizes performance | No short-circuit evaluation, overhead is higher |
| **Throughput speed:** | Extremely fast | Fast, but slower than Datastream |
| **Optimized for:** | Small pieces of data | Medium-sized objects, ByteBufs |
| **Programming model:** | More complicated | Simple and convenient |

To provide maximum efficiency, our framework widely utilizes combinations of CSP and Datastream. For this purpose, 
`ChannelSupplier`, `ChannelConsumer`, `StreamSupplier` and `StreamConsumer` have *transformWith()* methods and special 
Transformer interfaces. Using them, you can seamlessly transform channels into other channels or datastreams and vice 
versa, creating chains of such transformations.

You can add Datastream module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-datastream</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

## Examples
* [Simple Supplier](#simple-supplier) - 
shows how to create a simple custom *Supplier* and stream some data to *Consumer*. 
* [Simple Consumer](#simple-consumer) - 
shows how to create a simple custom *Consumer*. 
* [Custom Transformer](#custom-transformer) - 
shows how to create a custom *StreamTransformer*, which takes strings and transforms them to their length if it is less than *MAX_LENGTH*.
* [Built-in Stream Nodes Example](#built-in-stream-nodes) - 
demonstrates some of built-in Datastream possibilities, such as filtering, sharding and mapping.


{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project." %}


### Simple Supplier
When you run **SupplierExample**, you'll see the following output:
{% highlight bash %}
Consumer received: [0, 1, 2, 3, 4]
{% endhighlight %}

This output represents the data which our custom **StreamSupplier** provided to **StreamConsumer**. Let's have a 
look at the implementation:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/datastream/src/main/java/SupplierExample.java tag:EXAMPLE %}
{% endhighlight %}

### Simple Consumer
When you run **ConsumerExample**, you'll see the following output:
{% highlight bash %}
received: 1
received: 2
received: 3
End of stream received
{% endhighlight %}

**ConsumerExample** extends **AbstractStreamConsumer** and just prints out received data. The stream process is managed with 
overridden methods *onStarted()*, *onEndOfStream()* and *onError()*:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/datastream/src/main/java/ConsumerExample.java tag:EXAMPLE %}
{% endhighlight %}

### Custom Transformer
**TransformerExample** shows how to create a custom **StreamTransformer** which takes strings from input stream and 
transforms them to their length if it is less than defined MAX_LENGTH. 
First, we define **AbstractStreamConsumer** and **AbstractStreamSupplier**:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/datastream/src/main/java/TransformerExample.java tag:REGION_1 %}
{% endhighlight %} 
 
Now we define *main* method, which creates a supplier of test data, an instance of **TransformerExample** and **StreamConsumerToList**. 
Next, we define the sequence of transformation and output:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/datastream/src/main/java/TransformerExample.java tag:REGION_2 %}
{% endhighlight %} 

So, if you run the example, you'll receive the following output:
{% highlight java %}
[8, 9]
{% endhighlight %}

### Built-in Stream Nodes
**BuiltinStreamNodesExample** demonstrates some simple examples of utilizing built-in datastream nodes. If you 
run the example, you'll receive the following output:

{% highlight java %}
[1 times ten = 10, 2 times ten = 20, 3 times ten = 30, 4 times ten = 40, 5 times ten = 50, 6 times ten = 60, 7 times ten = 70, 8 times ten = 80, 9 times ten = 90, 10 times ten = 100]
third: [2, 5, 8]
second: [1, 4, 7, 10]
first: [3, 6, 9]
[1, 3, 5, 7, 9]
{% endhighlight %}

The first line is a result of `StreamMapper`:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/datastream/src/main/java/BuiltinNodesExample.java tag:REGION_3 %}
{% endhighlight %}

The next three lines of the output are results of utilizing `StreamSharder`:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/datastream/src/main/java/BuiltinNodesExample.java tag:REGION_2 %}
{% endhighlight %}

The last line of the output is a result of utilizing `StreamFilter`, which filters numbers and leaves only odd ones:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/datastream/src/main/java/BuiltinNodesExample.java tag:REGION_1 %}
{% endhighlight %}