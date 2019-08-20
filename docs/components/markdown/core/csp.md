---
id: csp
filename: csp
title: CSP Module
prev: core/datastream.html
next: core/net.html
nav-menu: core
layout: core
description: Efficient Java CSP (Communicating Sequential Process) implementation
keywords: csp,communication sequential process,java csp,java go,go language,go
---

CSP (stands for Communicating Sequential Process) provides I/O communication between channels and was inspired by Go 
language approach. 

## Features:
* High performance and throughput speed
* Optimized for working with medium-sized objects (like ByteBufs) 
* CSP has reach DSL, which provides a simple programming model
* Has an asynchronous back pressure management

## Channel Supplier and Channel Consumer
CSP communication is conducted with **ChannelSupplier** and **ChannelConsumer**, which provide and accept some data 
respectively. Each consecutive request to these channels should be called only after the previous request finishes, and 
[Promises](/docs/core/promise.html) are utilized to manage it.

**ChannelSupplier** has a *get()* method which returns a **Promise** of provided value. Until this **Promise** is 
completed either with a result or with an exception, the method shouldn't be called again. Also note, that if *get()* returns 
**Promise** of *null*, this represents end of stream and no additional data should be requested from this supplier.

**ChannelConsumer** has an *accept(@Nullable T value)* method which returns a **Promise** of *null* as a marker of 
completion of the accepting. Until this **Promise** is completed, *accept()* method mustn’t be called again. By analogy 
with the **ChannelSupplier**, if *null* value is accepted, it represents end of stream.

Here is a example of communication between Consumer and Supplier:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/csp/src/main/java/CspExample.java tag:REGION_1 %}
{% endhighlight %}


## Channel Queue
Another important concept of CSP is **ChannelQueue** interface and its implementations: **ChannelBuffer** and 
**ChannelZeroBuffer**. They provide communication between *Consumers* and *Suppliers* and allow to create chains of these 
pipes if needed. 
Basically, these buffers pass objects which were consumed by *Consumer* to *Supplier* as soon as the queue 
gets a free space. This process is controlled by **Promise**s. You can manually set the size for **ChannelBuffer**.
**ChannelZeroBuffer** doesn’t store any values but simply passes them one by one from Consumer to Supplier. 

Here is a simple example of working with buffers of items:

{% highlight java %}
public void accept(T item) {
	buffer.add(item);
	if (buffer.isSaturated()) {
		getSupplier().suspend();
	}
}

void produce() {
	while (!buffer.isEmpty()) {
		T item = buffer.poll();
		if (item != null) {
			send(item);
		} else {
			sendEndOfStream();
		}
	}
}
{% endhighlight %}


## Comparison to Datastream
CSP has a lot in common with [Datastream](/docs/core/datastream.html) module. 
Although they were both designed for I/O processing, there are several important distinctions:

| | Datastream | CSP |
| --- | --- | --- |
| **Overhead:** | Extremely low: stream can be started with 1 virtual call, short-circuit evaluation optimizes performance | No short-circuit evaluation, overhead is higher |
| **Throughput speed:** | Extremely fast | Fast, but slower than Datastream |
| **Programming model:** | More complicated | Simple and convenient |

To provide maximum efficiency, our framework widely utilizes combinations of CSP and Datastream. For this purpose, 
**ChannelSupplier**, **ChannelConsumer**, **StreamSupplier** and **StreamConsumer** have *transformWith()* methods and special 
Transformer interfaces. Using them, you can seamlessly transform channels into other channels or datastreams and vice 
versa, creating chains of such transformations.

You can add CSP module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-csp</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

## Examples
* [Basic Channel Example](#basic-channel-example)
* [CSP Example](#csp-example) 
* [Channel Buffer Example](#channel-buffer-example)
* [ChannelSplitter Example](#channelsplitter-example)
* [CSP Transformations](#csp-transformations)
* [Channel File Example](#channel-file-example) 
* [Custom CSP](#custom-csp)

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project." %}


### Basic Channel Example
**Channel Example** shows interaction between suppliers and consumers using *streamTo* and some helper methods:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/csp/src/main/java/ChannelExample.java tag:REGION_1%}
{% endhighlight %}

Thus, if you run this example, you'll receive the following output:

{% highlight bash %}
1
2
3
4
5
One
Two
Three
1 times 10 = 10
2 times 10 = 20
3 times 10 = 30
4 times 10 = 40
5 times 10 = 50
[1, 2, 3, 4, 5]
2
4
6
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/csp/src/main/java/ChannelExample.java)

### CSP Example
This example represents an **AsyncProcess** between **ChannelSupplier** and **ChannelConsumer**. In this example 
**ChannelSupplier** represents an input and **ChannelConsumer** - output:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/csp/src/main/java/CspExample.java tag:EXAMPLE %}
{% endhighlight %}

This process takes a string, sets it to upper-case and adds string's length in parentheses:

{% highlight bash %}
HELLO(5)
WORLD(5)
NICE(4)
TO(2)
SEE(3)
YOU(3)
{% endhighlight %}
##### [See this example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/csp/src/main/java/CspExample.java)

### Channel Buffer Example
As it was mentioned before, there are two **ChannelQueue** implementations: **ChannelBuffer** and **ChannelZeroBuffer**, 
both of them manage communication between Providers and Suppliers. 
You can manually set the size of **ChannelBuffer**, whereas **ChannelZeroBuffer** size is always 0.

To give you a better understanding of how all these Buffers work, let's have a simple example. Assume there is a **Granny** 
who wants to give her **Grandson** 25 **Apple**s. That's quite a lot, so she first puts the **Apple**s on a big **Plate**, 
which can place up to 10 apples simultaneously. When the **Plate** is full, **Grandson** should first take at least one apple, 
and only after that **Granny** can put a new **Apple** to the **Plate**:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/csp/src/main/java/ChannelBufferExample.java tag:REGION_1 %}
{% endhighlight %}

On the next day **Granny** wants to give **Apple**s to her **Grandson** again, but this time there are only 10 
**Apples**. So there is no real need in the plate: **Granny** can simply pass the **Apples** to her **Grandson** 
one by one:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/csp/src/main/java/ChannelBufferExample.java tag:REGION_2 %}
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/csp/src/main/java/ChannelBufferExample.java) 
<br>

### ChannelSplitter Example
In this example we use predefined **ChannelSplitter**. Splitter allows to split data from one input to several outputs. 
In our case output will be split into three **ChannelConsumers**:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/csp/src/main/java/SplitterExample.java tag:EXAMPLE %}
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/csp/src/main/java/SplitterExample.java)

### CSP Transformations
You can create chains of transformations of data that is provided by **ChannelSupplier**. Use *transformWith* method 
and predefined CSP chunkers, compressors, decompressors etc. In this example we will transform suppliers **ByteBufs**, 
chunk, compress and decompress them:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-datastream/src/test/java/io/datakernel/csp/net/StreamLZ4Test.java tag:EXAMPLE %}
{% endhighlight %}
##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/core-datastream/src/test/java/io/datakernel/csp/net/StreamLZ4Test.java) 

### Channel File Example  
This example demonstrates how to work with files with asynchronous approach using Promises and CSP built-in 
consumers and suppliers. This example writes two lines to the file with **ChannelFileWriter**, and then reads and prints 
them out utilizing **ChannelFileReader**:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/csp/src/main/java/ChannelFileExample.java tag:REGION_1%}
{% endhighlight %}

If you run the example, you'll see the content of the created file:
{% highlight bash %}
Hello, this is example file
This is the second line of file
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/csp/src/main/java/ChannelFileExample.java)

### Custom CSP
This example describes how to create a custom communicating process with one input and two outputs channels. 
It will bifurcate an input item and send it to both output channels.

In order to create a custom CSP, you need to create a class which extends **AbstractCommunicationProcess** and has one 
input and two outputs:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-csp/src/main/java/io/datakernel/csp/process/ChannelBifurcator.java tag:REGION_1 %}
{% endhighlight %}

Next, we want to initialize our input and output channels using chain call. 
We need to implement *withOutputs* method first:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-csp/src/main/java/io/datakernel/csp/process/ChannelBifurcator.java tag:REGION_2 %}
{% endhighlight %}

After this we can create a bifurcator in the following way: 
{% highlight java %}
bifurcator = ChannelBifurcator.create().withInput(input).withOutputs(first, second);
{% endhighlight %}

Next, we implement *getInput()* in our class:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-csp/src/main/java/io/datakernel/csp/process/ChannelBifurcator.java tag:REGION_3 %}
{% endhighlight %}

*doProcess* is the main method where item delivering logic should be described. In our case, it is duplicating and sending an input item:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-csp/src/main/java/io/datakernel/csp/process/ChannelBifurcator.java tag:REGION_4 %}
{% endhighlight %}

*doClose* method is called when communication process lifetime ends. We must remember to close all the channels:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-csp/src/main/java/io/datakernel/csp/process/ChannelBifurcator.java tag:REGION_5 %}
{% endhighlight %}

Finally, we define bifuractor startup:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-csp/src/main/java/io/datakernel/csp/process/ChannelBifurcator.java tag:REGION_6 %}
{% endhighlight %}

Here it is, your own CS process is ready!

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/core-csp/src/main/java/io/datakernel/csp/process/ChannelBifurcator.java) 

Now let's test our bifurcator out:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-csp/src/test/java/io/datakernel/csp/ChannelBifurcatorTest.java tag:REGION_1 %}
{% endhighlight %}