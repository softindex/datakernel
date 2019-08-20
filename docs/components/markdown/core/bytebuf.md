---
id: bytebuf
filename: bytebuf
title: ByteBuf Module
prev: core/promise.html
next: core/datastream.html
nav-menu: core
layout: core
description: Lightweight alternative to Java NIO ByteBuffers with extremely low GC footprint.
keywords: bytebuffer,byte buffers,java nio,bytebuffer alternative,java,java framework
---

## Features 

DataKernel mission is to make efficient yet high-level I/O, which requires extensive usage of user-space 
byte buffers. Unfortunately, traditional Java ByteBuffers impose heavy load on GC (either clogging young gen, or being 
allocated directly in old gen because of their large sizes).

To reduce GC overhead, DataKernel introduces its own GC-friendly and lightweight [ByteBufs](#bytebuf), which can be 
reused with [ByteBufPool](#bytebufpool). 

In addition, common I/O pattern is to treat ByteBuffers as queue: I/O operation produces the data, while application 
consumes the data or vice versa. ByteBufs are designed to facilitate this pattern as well, and also provides 
specialized [ByteBufQueue](#bytebufqueue) with queue-like operations across multiple ByteBufs.

### **ByteBuf**
An extremely lightweight and efficient implementation compared to the Java NIO **ByteBuffer**. There are no *direct buffers*, 
which simplifies and improves **ByteBuf** performance. 

ByteBuf is similar to a FIFO byte queue and has two positions: *head* and *tail*. When you write data to your 
ByteBuf, its *tail* increases by the amount of bytes written. Similarly, when you read data from your ByteBuf,
its *head* increases by the amount of bytes read. 

You can read bytes from ByteBuf only when *tail* is greater then *head*. Also, you can write bytes to ByteBuf until 
*tail* doesn't exceed the length of the wrapped array. In this way, there is no need for *ByteBuffer.flip()* operations. 

ByteBuf supports concurrent processes: while some data is written to the **ByteBuf** by one process, another one can 
read it.

To create a ByteBuf you can either wrap your byte array into **ByteBuf** or allocate it from ByteBufPool.
{% include note.html content="If you create a ByteBuf without allocating it from <b>ByteBufPool</b>, calling <i>ByteBuf.recycle()</i> will have no effect, such ByteBufs are simply collected by GC." %}

### **ByteBufPool**

Allows to reuse ByteBufs, and as a result reduces GC load. To make ByteBufPool usage more 
convenient, there are debugging and monitoring tools for allocated ByteBufs, including their stack traces.

To get a ByteBuf from the pool, use *ByteBufPool.allocate(int size)*. A ByteBuf of rounded up to the next nearest power 
of 2 size will be allocated (for example, if *size* is 29, a ByteBuf of 32 bytes will be allocated).

To return ByteBuf to the ByteBufPool, use *ByteBuf.recycle()*. Unlike languages like C/C++, it’s not required to recycle 
ByteBufs - in the worst case, it will be collected by the GC. 

To make everything consistent, DataKernel relies on the concept of ‘ownership’ (like in Rust language) - after allocation, 
the components pass ByteBuf from one to another, until last ‘owner’ recycles it to ByteBufPool. 

You can explore an example of ByteBuf pool usage [here](#bytebuf-pool-example)

### **ByteBufQueue**
**ByteBufQueue** class provides effective management of multiple ByteBufs. It creates an optimized queue of several 
ByteBufs with FIFO rules. 

You can explore an example of ByteBuf queue usage [here](#bytebuf-queue-example)

### **Utility classes**
ByteBuf module also contains utility classes to manage resizing of underlying byte buffer, **String** conversions, etc.

You can add ByteBuf module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-bytebuf</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %} 


## Examples

1. [ByteBuf Example](#bytebuf-example) - 
represents some basic ByteBuf possibilities, such as: 
    * wrapping data in ByteBuf for writing/reading, 
    * slicing particular parts out of data,
    * conversions.
2. [ByteBuf Pool Example](#bytebuf-pool-example) - 
represents how to work with ByteBufPool.
3. [ByteBuf Queue Example](#bytebuf-queue-example) - 
shows how queues of ByteBufs are created and processed.

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project.
<br> These examples are located at <b>datakernel -> examples -> core -> bytebuf.</b>" %}

### **ByteBuf Example**
If you run the example, you'll receive the following output:

{% highlight bash %}
0
1
2
3
4
5

[0, 1, 2, 3, 4, 5]

Hello

Sliced ByteBuf array: [1, 2, 3]

Array of ByteBuf converted from ByteBuffer: [1, 2, 3]
{% endhighlight %}

* The first six lines are result of wrapping byte array to ByteBuf wrapper for reading and then printing it:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufExample.java tag:REGION_1 %}
{% endhighlight %}

* The line `[0, 1, 2, 3, 4, 5]` is a result of converting an empty array of bytes to ByteBuf and wrapping them for 
writing. Then the ByteBuf was filled with bytes with the help of `while` loop:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufExample.java tag:REGION_2 %}
{% endhighlight %}

* "Hello" line was first converted from String to ByteBuf and wrapped for reading, then represented as a String for 
output with the help of `byteBuf.asString()`:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufExample.java tag:REGION_3 %}
{% endhighlight %}

* The last two outputs represent some other possibilities of ByteBuf, such as slicing:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufExample.java tag:REGION_4 %}
{% endhighlight %}

and conversions of default ByteBuffer to ByteBuf:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufExample.java tag:REGION_5 %}
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufExample.java)

### **ByteBuf Pool Example**
If you run the example, you'll receive the following output:
{% highlight bash %}
Length of array of allocated ByteBuf: 128
Number of ByteBufs in pool before recycling: 0
Number of ByteBufs in pool after recycling: 1
Number of ByteBufs in pool: 0

Size of ByteBuf: 4
Remaining bytes of ByteBuf after 3 bytes have been written: 1
Remaining bytes of a new ByteBuf: 5

[0, 1, 2, 3, 4, 5]
{% endhighlight %}

Let's have a look at the implementation:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufPoolExample.java tag:EXAMPLE %}
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufPoolExample.java)

### **ByteBuf Queue Example**
If you run the example, you'll receive the following output:
{% highlight bash %}
bufs:2 bytes:7

Buf taken from queue: [0, 1, 2, 3]

Buf taken from queue: [3, 4, 5, 6, 7, 8]

[1, 2, 3, 4]
[5, 6, 7, 8]
Is queue empty? true
{% endhighlight %}

The first line represents our queue after we added two bufs: `[0, 1, 2, 3]` and `[3, 4, 5]` with `QUEUE.add()` method.

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufQueueExample.java tag:REGION_1 %}
{% endhighlight %}

Then method `QUEUE.take()` is applied and the first added buf, which is `[0, 1, 2, 3]`, is taken from the queue.

The next line represents the consequence of two operations: adding a new `[6, 7, 8]` buf and then applying 
`QUEUE.takeRemaining()` which takes all remaining bufs from the queue. 

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufQueueExample.java tag:REGION_2 %}
{% endhighlight %}

{% include note.html content="pay attention to the difference between <i>take()</i> and <i>poll()</i> ByteBufQueue 
methods. When using <i>take()</i>, you must be sure that there is at least one ByteBuf remaining in the queue, otherwise 
use <i>poll()</i> which can return <b>null</b>." %}

Finally, the last three lines represent the following operations:

* Creating two bufs: `[1, 2, 3, 4]` and `[5, 6, 7, 8]`.
* Draining the queue to consumer which prints the bufs.
* Then we check if the queue is empty now.

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufQueueExample.java tag:REGION_3 %}
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/bytebuf/src/main/java/ByteBufQueueExample.java)
