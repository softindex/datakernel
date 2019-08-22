---
id: eventloop
filename: eventloop
title: Eventloop Module
prev: core/http.html
next: core/promise.html
nav-menu: core
layout: core
description: Node.js-inspired single-threaded Eventloop for developing asynchronous Java applications
keywords: nodejs,node js,java,java framework,asynchronous,asynchronous application,client-server application
---

Eventloop module is the foundation of other modules that run their code inside event loops and threads. It provides 
efficient management of asynchronous operations without multithreading overhead. Particularly useful for building 
client-server applications with high performance requirements.

* Eventloop utilizes Java's NIO to allow asynchronous computations and I/O operations (TCP, UDP).
* Eliminates traditional bottleneck of I/O for further business logic processing.
* Can run multiple eventloop threads on available cores.
* Minimal GC load: arrays and byte buffers are reused.
* Eventloop can schedule/delay certain tasks for later execution or background execution.
* Eventloop is single-threaded, so it doesn't have concurrency overhead.

[**Eventloop**](https://github.com/softindex/datakernel/blob/master/core-eventloop/src/main/java/io/datakernel/eventloop/Eventloop.java) 
represents an infinite loop, where each `loop` executes all the tasks that are provided by **Selector** or stored in 
special queues. Each of these tasks should be small and its execution is called `tick`. 

The only blocking operation of Eventloop infinite loop is *Selector.select()*. This operation selects a set of keys 
whose corresponding channels are ready for I/O operations. With these keys and queues with tasks, eventloop asynchronously 
executes them in one thread in the overridden method *run()* (**Eventloop** is an implementation of **Runnable**). 
 
Eventloop works with different types of tasks that are stored in separate queues:

|Tasks| Description|
| --- | --- |
| **Local tasks** | Tasks which were added from current Eventloop thread |
| **Concurrent tasks** | Tasks which were added from other threads |
| **Scheduled tasks** | Tasks which are scheduled to be executed later |
| **Background tasks** | Same as *Scheduled*, but if there are only *Background* tasks left, Eventloop will be closed |
 
Execution of an eventloop will be ended when its queues with non-background tasks are empty, **Selector** has no selected 
keys and amount of concurrent operations in other threads equals 0. To prevent Eventloop closing in such cases, you 
should use `keepAlive` flag (when set **true**, Eventloop will continue running even without tasks).

You can add Eventloop module to your project by inserting dependency in `pom.xml`: 
{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-eventloop</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

## Examples 

* [BasicExample](#basic-example) - a simple example of an eventloop which prints out a "Hello World" message.
* [EventloopExample](#eventloop-example) - represents the sequence of operations execution in eventloops.


{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project.
<br> These examples are located at <b>datakernel -> examples -> core -> eventloop</b>." %}


## Basic Example
In this example we create an eventloop, post a task to it (which is printing out "Hello World" message) and 
then starting the eventloop:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/eventloop/src/main/java/BasicExample.java tag:EXAMPLE %}
{% endhighlight %}

## Eventloop Example
This example represents how tasks are scheduled in eventloops:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/eventloop/src/main/java/EventloopExample.java tag:EXAMPLE %}
{% endhighlight %}


If you run the example, you'll receive an output which looks something like this:
{% highlight bash %}
Not in eventloop, time: 4
Thread.sleep(2000) is finished, time: 2008
Eventloop.delay(100) is finished, time: 2008
Eventloop.delay(1000) is finished, time: 2008
Eventloop.delay(3000) is finished, time: 3001
{% endhighlight %}

So, the tasks will be executed in the following order:
1. `Not in eventloop...`, it will be executed even before the eventloop is started.
2. `Thread.sleep(2000)...` as we posted it to the beginning of eventloop's *localTasks*.
3. Immediately after that - `Eventloop.delay(100)...` and `Eventloop.delay(1000)...` as they are already expired.
4. And finally `eventloop.delay(3000)` will be executed right at its time.
