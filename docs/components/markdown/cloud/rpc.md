---
id: rpc
filename: rpc
title: RPC Module
prev: core
next: cloud/fs.html
nav-menu: cloud
layout: cloud
---
### Table of contents:
* [Features](#features)
* [Examples](#examples)
    * [Simple RPC Example](#simple-rpc-example)
    * [Round-Robin Strategy](#round-robin-strategy)
    * [Round-Robin and First Available Strategies Combined](#round-robin-and-first-available-strategies-combined)
    * [Sharding and First Valid Strategies Combined](#sharding-and-first-valid-strategies-combined)
    * [Rendezvous Hashing Strategy](#rendezvous-hashing-strategy)
    * [Type Dispatch Strategy](#type-dispatch-strategy)

## Features
RPC module allows to build distributed applications that require efficient client-server interconnections between servers.

* Ideal for creation of near-realtime (i.e. memcache-like) servers with application-specific business logic
* Up to ~15M of requests per second on single core
* Pluggable high-performance asynchronous binary RPC streaming protocol
* Consistent hashing and round-robin distribution strategies
* Fault tolerance - with re-connections to fallback and replica servers

You can add RPC module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-rpc</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

## Benchmarks
We've measured RPC performance using JMH as the benchmark tool:
{% highlight plaintext %}
Time: 6724ms; Average time: 672.4ms; Best time: 657ms; Worst time: 694ms; Requests per second: 14872099
{% endhighlight %}

And MemcacheRPC performance: 
{% highlight plaintext %}
Put
Time: 7019ms; Average time: 1002.714285714285ms; Best time: 954ms; Worst time: 1180ms; Requests per second: 9479697
Get
Time: 6805ms; Average time: 972.1428571428571ms; Best time: 931ms; Worst time: 1064ms; Requests per second: 9714915
{% endhighlight %}

**You can find benchmark sources on [GitHub](https://github.com/softindex/datakernel/tree/master/examples/cloud/rpc/src/main/java).**

## Examples
### Simple RPC Example
{% include note.html content="To run the example, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the example, build the project. 
<br> The example is located at <b>datakernel -> examples -> cloud -> rpc</b>." %}

In the "Hello World" client and server **RPC Example** client sends a request which contains word "World" to server. When 
server receives it, it sends a respond which contains word "Hello ". If everything completes successfully, we get the 
following output:

{% highlight bash %}
Got result: Hello World
{% endhighlight %}

Let's have a look at the implementation:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/cloud/rpc/src/main/java/RpcExample.java tag:EXAMPLE %}
{% endhighlight %}

**RpcExample** class extends **Launcher** to help us manage application lifecycle.

We need to provide **RpcServer** and **RpcClient** with relevant configurations and required dependencies using 
DataKernel DI. **RpcClient** sends requests to the specified server according to the provided **RpcStrategies** 
(getting a single RPC-service). 
For **RpcServer** we define the type of messages which it will proceed, corresponding **RpcRequestHandler** and listen port.

Since we extend **Launcher**, we will also override 2 methods: *getModule* to provide [**ServiceGraphModule**](/docs/core/service-graph.html) 
and *run*, which represents the main logic of the example.

Finally, we define *main* method, which will launch our example.

### Round-Robin Strategy
RPC module contains pre-defined strategies for requests arrangement between RPC servers or shards of servers. 
Round-Robin is one of the simplest of the strategies: it just goes through the servers or shards in a cyclic way one by 
one.
So, in this example we create an RPC *pool* with 5 equal *connections* and set `roundRobin` strategy for them. Next, we 
create a sender for the pool with the previously defined strategy. And that's it, 100 requests will be equally 
distributed between the servers:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/cloud-rpc/src/test/java/io/datakernel/rpc/client/sender/RpcStrategiesTest.java tag:REGION_1 %}
{% endhighlight %}

### Round-Robin and First Available Strategies Combined
You can simply combine RPC strategies. In this example we will combine `roundRobin` and `firstAvailable` strategies. 
First, we create 4 connections but don't put *connection3* into the pool. Then we start sending 20 requests. 
As a result, all the requests will be equally distributed between *connection1* (as it is always `firstAvailable`) and 
*connection4* (as *connection3* isn't available for the pool):

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/cloud-rpc/src/test/java/io/datakernel/rpc/client/sender/RpcStrategiesTest.java tag:REGION_2 %}
{% endhighlight %}

### Sharding and First Valid Strategies Combined
You can also create your own sharding functions and combine them with other strategies if needed. In this example we 
create 5 equal connections but don't add *connection2* into the pool. Next, we provide a simple sharding function which 
distributes requests between shards in accordance to the content of the request. 
We split the connections into two shards, 
and set `firstValidResult` strategy for both of them. This strategy sends request to all the available servers.
Now we manually send 7 requests: 
* 4 with `0` message, so they'll be sent to the first shard's *connection1* 
* 3 with `1`, so they'll all be sent to all three connections of the second shard

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/cloud-rpc/src/test/java/io/datakernel/rpc/client/sender/RpcStrategiesTest.java tag:REGION_3 %}
{% endhighlight %}

### Rendezvous Hashing Strategy
Rendezvous hashing strategy pre-calculates the hash function for the **RpcSender** and creates a map of the RPC servers. 
The map is stored in cache and will be re-calculated only if servers go online/offline. 

In this example requests will be equally distributed between *connection1*, *connection2*, *connection3*:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/cloud-rpc/src/test/java/io/datakernel/rpc/client/sender/RpcStrategiesTest.java tag:REGION_4 %}
{% endhighlight %}

When we remove some of the connections form the pool, hash function is recalculated:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/cloud-rpc/src/test/java/io/datakernel/rpc/client/sender/RpcStrategiesTest.java tag:REGION_5 %}
{% endhighlight %}

### Type Dispatch Strategy
This strategy simply distributes requests among shards in accordance to the type of the request. In the example 
all **String** requests are sent on the first shard which has `firstValidResult` strategy for the servers. Request 
with all other types are sent to the second shard with `firstAvailable` strategy. As a result, *connection1* and 
*connection2* will process 35 requests, *connection3* - 25 requests, while *connection4* and *connection5* - 0 requests 
as *connection3* was always `firstAvailable`:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/cloud-rpc/src/test/java/io/datakernel/rpc/client/sender/RpcStrategiesTest.java tag:REGION_6 %}
{% endhighlight %}