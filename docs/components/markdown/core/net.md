---
id: net
filename: net
title: Net Module
prev: core/csp.html
next: core/serializer.html
nav-menu: core
layout: core
description: Abstraction layer for building asynchronous HTTP, RPC, TCP servers with no performance overhead.
keywords: tcp,rpc,http,java,java framework,spring alternative,jetty alternative,netty alternative,eventloop,asynchronous server,async
---
## Features

Tiny abstraction layer on top of [Eventloop](/docs/core/eventloop.html) and Java NIO
Adapters for **AsyncTcpSocket**, **AsyncUdpSocket**:
* support of **Promises** for read and write operations
* compatibility with [CSP](/docs/core/csp.html) **ChannelSupplier** and **ChannelConsumer**. **AsyncTcpSocket** can work 
as a CSP channel with built-in back pressure propagation, and can be plugged into CSP/[Datastream](/docs/core/datastream.html) 
pipeline with all its features (like buffering, compression, serialization/deserialization, data transformations, data 
filtering, reducing etc.)
* extensively optimized and has almost no performance overhead, uses [ByteBufPool](/docs/core/bytebuf.html#bytebufpool) widely

**AbstractServer** class serves as a foundation for building Eventloop-aware TCP servers (HTTP servers, RPC servers, TCP 
file services, etc.):
* support of *start/stop* semantics
* implements **EventloopServer**, with *listen/close* capabilities
* implements **WorkerServer** interface, so all subclasses of **AbstractServer** can be readily used as worker servers
* support of **ServerSocketSettings**, **SocketSettings**

Ready-to-use **PrimaryServer** implementation which works in primary Eventloops as balancer: it redistributes external 
accept requests to **WorkerServers**, which do actual accepts in their corresponding worker Eventloop threads

You can add Net module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-net</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

## Examples
* [Ping-Pong Socket Connection](#ping-pong-socket-connection)
* [CSP TCP Client Example](#csp-tcp-client)
* [CSP TCP Server Example](#csp-tcp-server)
* [Datastream TCP Client Example](#datastream-tcp-client)
* [Datastream TCP Server Example](#datastream-tcp-server)


{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project.
<br> These examples are located at <b>datakernel -> examples-> core -> net</b>." %}

#### **Ping-Pong Socket Connection**
In this example we are using an implementation of **AbstractServer** - **SimpleServer** which receives a message and 
sends a response (`PONG`). We also use **AsyncTcpSocketImpl** to send 3 request messages (`PING`).
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/net/src/main/java/PingPongSocketConnection.java tag:REGION_1 %}
{% endhighlight %}

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/net/src/main/java/PingPongSocketConnection.java)

#### **CSP TCP Client**
A simple TCP console client which connects to TCP server:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/net/src/main/java/csp/TcpClientExample.java tag:REGION_1 %}
{% endhighlight %}

It sends characters, receives some data back through CSP channel, parses it and then prints out to console.

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/net/src/main/java/csp/TcpClientExample.java)

#### **CSP TCP Server**
Simple TCP echo server which runs in an eventloop:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/net/src/main/java/csp/TcpServerExample.java tag:REGION_1 %}
{% endhighlight %}

This server listens for connections and when client connects, it parses its message and sends it back as CSP channel via socket.

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/net/src/main/java/csp/TcpServerExample.java)

#### **Datastream TCP Client**

{% include image.html file="http://www.plantuml.com/plantuml/png/dPH1RiCW44Ntd694Dl72aT83LBb3J-3QqmJLPYmO9qghtBrGspME0uwwPHwVp_-2W-N2SDVKmZAPueWWtz2SqS1cB-5R0A1cnLUGhQ6gAn6KPYk3TOj65RNwGk0JDdvCy7vbl8DqrQy2UN67WaQ-aFaCCOCbghDN8ei3_s6eYV4LJgVtzE_nbetInvc1akeQInwK1y3HK42jB4jnMmRmCWzWDFTlM_V9bTIq7Kzk1ablqADWgS4JNHw7FLqXcdUOuZBrcn3RiDCCylmLjj4wCv6OZNkZBMT29CUmspc1TCHUOuNeVIJoTxT8JVlzJnRZj9ub8U_QURhB_cO1FnXF6YlT_cMTXEQ9frvSc7kI6nscdsMyWX4OTLOURIOExfRkx_e1" max-width="750px" %}

This image illustrates communication and transformations between two Datastream servers. Datastream TCP client represents Server#1:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/net/src/main/java/datastream/TcpClientExample.java tag:EXAMPLE %}
{% endhighlight %}

##### [See this example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/net/src/main/java/datastream/TcpClientExample.java)

#### **Datastream TCP Server**
This server represents Server#2 from the illustration above:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/net/src/main/java/datastream/TcpServerExample.java tag:EXAMPLE %}
{% endhighlight %}

##### [See this example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/net/src/main/java/datastream/TcpServerExample.java)
