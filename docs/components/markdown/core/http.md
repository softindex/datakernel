---
id: http
filename: http
title: HTTP Module
prev: core/datastream.html
next: core/codec.html
nav-menu: core
layout: core
description: Create legacy-free asynchronous HTTP server and client applications with DataKernel.
keywords: java http,java,http server,http client,java http server,java http client,java framework,spring alternative,netty alternative,jetty alternative
---
## Features

Provides tools for building HTTP servers and clients with asynchronous I/O in a simple and convenient way:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/getting-started/src/main/java/HttpHelloWorldExample.java tag:EXAMPLE%}
{% endhighlight %}

Legacy-free approach, designed to be asynchronous
 * no legacy layers of adapters
 * uses low-overhead, but high-level abstractions: **AsyncServlets**, [**Promises**](/docs/core/promise.html) and [CSP channels](/docs/core/csp.html)
 * can be used as application web server: support of externally provided [DI](/docs/core/di.html) Modules with business logic and **AsyncServlets**



Simple **AsyncServlet** interface
 * basically, it's just an async function, mapping **HttpRequest** to **Promise&lt;HttpResponse>**
 * collection of pre-defined **AsyncServlets** out of the box ([**StaticServlet**](#static-servlet-example), 
 [**BlockingServlet**](#blocking-servlet-example), [**RoutingServlet**](#routing-servlet-example) etc.)
 * extensive support of functional composition of **AsyncServlets**



[**RoutingServlet**](#routing-servlet-example) for building servlet routing 
 * flexible mapping of HTTP paths and methods to **AsyncServlets** (including other RoutingServlets)
 * support of path parameters (like */users/:id*) and relative paths
 
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/RoutingServletExample.java tag:REGION_1 %}
{% endhighlight %}



[**AsyncServletDecorators**](#servlet-decorator-example) for pre- and post- processing of **HttpRequest** and **HttpResponse**:
 * **HttpRequest** and **HttpResponse** listeners
 * mappers of **HttpExceptions** to **HttpResponse** (to render application errors across entire servlet tree in a consistent manner)
 * **HttpRequest** body preload
 * functional composition of **AsyncServletDecorators**
 * can be compared to Node.js ‘middleware’ pre- and post- filters, but with heavy emphasis on functional Java 8+ programming style



**HttpDecoder** mini-framework:
 * brief DSL for building user-defined reusable decoders of **HttpRequests** into structured app-specific POJO classes
 * built-in support of user-provided validators and error messages
 * error messages can be fully localized, while being template engines-friendly



Highly optimized and GC-friendly:
 * automatic recycling of [**ByteBufs**](/docs/core/bytebuf.html) associated with **HttpRequest** and **HttpResponse** 
 and also **ByteBufs** received from **AsyncSocket**
 * optimized headers multimap and internal URL representation with low yield of garbage objects
 * specialized headers subclasses render their content directly into **ByteBuf**, without intermediate garbage objects



You can add HTTP module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-http</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}



## Examples
 * [Simple "Hello World" Server](#simple-hello-world-server) - simple async server created using **AsyncHttpServer**.
 * ["Hello World" Server with Pre-defined Launcher](#hello-world-server-with-pre-defined-launcher) - HTTP module 
 provides you with some pre-defined launchers, which are extremely simple to use to create servers.
 * [Custom Server](#custom-server) - example of creating a server from scratch using **Launcher**. 
 * [Multithreaded Server Example](#multithreaded-server-example) - HTTP multithreaded server example. 
 * [Request Parameters Example](#request-parameters-example) - example of processing requests with parameter.
 * [Static Servlet Example](#static-servlet-example) - example of **StaticServlet** utilizing. 
 * [Servlet Decorator Example](#servlet-decorator-example) - example of using **AsyncServletDecorator**, a wrapper over **AsyncServlet**. 
 * [Routing Servlet Example](#routing-servlet-example) - example of **RoutingServlet** usage for creating servlet tree. 
 * [Blocking Servlet Example](#blocking-servlet-example) - example of handling complex operations on server in new thread. 
 * [File Upload Example](#file-upload-example) - example of uploading a file from client local storage to server. 
 * [Client Example](#client-example) - creating an HTTP client utilizing **Launcher**.  


{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project." %}


#### Simple "Hello World" Server
**HelloWorldExample** uses **AsyncHttpServer** class of HTTP module. It is a non-blocking server, which works in an eventloop:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/HelloWorldExample.java tag:REGION_1 %}
{% endhighlight %}

So, this server runs in the provided eventloop and waits for connections on port `8080`. When server receives a request, 
it sends back a Promise of greeting response. 

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/HelloWorldExample.java)


#### "Hello World" Server with pre-defined Launcher
Launchers manage application lifecycle and allow to create applications simply and minimalistically:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/getting-started/src/main/java/HttpHelloWorldExample.java tag:EXAMPLE%}
{% endhighlight %}
So, all you need to do is provide a servlet which processes the requests and launch the application. 
**HttpServerLauncher** will take care of everything else.


#### Custom Server
With Launcher you can easily create HTTP servers from scratch. In this example we're creating a simple server which sends 
a greeting:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/CustomHttpServerExample.java tag:EXAMPLE %}
{% endhighlight %}

First, we provide an eventloop, servlet and an async server itself. Then, we override **getModule** to provide our server with 
configs and **ServiceGraphModule** for building dependency graph.

Finally, we override Launcher main method *run()* and then define *main* method of the example. 

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/CustomHttpServerExample.java).


#### Multithreaded Server Example
In this example we are using pre-defined **MultithreadedHttpServerLauncher** to create a multithreaded HTTP server. By 
default, there will be 4 worker servlets with **workerId**s, each of them sends back a greeting and the number of worker 
which served the connection:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/MultithreadedHttpServerExample.java tag:EXAMPLE %}
{% endhighlight %}


To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 


##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/MultithreadedHttpServerExample.java).



#### Request Parameters Example
Represents requests with parameters which are received with methods *getPostParameters* and *getQueryParameter* 

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpRequestParametersExample.java tag:REGION_1 %}
{% endhighlight %}

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpRequestParametersExample.java).


#### Static Servlet Example
Shows how to set up and utilize `StaticServlet` to create servlets with some static content, in our case it will get 
content from `static/site` directory.

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/StaticServletExample.java tag:EXAMPLE %}
{% endhighlight %}

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/StaticServletExample.java).


#### Servlet Decorator Example
Shows basic functionality of [**AsyncServletDecorator**](https://github.com/softindex/datakernel/blob/master/core-http/src/main/java/io/datakernel/http/AsyncServletDecorator.java) 
class. It creates a wrap over **AsyncServlets** and adds behaviour for particular events, for example exception handling or 
processing received responses. 
In the example, we made loading of request body default on the servlet using *loadBody()*:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/ServletDecoratorExample.java tag:REGION_1 %}
{% endhighlight %}

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/ServletDecoratorExample.java).


#### Routing Servlet Example
Represents how to set up servlet routing tree. This process resembles Express approach. To add a rout to 
**RoutingServlet**, you should use method *map*:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/RoutingServletExample.java tag:REGION_2 %}
{% endhighlight %}

* *method* (optional) is one of the HTTP methods (GET, POST etc)
* *path* is the path on the server
* *servlet* defines the logic of request processing.

So, the whole servlet tree will look as follows:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/RoutingServletExample.java tag:REGION_1 %}
{% endhighlight %}

Note this request:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/RoutingServletExample.java tag:REGION_3 %}
{% endhighlight %}

`*` states, that whichever path until the next `/` is received, it will be processed by this servlet

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/RoutingServletExample.java).

#### Blocking Servlet Example
Shows how to create new thread for processing some complex operations on servlet.

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/BlockingServletExample.java tag:EXAMPLE %}
{% endhighlight %}

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/BlockingServletExample.java).

#### File Upload Example
In this example user uploads some file from local storage to the server:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/FileUploadExample.java tag:EXAMPLE %}
{% endhighlight %}

To check how the example works, open your favorite browser and go to [localhost:8080](http://localhost:8080). 

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/FileUploadExample.java).

#### Client Example
`ClientExample` is an example of creating an HTTP client. It extends **Launcher** and utilizes pre-defined **AsyncHttpClient** 
and **AsyncDnsClient** (can resolve given domain names into their respective IP addresses):

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpClientExample.java tag:REGION_1 %}
{% endhighlight %}

Next, *getModule* provides with needed configs and dependency graph **ServiceGraph**:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpClientExample.java tag:REGION_2 %}
{% endhighlight %}

Since our client extends **Launcher**, it overrides method *run* which defines the main functionality. In our case, it 
sends a request, waits for server response (either successful or failed) and then processes it:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpClientExample.java tag:REGION_3 %}
{% endhighlight %}

*eventloop.submit* submits request sending and response receiving to eventloop thread. So, our main thread will wait until 
*future* in eventloop thread will return a result and only then the response will be printed out.

To check how the client works, launch [Simple "Hello World" Server](#simple-hello-world-server) or 
[Server Scratch](#custom-server) and then run **ClientExample**.

##### [See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpClientExample.java).
