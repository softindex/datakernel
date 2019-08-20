---
id: service-graph
filename: service-graph
title: Service Graph
prev: core/launcher.html
next: core/workerpool.html
nav-menu: core
layout: core
description: Service Graph starts and stops application services according to their dependency graph using multithreaded graph traversal algorithm.
keywords: service graph,java,java framework,spring alternative,netty alternative,jetty alternative,application services,threadpool,closeables,datasource
---

* Designed to be used in combination with [DataKernel DI](/docs/core/di.html) and 
[DataKernel Launcher](/docs/core/launcher.html) as a mean to start/stop application services according to their 
dependency graph
* Starts services by multithreaded graph traversal algorithm: leaf services first and so on
* Stops services in opposite direction
* Services dependency graph is automatically built upon DataKernel DI dependencies graph, but can be customized by 
user-specified dependencies.
* Supports multiple standard services like **ThreadPool**, **Closeables**, **DataSource** as well as DataKernel-specific 
services like [eventloops](/docs/core/eventloop.html), async servers and async services.
* Fully supports async services provided in *@Worker* scope - in that case, a whole vector of such worker instances is 
started/stopped
* Can be configured to support other services as well with user-provided adapters

To get a basic understanding of ServiceGraph role, let's have a look at a very simple example of an HTTP Server:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/getting-started/src/main/java/HttpHelloWorldExample.java tag:EXAMPLE %}
{% endhighlight %}

* This application extends predefined **HttpServerLauncher** which includes **ServiceGraphModule**. 
* **HttpServerLauncher** uses two services: an **AsyncHttpServer** and an **Eventloop** for it. 
{% include image.html file="/static/images/http-hello-world-service-graph.png" max-width="400px" %}
* According to this service graph, Service Graph starts **Eventloop** first. Only after that it starts the dependent 
**AsyncHttpServer**.
* When application stops, the services will be stopped in opposite direction: **AsyncHttpServer** first and 
**Eventloop** next.


## Examples

* [Simple Service Graph Example](#simpleserviceexample)
* [Eventloop Service Example](#eventloopserviceexample)
* [Advanced Service Example](#advancedserviceexample)

{% include note.html content="To run the example, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the example, build the project.
<br> These examples are located at <b>datakernel -> examples -> core -> boot</b>" %}

#### **SimpleServiceExample**
In this example we create an application, which extends Launcher and has a simple custom service which basically only 
starts and stops:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/boot/src/main/java/SimpleServiceExample.java tag:EXAMPLE %}
{% endhighlight %}

#### **EventloopServiceExample**
You can create your custom services and Service Graph will also start and stop them:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/boot/src/main/java/EventloopServiceExample.java tag:EXAMPLE %}
{% endhighlight %}

#### **AdvancedServiceExample**
Service Graph can manage more complex services dependencies. For example, let's assume we have an e-mail service 
prototype. To work properly, it requires two services - authorization service and database service. Authorization 
service also requires database service, along with Eventloop and Executor. So, we have the following service graph:
{% include image.html file="/static/images/advanced-service-graph.png" max-width="600px"%}
And **ServiceGraphModule** will start and stop all those services in the proper order:

{% highlight bash %}
 === STARTING APPLICATION

Started java.util.concurrent.Executor 
Started io.datakernel.eventloop.Eventloop 
Started AdvancedServiceExample$DBService 

Started AdvancedServiceExample$AuthService

Started AdvancedServiceExample$EmailService

 === STOPPING APPLICATION

Stopped AdvancedServiceExample$EmailService

Stopped AdvancedServiceExample$AuthService

Stopped java.util.concurrent.Executor
Stopped io.datakernel.eventloop.Eventloop 
Stopped AdvancedServiceExample$DBService
{% endhighlight %}

So, this application looks as follows:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/boot/src/main/java/AdvancedServiceExample.java tag:EXAMPLE %}
{% endhighlight %}
