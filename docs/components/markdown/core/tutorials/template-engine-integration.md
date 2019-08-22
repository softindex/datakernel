---
id: template-engine-integration
filename: tutorials/template-engine-integration
title: Simple web application with template engine integration
prev: core/tutorials/getting-started.html
next: core/tutorials/http-decoder.html
nav-menu: core
layout: core
keywords: java,java framework,tutorial,java for beginners,guide,http server,spring alternative,netty alternative,jetty alternative,template engine,mustache,poll application
description: Create poll web-application using DataKernel Java framework and Mustache template engine. The embedded application server in 100 lines of code and no xml configurations.
---
## Introduction
In this example you can explore how to implement template engines in DataKernel applications. This example is a Poll app 
which can create new polls with custom title, description and options. After poll is created, its unique link is generated. It 
leads to a page where you can vote.

See how simple it is to implement such features using DataKernel HTTP module. The 
embedded application server has only about **100 lines of code** with **no additional xml 
configurations**. In this example we used **Mustache** as a template engine.

Here we will consider only [**ApplicationLauncher** class](#creating-launcher), which is the main class of the application. **You can find 
full example sources on [GitHub](https://github.com/softindex/datakernel/tree/master/examples/tutorials/template-engine).**

### Creating launcher
**ApplicationLauncher** launches our application and takes care of routing and generating needed content on HTML pages. We 
will extend DataKernel **HttpServerLauncher**, which manages application lifecycle:

{% include note.html content=" In this example we are omitting error handling to keep everything brief and simple." %}

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/template-engine/src/main/java/ApplicationLauncher.java tag:REGION_1 %}
}
{% endhighlight %}

Let's have a closer look at the launcher. 

* *applyTemplate(Mustache mustache, Map<String, Object> scopes)* fills the provided Mustache template with given data.
* provide **PollDaoImpl** which includes business logic of our application. 

Next, we provide **AsyncServlet**:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/template-engine/src/main/java/ApplicationLauncher.java tag:REGION_2 %}
}
{% endhighlight %}

In the **AsyncServlet** we create three Mustache objects for our three HTML pages. 
Then we create a DataKernel **RoutingServlet** and define routing. Routing approach resembles Express. In the example 
we've added the request to the homepage.

In the request we are getting all current polls and information about them. This information is used to generate *listPolls* page correctly.

Method *map(@Nullable HttpMethod method, String path, AsyncServlet servlet)* adds the route to the **RoutingServlet**: 
 * *method* is one of the HTTP methods (`GET`, `POST` and so on) 
 * *path* is the path on the server 
 * *servlet* defines the logic of request processing. If you need to get some data from the *request* while processing you can use:
     * *request.getPathParameter(String key)*/*request.getQueryParameter(String key)* ([see example of query parameter usage](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpRequestParametersExample.java) 
      to provide the key of the needed parameter and receive back a corresponding String
     * *request.getPostParameters()* to get a Map of all request parameters

Let's add one more request:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/template-engine/src/main/java/ApplicationLauncher.java tag:REGION_3 %}
{% endhighlight %}

This request returns a page with specific poll (if there is a poll with such *id*). 
Pay attention to the provided path `/poll/:id`. `:` states that the following characters until the next `/` is a 
variable which keyword is, in this case, *id*. 

The next requests with `/create`, `/vote`, `/add` and `/delete` paths take care of providing page for creating 
new polls, voting, adding created polls to the *pollDao* and deleting them from the *pollDao* respectively:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/template-engine/src/main/java/ApplicationLauncher.java tag:REGION_4 %}
{% endhighlight %}

Also, we defined *main()* method which will start our launcher:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/template-engine/src/main/java/ApplicationLauncher.java tag:REGION_5 %}
{% endhighlight %}

And that's it, we have a full-functioning poll application!

## Running the application
If you want to run the example, you need to [clone DataKernel](https://github.com/softindex/datakernel.git) and import 
it as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).
Open **PollLauncher** class and run its *main()* method.
Then open your favourite browser and go to [localhost:8080](http://localhost:8080).