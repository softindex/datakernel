---
id: todo-list-tutorial
filename: tutorials/todo-list-tutorial
title: To-Do list app using React
prev: core/tutorials/using-react.html
nav-menu: core
layout: core
---
## Introduction
This is a To-Do List app extended example which was created with DataKernel and React. It shows how to integrate React 
in DataKernel project and how to simply manage routing using HTTP module. **You can find full example sources on 
[GitHub](https://github.com/softindex/datakernel/blob/master/examples/tutorials/advanced-react-integration)**.

Here we will consider only the main app class - [**ApplicationLauncher**](#creating-launcher). It uses DataKernel **HttpServerLauncher** and 
**AsyncServlet** classes for setting up embedded application server. With this approach, you can create servers with no 
XML configurations or third-party dependencies. Moreover, **HttpServerLauncher** will automatically take care of 
launching, running and stopping the application, you'll only need to provide launcher with servlets.

### Creating Launcher
**ApplicationLauncher** is the main class of the program. Besides launching the application, it also handles routing and 
most of the corresponding logic. We will use DataKernel **HttpServerLauncher** and extend it:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/advanced-react-integration/src/main/java/ApplicationLauncher.java tag:REGION_1 %}
{% endhighlight %}

Let's take a closer look at the launcher. It can add a new record, get all available records, delete record by its id and 
also mark plans of particular record as completed or not.

So, first, we've defined codecs for our two entities: **Plan** and **Record**:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/advanced-react-integration/src/main/java/ApplicationLauncher.java tag:REGION_1 %}
{% endhighlight %}
These codecs will help us to encode/decode **Plan** and **Record** from/to JSONs to communicate with **TodoService.js**.

Method *object* returns a new **StructuredCodec** and, in case of **Plan** and **Record** entities, requires the 
following parameters:
 * **TupleParser2** *constructor* - basically a constructor of your class with 2 parameters. There are 
 several predefined **TupleParser**s for up to 6 parameters.
 * **String** *field1* - the first field of the encoded/decoded class
 * **Function** *getter1* - getter of *field1*
 * **StructuredCodec** *codec1* - codec for *field1* (depends on the type of the field, for example, `STRING_CODEC`, `BOOLEAN_CODEC`)
 * **String** *field2* - another field of the class
 * **Function** *getter2* - getter of *field2*
 * **StructuredCodec** *codec1* - codec for *field2*

Next, provide **RecordDAO** 
and **AsyncServlet** for loading static content from `/build` directory and taking care of routing.

Routing in DataKernel HTTP module resembles Express approach. Method *map(@Nullable HttpMethod method, String path, AsyncServlet servlet)* 
adds routes to the **RoutingServlet**: 
 * *method* (optional) is one of the HTTP methods (`GET`, `POST` etc) 
 * *path* is the path on the server 
 * *servlet* defines the logic of request processing. If you need to get some data from the *request* while processing you can use:
    * *request.getPathParameter(String key)*/*request.getQueryParameter(String key)* ([see example of query parameter usage](https://github.com/softindex/datakernel/blob/master/examples/core/http/src/main/java/HttpRequestParametersExample.java)) 
     to provide the key of the needed parameter and receive back a corresponding String
    * *request.getPostParameters()* to get a [Promise](http://datakernel.io/docs/core/promise.html) of Map of all request parameters

Pay attention to the requests with `:`, for example:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/advanced-react-integration/src/main/java/ApplicationLauncher.java tag:REGION_3 %}
{% endhighlight %}
`:` states that the following characters until the next `/` is a variable whose keyword, in this case, is *recordId*. 

Also, take a look at the first request:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/advanced-react-integration/src/main/java/ApplicationLauncher.java tag:REGION_2 %}
{% endhighlight %}
`*` states, that whichever path until the next `/` is received, it will be processed by our static servlet, which uploads 
static content from `/build` directory.

## Running the application
If you want to run the example, [Clone DataKernel](https://github.com/softindex/datakernel.git) and import it 
as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then, run the following command in example's folder in terminal:
{% highlight bash %}
$ npm run-script build
{% endhighlight %}

Open **ApplicationLauncher** and run its *main* method. Then open your favourite browser and go to 
[localhost:8080](http://localhost:8080).
Try to add and delete some tasks or mark them as completed.