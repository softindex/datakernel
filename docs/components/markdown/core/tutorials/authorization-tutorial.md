---
id: authorization-tutorial
filename: tutorials/authorization-tutorial
title: Simple app with authorization and session management
prev: core/tutorials/http-decoder.html
next: core/tutorials/using-react.html
nav-menu: core
layout: core
keywords: java,for beginners,java framework,guide,tutorial,web application,async,spring,spring alternative,netty alternative,jetty alternative,authorization,server,client,servlet
description: Create a Java web application with authorization and session management in about 100 lines of code using DataKernel framework.
---
## Introduction
In this extended example you will see how to create a simple authorization app with **log in**/**sign up** scenarios 
and session management. 

DataKernel doesn't include built-in authorization modules or solutions, as this process may significantly vary depending 
on project's business logic. This example represents a simple "best practice" which you 
can extend and modify depending on your needs. **You can find full example sources on [GitHub](https://github.com/softindex/datakernel/tree/master/examples/tutorials/auth).**

In the example we will consider only the [server](#creating-launcher) which was created using DataKernel **HttpServerLauncher** and 
**AsyncServlet**. This approach allows to create embedded application server in about 100 lines of code with no 
additional XML configurations or third-party dependencies.

### Creating Launcher
Let's create an **AuthLauncher**, which is the main part of the application as it manages application lifecycle, routing 
and authorization processes. We will use DataKernel **HttpServerLauncher** and extend it:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_1 %}
}
{% endhighlight %}

We provided the following objects:
* **AuthService** - authorization and register logic 
* **Executor** - needed for StaticLoader
* **StaticLoader** - loads static content from `/root` directory
* **SessionStore** - handy storage for information about sessions
* **AsyncServlet** *servlet* - the main servlet which combines public and private servlets (for authorized and 
unauthorized sessions). As you can see, due to DI, this servlet only requires two servlets without their own dependencies

Now lets provide the **public** and **private** servlets.
* **AsyncServlet** *publicServlet* - manages unauthorized sessions:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_2 %}
{% endhighlight %}

Let's take a closer look at how we set up routing for servlets. DataKernel approach resembles Express. For example, 
here's the request to the homepage for unauthorized users:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_3 %}
{% endhighlight %}

Method *map(@Nullable HttpMethod method, String path, AsyncServlet servlet)* adds the route to the **RoutingServlet**: 
 * *method* (optional) is one of the HTTP methods (`GET`, `POST` etc) 
 * *path* is the path on the server 
 * *servlet* defines the logic of request processing. If you need to get some data from the *request* while processing you can use:
    * *request.getPathParameter(String key)*/*request.getQueryParameter(String key)* ([see example of query parameter usage](/docs/core/http.html#request-parameters-example)) 
  to provide the key of the needed parameter and receive back a corresponding String
    * *request.getPostParameters()* to get a Map of all request parameters

`GET` requests with paths **"/login"** and **"/signup"** upload the needed HTML pages.
`POST` requests with paths **"/login"** and **"/signup"** take care of log in and sign up logic respectively:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_4 %}
{% endhighlight %}

Pay attention at `POST` **"/login"** rout. *serveFirstSuccessful* takes two servlets and waits until one of them 
finishes processing successfully. So if authorization fails, a Promise of **null** will be returned (**AsyncServlet.NEXT**), 
which means fail. In this case, simple **StaticServlet** will be created to load the *errorPage*. Successful log in will 
generate a session *id* for user, save string `"My saved object in session"` to browser cookies and also redirect user 
to **"/members"**.

Now let's get to the next servlet which handles authorized sessions. 
* **AsyncServlet** *privateServlet* - manages authorized sessions:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_5 %}
{% endhighlight %}

First, it redirects requests from homepage to **"/members"**: 
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_6 %}
{% endhighlight %}

Next, it takes care of all of the requests that go after **"/members"** path:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_7 %}
{% endhighlight %}

Pay attention to the path **"/members/*"**. `*` states, that whichever path until the next `/` goes after **"/members/"**, 
it will be processed by this servlet. So, for example this route:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_8 %}
{% endhighlight %}
is a GET request for **"/members/cookie"** path. This request shows all cookies stored in the session.

**"/members/logout"** logs user out, deletes all cookies related to this session and redirects user to homepage.

After **public** and **private** servlets are set up, we define `main()` method, which will start our launcher:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/auth/src/main/java/AuthLauncher.java tag:REGION_9 %}
{% endhighlight %}

## Running the application 
If you want to run the example, [clone DataKernel](https://github.com/softindex/datakernel.git) and import it 
as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Open `AuthLauncher` class and run its *main()* method.
Then open your favourite browser and go to [localhost:8080](http://localhost:8080). Try to sign up and then log in. When 
logged in, check out your saved cookies for session. You will see the following content: `My saved object in session`. 
Finally, try to log out. You can also try to log in with invalid login or password. 