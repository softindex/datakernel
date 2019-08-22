---
id: welcome-article
filename: articles/welcome-article
title: Introduction to Java DataKernel for Node.js Developers
nav-menu: core
layout: core
keywords: java,nodejs,node js,for beginners,migrate to java,server,web application,servlet,express,java framework
description: DataKernel framework is a perfect start if you're a Node.js developer who would like to move to Java. DataKernel incorporates the concepts you're already familiar with, such as asynchronous work, Event Loop and single-threaded Promises. 
---

This article is for Node.js developers who have basic Java knowledge and intend to continue with Java, 
looking for an alternative to the intricate modern Java frameworks.

DataKernel is a *lightweight asynchronous* framework that preserves all the Java advantages and implements 
Node.js-inspired features.

But first things first. Let's briefly go through the necessary prerequisites.

*If you already have JDK, IDE and Maven installed, you can skip this part and go directly to the [Comparison](#comparison)
or [Getting Started](/docs/core/tutorials/getting-started.html) tutorial.*

## What you will need:
* JDK 1.8+
* IDE (might be IntelliJ IDEA)
* Maven 3.0+

### Step-by-step guide
* For JDK installation consider this [source](https://www3.ntu.edu.sg/home/ehchua/programming/howto/JDK_Howto.html)
* [Download](https://www.jetbrains.com/idea/download/#section=linux) the installer of IntelliJ IDEA Community Edition.
Run it and follow the wizard steps.
For detailed information - [installation guide]( https://www.jetbrains.com/help/idea/installation-guide.html) 
and [tips](https://www.jetbrains.com/help/idea/run-for-the-first-time.html)

* We will also use [Maven](https://maven.apache.org/what-is-maven.html) to build the projects. You can [download](https://maven.apache.org/download.cgi)
it and [install](https://maven.apache.org/install.html).

Then you can navigate to [Getting Started](/docs/core/tutorials/getting-started.html) tutorial and launch
a simple [HTTP](/docs/core/http.html) server.

## Comparison
Let's make a concise comparison of **Node.js** and **DataKernel**, to give you a better understanding of what is going on.

### Core similarities
* Both are asynchronous
* Use single-threaded async Promises
* Run in Event Loop
 
### Core differences
* Java is a strongly typed programming language, so DataKernel uses this too.
* Unlike JS, Java doesn't have async/await special syntax. Instead, DataKernel uses [Promises](/docs/core/promise.html).
* Since DataKernel preserves the Java advantages, [multithreading](/docs/core/workerpool.html) can be used too. 
 For example, to run several Event Loops in parallel inside one JVM process.

### Hello World servers
{% highlight js %}
//Node.js
const http = require('http');

const hostname = '127.0.0.1';
const port = 3000;

const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('Hello World\n');
});

server.listen(port, hostname, () => {
  console.log(`Server is running`);
});
{% endhighlight %}

DataKernel provides two options:
 * server runs in the explicitly provided [Event Loop](/docs/core/eventloop.html) 

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/HelloWorldExample.java tag:REGION_1 %}
{% endhighlight %}

 * with the help of the [Launcher](/docs/core/launcher.html) class that will provide Event Loop automatically.
 
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/getting-started/src/main/java/HttpHelloWorldExample.java tag:EXAMPLE%}
{% endhighlight %}

* Despite the typical Java concept that regular application is usually a WAR archive that should be deployed to some 
[application server](https://en.wikipedia.org/wiki/Application_server), 
DataKernel application has embedded server. So it looks like a regular Java program. You just launch it and that's it - 
like Node.js.

* DataKernel supports running *Event Loop* in the *main*, like Node.js executes in Event Loop. Moreover, DK provides features 
like Launcher and [DI](/docs/core/di.html).

### Promises
{% highlight js %}
//JS
var promise = new Promise( function(resolve, reject) {  resolve('Hello World'); } )
//DataKernel
Promise<String> promise = Promise.of("Hello World");
{% endhighlight %}

* JS has several dynamical chaining methods - *then()*, *catch()*.
{% highlight js %}
doSomething()
      .then(doSomethingElse)
      .catch(handleError)
      .then(doMoreStuff)
      .then(doFinalThing)
      .catch(handleAnotherError)
{% endhighlight %}

* DataKernel provides a wide range of methods - *then()*, *map()*, *thenEx()*, *mapEx()*, *whenResult()*, *whenException()* etc.
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseChainExample.java tag:REGION_1%}
{% endhighlight %}

* JS allows to execute promise-ified functions using *all()* method.

{% highlight js %}
Promise.all(arrayOfPromises)
.then(function(arrayOfResults) {
    /* Do something when all Promises are resolved */
})
.catch(function(err) {
    /* Handle error if any of Promises fails */
})
{% endhighlight %}

* DataKernel provides even more useful methods - *combine()*, *both()*, *either()*, *any()* etc.
Have a look at *combine()*:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseAdvanceExample.java tag:REGION_1%}
{% endhighlight %}

For more examples of these methods, navigate to [Promises](/docs/core/promise.html).

### Middleware VS Servlets
* Express.js allows to use a chain of functions (middlewares) that are called one after the other, make any modifications to request, and then send a response back.
* DataKernel suggests an alternative named *routing servlets*.

Compare:

{% highlight java %}
//Express.js
app.use('/', function (req, res, next) {
  res.status(200).send('Hello World!')
})

//DataKernel
RoutingServlet.create()
	.map(GET, "/", request -> Promise.of(
		HttpResponse.ok200().withPlainText("Hello World!")));
{% endhighlight %}

* DK routing servlet allows to simply deal with the application’s endpoints. 
* You can create servlets for each of the specified paths to handle requests independently and preserve modularity.
* DataKernel uses functional composition, so servlets can be wrapped one in another.
{% highlight java %}
AsyncServlet servlet(AsyncServlet servlet1, AsyncServlet servlet2) {
		return RoutingServlet.create()
				.map("/", $ -> Promise.of(HttpResponse.ok200().withHtml("<a href=\"/first\">first</a><br><a href=\"/second\">second</a>")))
				.map("/first", servlet1)
				.map("/second", servlet2);
	}

	AsyncServlet servlet1() {...}
	AsyncServlet servlet2() {...}
}
{% endhighlight %}

* Whereas Node.js always follows the chaining construct, like:

{% highlight js %}
var app = express()
var router = express.Router()

router.use(function (req, res, next) { ... })

router.use('/first', function (req, res, next) {
  ...
  next()
}, function (req, res, next) {
  ...
  next()
})

router.get('/second', function (req, res, next) { ... })

app.use('/', router)
{% endhighlight %}

## What's next?
To make DataKernel more developer-friendly, we've created dozens of tutorials and examples of different scales, 
representing most of the framework's capabilities. You can also go through [tutorials](/docs/core/tutorials/getting-started.html) 
or explore our [docs](/docs/core/index.html) first.
