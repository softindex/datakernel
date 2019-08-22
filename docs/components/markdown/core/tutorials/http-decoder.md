---
id: http-decoder
filename: tutorials/http-decoder
nav-menu: core
layout: core
title: Form Validation Using HTTP Decoder
prev: core/tutorials/template-engine-integration.html
next: core/tutorials/authorization-tutorial.html
keywords: java,java for beginners,java framework,guide,tutorial,web application,server,servlet,spring,spring alternative,netty alternative,jetty alternative,decoder,mvc,form validation,async
description: Create an async Java servlet which can parse requests and process form validation using DataKernel components.
---
## Introduction
In this example we created an async servlet that adds contacts to the list, parse requests and process form 
validation with the help of `HttpDecoder`. **You can find full example sources on 
[GitHub](https://github.com/softindex/datakernel/tree/master/examples/tutorials/auth).**

Here we will consider only **HttpDecoderExample** class with **AsyncServlet** as it contains 
DataKernel-specific features.

Consider this example as a concise presentation of MVC pattern:
* To model a **Contact** representation, we will create a plain java class with fields (name, age, address), constructor and accessors to the fields.
* To simplify example, we will use an **ArrayList** to store the **Contact** objects. **ContactDAO** interface and its implementation are used for this purpose.
* To build a view we will use a single html file, compiled with the help of the Mustache template engine.
* An **AsyncServlet** will be used as a controller. We will also add **RoutingServlet** to determine respond to a particular endpoint.
* **HttpDecoder** provides you with tools for parsing requests. 

### Creating `HttpDecoderExample` Class
Let's create **HttpDecoderExample** class which extends **HttpServerLauncher**. **HttpServerLauncher** will take care 
of the lifecycle of our server, will start and stop needed services. Next, provide two custom parsers which will be used 
for validation - *ADDRESS_DECODER* and *CONTACT_DECODER*:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/decoder/src/main/java/HttpDecoderExample.java tag:REGION_1 %}
{% endhighlight %}

Also, we need to create *applyTemplate(Mustache mustache, Map<String, Object> scopes)* method, which will fill the provided 
Mustache template with the given data:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/decoder/src/main/java/HttpDecoderExample.java tag:REGION_5 %}
{% endhighlight %}

And provide a **ContactDAOImpl** factory method:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/decoder/src/main/java/HttpDecoderExample.java tag:REGION_6 %}
{% endhighlight %}

Now we have everything needed to create **AsyncServlet** to handle requests:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/decoder/src/main/java/HttpDecoderExample.java tag:REGION_2 %}
{% endhighlight %}

* Here we provide an **AsyncServlet**, which receives **HttpRequest** from clients, creates **HttpResponse** depending on route path and sends it.
* Inside the **RoutingServlet** two route paths are defined. First one matches requests to the root route `"/"` - 
it simply displays a contact list.
The second one, `"/add"` - is an HTTP `POST` method which adds or dismisses new users. We will process this request parsing with the 
help of aforementioned **HttpDecoder**, using *decode(request)* method:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/decoder/src/main/java/HttpDecoderExample.java tag:REGION_3 %}
{% endhighlight %}
* **Either** represents a value of two possible data types (**Contact**, **DecodeErrors**). **Either** is either **Left** or **Right**.
We can check if **Either** contain only **Left**(**Contact**) or **Right**(**DecodeErrors**) using *isLeft* and *isRight* methods.

Finally, write down the *main()* method which will launch our application:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/decoder/src/main/java/HttpDecoderExample.java tag:REGION_4 %}
{% endhighlight %}

### Running the application
If you want to run the example, you need to [clone DataKernel](https://github.com/softindex/datakernel.git) and
import it as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open **HttpDecoderExample** class, which is located at **datakernel -> examples -> tutorials -> decoder**
and run its *main()* method. Open your favourite browser and go to [localhost:8080](http://localhost:8080).