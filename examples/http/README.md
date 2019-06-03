### "Hello World" Server Examples:
 * [Simple "Hello World" Server](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HelloWorldExample.java) - 
 a simple async server created using `AsyncHttpServer`.
 * [HTTP Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpServerExample.java) - 
a simple HTTP Server utilizing `HttpServerLauncher`. 
 * [HTTP Multithreaded Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpMultithreadedServerExample.java) - 
HTTP multithreaded server example. 
 * [HTTP Client Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpClientExample.java) - 
an HTTP client example utilizing `Launcher`. 

[Launch](#1-hello-world-server-examples)

### Servlet Examples:
 * [Middleware Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/MiddlewareServletExample.java) - 
an example of `MiddlewareServlet` usage.
 * [Request Parameter Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/RequestParameterExample.java) - 
an example of processing requests with parameter.
 * [Static Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/StaticServletExample.java) - 
an example of `StaticServlet` utilizing. 
 * [Servlet Decorator Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/ServletDecoratorExample.java) - 
an example of using `AsyncServletDecorator`, a wrapper over `AsyncServlet`.
 * [Routing Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/RoutingServletExample.java) - 
 an example of `RoutingServlet` usage.
 * [Blocking Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/BlockingServletExample.java) - 
 an example of handling complex operations in new thread.

[Launch](#2-servlet-examples)

### 1. "Hello World" Server Examples
#### Launch
To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:
* `HelloWorldExample`, `HttpServerExample` - simple HTTP servers, which send a "Hello World" response
* `HttpMultithreadedExample` - multithreaded HTTP server with 4 workers
which are located at **datakernel -> examples -> http** and run *main()* of the chosen example.

To check how **HelloWorldExample**, **HttpServerExample** or **HttpMultithreadedServerExample** works, you should start your client 
by opening `HttpClientExample` class which is located at the same folder and running its *main()* method. Or you can open your 
favorite browser and go to [localhost:8080](http://localhost:8080). 

#### Explanation

If you connect to the multithreaded server, you'll receive a message representing which worker processed your request:
```
"Hello from worker server #..." 
```
Otherwise, you'll see a message: `"Hello World!"`

`HelloWorldExample` doesn't use any predefined launchers, only `AsyncHttpServer` class of HTTP module.

`HttpServerExample` utilizes `HttpServerLauncher`, while `HttpMultithreadedServerExample` extends 
`MultithreadedHttpServerLauncher` which allows to simply create servers with several workers.
 
When using predefined launchers, you should override the following methods:
 * *getBusinessLogicModules()* - to specify the actual logic of your application
 * *getOverrideModules()* - to override default modules.


### 2. Servlet Examples
#### Launch
To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:
* `MiddlewareServletExample`
* `RequestParametrExample`
* `StaticServletExample`
* `ServletDecoratorExample`
* `RoutingServletExample`
* `BlockingServletExample`

which are located at **datakernel -> examples -> http** and run *main()* of the chosen example.

Now you can connect to you server by visiting [localhost:8080](http://localhost:8080/) in your browser.

#### Explanation

**Middleware Servlet Example** processes requests and redirects you to chosen web-page. You can set up configurations for your 
Servlet with method *with()*. It sets up HTTP methods (optionally), paths and `AsyncServlet`.

**Request Parameter Example** represents requests with parameters which are received with *getPostParameters()* and then 
utilized with *postParameters.get()*.

**Static Servlet Example** shows how to set up and utilize `StaticServlet`. Method *StaticServlet.create()* returns a 
new `StaticServlet`. 

**ServletDecoratorExample** shows basic functionality of [**AsyncServletDecorator**](https://github.com/softindex/datakernel/blob/master/core-http/src/main/java/io/datakernel/http/AsyncServletDecorator.java) 
class. It creates a wrap over **AsyncServlets** and adds behaviour for particular events, for example exception handling or 
processing received responses. To add new behaviour, use *wrappedDecoratorOf(AsyncServletDecorator... decorators)*. 
In the example, we defined exception handling and made loading request body default in the servlet.

**RoutingServletExample** represents how to set up routing in HTTP module. This process resembles Express approach. To add a rout to 
**RoutingServlet**, you should use method with(@Nullable HttpMethod method, String path, AsyncServlet servlet). 
 * *method* (optional) is one of the HTTP methods (GET, POST etc)
 * *path* is the path on the server
 * *servlet* defines the logic of request processing.
 
 **BlockingServletExample** shows how to create new thread for processing some complex operations on servlet.