"Hello World" Server Examples:
1. [HTTP Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpServerExample.java) - 
a simple HTTP Server utilizing `HttpServerLauncher`. 
2. [HTTP Multithreaded Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpMultithreadedServerExample.java) - 
HTTP multithreaded server example. 
3. [HTTP Client Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpClientExample.java) - 
an HTTP client example utilizing `Launcher`. 

[Launch](#1-hello-world-server-examples)

Servlet Examples:
1. [Middleware Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/MiddlewareServletExample.java) - 
an example of `MiddlewareServlet` usage.
2. [Request Parameter Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/RequestParameterExample.java) - 
an example of processing requests with parameter.
3. [Static Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/StaticServletExample.java) - 
an example of `StaticServlet` utilizing. 
4. [ServletDecoratorExample](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/ServletDecoratorExample.java) - 
an example of using `AsyncServletDecorator`, a wrapper over `AsyncServlet`.

[Launch](#2-servlet-examples)

### 1. "Hello World" Server Examples
#### Launch
To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:
* `HttpServerExample`
* `HttpMultithreadedExample`
which are located at **datakernel -> examples -> http** and run *main()* of the chosen example.

To check how **HTTP Server Example** or **HTTP Multithreaded Server Example** works, you should start your client 
by opening `HttpClientExample` class which is located at the same folder and running its *main()* method.

#### Explanation

If you connect to the multithreaded server, you'll receive a message representing which worker processed your request:
```
"Hello from worker server #..." 
```
Otherwise, you'll see a message: `"Hello World!"`

The difference between **HTTP Multithreaded Server Example** and **HTTP Server Example** is that the first one creates 
several worker threads for requests processing. 

`HttpServerExample` utilizes `HttpServerLauncher` while `HttpMultithreadedServerExample` extends 
`MultithreadedHttpServerLauncher`. When using predefined launchers, you should override the following methods:
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