
1. [HTTP Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpServerExample.java)
2. [HTTP Multithreaded Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpMultithreadedServerExample.java)
3. [HTTP Client Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpClientExample.java)
4. [Middleware Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/MiddlewareServletExample.java)
5. [Request Parameter Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/RequestParameterExample.java)
6. [Static Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/StaticServletExample.java)

To run the examples, you should first execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/http
$ mvn clean compile exec:java@HttpServerExample
$ # or
$ mvn clean compile exec:java@HttpMultithreadedServerExample
$ # or
$ mvn clean compile exec:java@MiddlewareServletExample
$ # or
$ mvn clean compile exec:java@RequestParametrExample
$ # or
$ mvn clean compile exec:java@StaticServletExample
```

The difference between multithreaded HTTP server and simple HTTP server is that the first one creates several worker 
threades for requests processing. In the example above your server will have 4 workers.

To check how HTTP Server or HTTP Multithreaded Server works, you can start your client:
```
$ mvn clean compile exec:java@HttpClientExample
```

If you connected to the multithreaded server, you'll receive a message representing which worker processed your request:
```
"Hello from worker server #..." 
```

Otherwise, you'll see a message: `"Hello World!"`

If you run Examples 4-6, you can connect to you server by visiting [this link](http://localhost:8080/) in your browser.

Middleware Servlet Example processes requests and redirects to needed web-page. 

Request Parameter Example represents requests with `getPostParameters()` and then utilizing them with  `postParameters.get()`.

Static Servlet Example shows how to set up and utilize `StaticServlet`.