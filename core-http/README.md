HTTP module enables users to build HTTP servers and clients that take full advantage of asynchronous I/O.

* HTTP Server - ideal for web services which require async I/O (like using RPC or calling other web services for serving
 requests)
* HTTP Client - ideal for high-performance clients of web services with a large number of parallel HTTP requests
* [up to ~238K of requests per second per core](#benchmark)
* ~50K of concurrent HTTP connections
* Low GC pressure
* Built on top of [Eventloop](/docs/modules/eventloop/) module
* GC pressure is low because HTTP connections are managed in a pool and recyclable ByteBufs are used to wrap HTTP messages.
* Supports requests with streaming request body (using CSP).
* Contains a DNS client that can be used to cache results of DNS queries.
* AsyncServlet represents a functional interface which allows simple custom servlet creation without overhead.
* RFC conformity.

## Examples

1. [HTTP Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpServerExample.java)
2. [HTTP Multithreaded Server Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpMultithreadedServerExample.java)
3. [HTTP Client Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/HttpClientExample.java)
4. [Middleware Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/MiddlewareServletExample.java)
5. [Request Parametr Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/RequestParametrExample.java)
6. [Static Servlet Example](https://github.com/softindex/datakernel/blob/master/examples/http/src/main/java/io/datakernel/examples/StaticServletExample.java)

To run the examples, you should first execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/http
$ mvn clean compile exec:java@HttpServerExample
$ # OR
$ mvn clean compile exec:java@HttpMultithreadedServerExample
$ # OR
$ mvn clean compile exec:java@MiddlewareServletExample
$ # OR
$ mvn clean compile exec:java@RequestParametrExample
$ # OR
$ mvn clean compile exec:java@StaticServletExample
{% endhighlight %}

The difference between multithreaded HTTP server and simple HTTP server is that the first one creates several worker threades for requests processing. In the example above your server will have 4 workers.

To check how HTTP Server or HTTP Multithreaded Server works, you can start your client:
{% highlight bash %}
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.HttpClientExample
{% endhighlight %}

If you connected to the multithreaded server, you'll receive a message representing which worker processed your request:
{% highlight bash %} 
"Hello from worker server #..." 
{% endhighlight %}
Otherwise, you'll see a message: {% highlight bash %}"Hello World!"{% endhighlight %} 

If you run Examples 4-6, you can connect to you server by visiting [this link](http://localhost:8080/) in your browser.

## Benchmark

We have compared our single-threaded HTTP server performance with [Nginx](http://nginx.org) (default settings) using [ApacheBench tool](http://httpd.apache.org/docs/2.4/programs/ab.html).

Results along with the parameters and some additional info are presented in the table below.

<table>
  <tr>
    <th rowspan="2">ApacheBench parameters</th>
    <th rowspan="2">Method</th>
    <th rowspan="2">Additional info</th>
    <th colspan="2">Requests per second</th>
  </tr>
  <tr>
    <th>Nginx</th>
    <th>DataKernel HTTP</th>
  </tr>
  <tr>
    <td>default</td>
    <td rowspan="3">GET</td>
    <td rowspan="3">Response body size: 820 bytes</td>
    <td>26,592</td>
    <td>25,270</td>
  </tr>
  <tr>
    <td>-k</td>
    <td>41,183</td>
    <td>84,163</td>
  </tr>
  <tr>
    <td>-c 100 -k</td>
    <td>116,917</td>
    <td>238,393</td>
  </tr>
  <tr>
    <td>-c 100</td>
    <td>POST</td>
    <td>Request/response body size: 16 bytes</td>
    <td>53,816</td>
    <td>59,258</td>
  </tr>
</table>
