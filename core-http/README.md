## HTTP

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

### You can explore HTTP examples [here](https://github.com/softindex/datakernel/tree/master/examples/http)

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
