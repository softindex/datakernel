[![Maven Central](https://img.shields.io/maven-central/v/io.datakernel/datakernel)](https://mvnrepository.com/artifact/io.datakernel)
[![Apache license](https://img.shields.io/badge/license-apache2-green.svg)](https://github.com/softindex/datakernel/blob/master/LICENSE)
[![Twitter](https://img.shields.io/badge/twitter-%40datakernel__io-38A1F3.svg)](https://twitter.com/datakernel_io)

## Project status
Please note that DataKernel framework was **massively improved and restructured** into a new project named **[ActiveJ](https://github.com/activej/activej)**. 
Now it's even more streamlined, convenient, and powerful! We highly recommend you to migrate to **ActiveJ**. 

## Introduction

DataKernel is a full-featured alternative web and big data Java framework built from the ground up. It does not use Netty, Jetty, Spring/Guice DI, RxJava, etc. Instead, it features a full application stack: Event Loop, Promises, HTTP, DI, and others, including decentralized big-data technologies and map-reduce algorithms.

No overhead of intermediate abstractions, legacy standards and third-party libraries makes the framework minimalistic, streamlined and lightning-fast!

## Getting started

Just insert this snippet to your terminal...

```
mvn archetype:generate -DarchetypeGroupId=io.datakernel -DarchetypeArtifactId=archetype-http -DarchetypeVersion=3.1.0
```

... and open project in your favourite IDE. Then, build the application and run it. Open your browser on [localhost:8080](http://localhost:8080) to see the "Hello World" message. 

To learn more about DataKernel, visit [**datakernel.io**](https://datakernel.io) or follow our 5-minute getting-started [guide](https://datakernel.io/docs/core/tutorials/getting-started). 

## Examples

### Basic HTTP server in less than 15 lines of code:
```java
public final class HelloWorldExample { 
    public static void main(String[] args) throws IOException {
        Eventloop eventloop = Eventloop.create();
        AsyncHttpServer server = AsyncHttpServer.create(eventloop,
                request -> Promise.of(
                        HttpResponse.ok200()
                                .withPlainText("Hello, World!")))
                .withListenPort(8080);
        server.listen();
        eventloop.run();
    }
}
```
`AsyncHttpServer` is a built-in implementation of an HTTP server that runs asynchronously in a Node.js-inspired Event Loop.

- *`AsyncHttpServer` is up to 20% faster than [multithreaded Vert.x server](https://github.com/networknt/microservices-framework-benchmark/tree/master/vertx), with 1/2 of CPU usage, on a single core!*

### Full-featured embedded web application server with Dependency Injection:
```java
public final class HttpHelloWorldExample extends HttpServerLauncher { 
    @Provides
    AsyncServlet servlet() { 
        return request -> HttpResponse.ok200().withPlainText("Hello, World!");
    }

    public static void main(String[] args) throws Exception {
        Launcher launcher = new HttpHelloWorldExample();
        launcher.launch(args); 
    }
}
```
`HttpServerLauncher` - a predefined DataKernel [Launcher](https://datakernel.io/docs/core/launcher.html) that takes care of the application lifecycle and provides needed components for our server

`@Provides` - one of the [DataKernel DI](https://datakernel.io/docs/core/di.html) annotations

`AsyncServlet` - asynchronous servlet interface

`Promise` - Node.js-inspired async single-threaded Promises, an alternative to `CompletableFuture`

- *The JAR file size of this example is only 1.4 MB. In comparison, minimal Spring web app size is 17 MB*
- *This example utilizes quite a few components - [Eventloop](https://datakernel.io/docs/core/eventloop.html), [DI](https://datakernel.io/docs/core/di.html), [Promise](https://datakernel.io/docs/core/promise.html), [HTTP](https://datakernel.io/docs/core/http.html), [Launcher](https://datakernel.io/docs/core/launcher.html). Yet, it builds and starts in 0.65 sec.*
- *DataKernel [DI](https://datakernel.io/docs/core/di.html) is 5.5 times faster than Guice and 100s times faster than Spring.*
- *DataKernel [Promise](https://datakernel.io/docs/core/promise.html) is 7 times faster than Java `CompletableFuture`.*

### Lightning-fast RPC server:
```java
public RpcServer rpcServer(Eventloop eventloop) {
    return RpcServer.create(eventloop)
            .withStreamProtocol(...)
            .withMessageTypes(Integer.class)
            .withHandler(Integer.class, Integer.class, req -> Promise.of(req * 2));
}
```
- *This RPC server handles up to [15M requests](https://datakernel.io/docs/cloud/rpc.html#benchmarks) per second on a single CPU core*.

## Documentation
See the docs, examples, and tutorials on [our website](https://datakernel.io).

## Need help or found a bug?
Feel free to open a [GitHub issue](https://github.com/softindex/datakernel/issues).

## Communication
* Twitter: [@datakernel_io](https://twitter.com/datakernel_io)
