<p align="center">
  <a href="https://datakernel.io" target="_blank">
    <img alt="DataKernel Logo" src="http://datakernel.io/static/images/logo-icon.png" width="409">
  </a>
</p>

## Introduction

DataKernel is a full-featured alternative Java framework, created from ground up for **efficient** and **scalable** solutions. Was inspired by Node.js.

## Features

- 🌞 Fully **asynchronous** modular framework; **exceptionally fast and simple** Dependency Injection.
- 🚀 Magnificently **fast build** and **start-up times** of your applications with **extremely small** JAR sizes.
- 📗 A wide selection of application launchers with **embedded servers**.
- 🔥 Contains full set of **elegant** data structures and components with outstanding **performance**.
- 🏎 Supports **HTTP, TCP, UDP** protocols; **microservice** architecture with **15M requests per second** per CPU core. 
- 💥 Low entry barrier; archetypes for HTTP and RPC applications scaffolding with **minimal configuration**.

## Getting started

Just insert this snippet to your terminal...

```
mvn archetype:generate \
        -DarchetypeGroupId=io.datakernel                  \
        -DarchetypeArtifactId=datakernel-http-archetype   \
        -DarchetypeVersion=3.0.0-SNAPSHOT                 \
        -DgroupId=org.example                             \
        -DartifactId=dkapp                                \
        -DmainClassName=MyFirstDkApp 
```

... and open project in your favourite IDE. Then, you can build and run the application and open browser on [localhost:8080](http://localhost:8080). 

To learn more about DataKernel, visit [**datakernel.io**](https://datakernel.io) or follow 5-minute getting-started [guide](https://datakernel.io/docs/core/tutorials/getting-started). 

## Why DataKernel?

**Best technologies**  
DataKernel is legacy-free. Build application-specific embedded databases and high-performance HTTP/RPC servers using high-level abstractions, LSM-Tree, Operational Transformations, CRDT, Go-inspired CSP and other modern algorithms and technologies.

**Explicit design**  
There are no under-the-hood magic, endless XML configurations and dependency hell of third-party components glued together via layers of abstractions. DataKernel gives a full control over your applications.

**Born to be async**  
DataKernel allows you to create async web applications in a Node.js manner while preserving all of the Java advantages. We also use Node.js-inspired features, such as single-threaded async Promises and pool of event loops as the building blocks of our framework.

**No overweight**  
To achieve the lowest GC footprint possible, we’ve designed thoroughly optimized core modules - improved Java ByteBuffer ByteBuf, minimalistic Datastreams, stateless single-threaded Promises and also one of the fastest Serializers available nowadays.

**Easy-to-use and flexible**  
DataKernel has everything you need to create applications of different scales - from standalone high-performance async network solutions and HTTP web applications up to big-data cloud solutions and decentralized internet-wide applications.

**Modern approach**  
DataKernel has simple yet powerful set of abstractions with clean OOP design favoring Java 8+ functional programming style. It also radically downplays Dependency Injection role, giving way to your business logic instead.

## DataKernel structure

DataKernel consists of three modules:
 * [Core](https://datakernel.io/docs/core/) - building blocks of the framework and everything you need to create **asynchronous web applications**.
 * [Cloud](https://datakernel.io/docs/cloud/) - components for **decentralized cloud solutions** of different complexity.
 * [Global Cloud](https://datakernel.io/docs/global-cloud/) (coming soon) - components for **ultimately scalable**, decentralized, yet practical and high-performance **cloud solutions**.

## License
Apache License 2.0