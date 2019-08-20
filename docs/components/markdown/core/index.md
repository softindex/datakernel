---
is-index: true
nav-menu: core
layout: core
id: core
filename: core/
title: Core components of DataKernel Framework
status: STABLE
description: Core components of DataKernel framework. Fast asynchronous technologies, Node.js-inspired Promises and Eventloops, launchers, tools for smart application booting.
keywords: dependency injection,di,launcher,eventloop,promise,node js,java,java framework,nodejs java,uikernel,bytebuffer,serializer,code generator,codec,net,datastream,csp,asynchronous,java async,spring alternative,netty alternative,jetty alternative
---

These components form the basis of DataKernel framework. They include fast asynchronous technologies, Node.js-inspired Promises 
and Eventloops, launchers for common use cases, tools for smart and efficient application booting. Also, there is UIKernel 
integration component, which allows to integrate UIKernel JS front-end into your application.

If you want to create Java **web application** using DataKernel, we suggest you to learn docs in the following order:


| Component | Description |
| --- | --- | --- |
| **[DI](di.html)**| Custom Dependency Injection, an extremely lightweight yet simple and powerful solution. |
| **[Launcher](launcher.html)**| Takes care of full application lifecycle and also provides handy diagnostic. |
| **[Service Graph](service-graph.html)**| Starts/ends services according to their dependency graph, designed to be used with DI and Launcher. |
| **[Worker Pool](workerpool.html)**| Allows to create multithreaded applications without overhead and complexities of traditional multithreading programming. |
| **[Configs](configs.html)**| Useful component which provides configs to your applications in a handy way. |
| **[HTTP](http.html)** | High-performance asynchronous HTTP client and server. Contains built-in servlets for request dispatching and static content loading. |
| **[Eventloop](eventloop.html)** | Resembles Node.js Event Loop and provides efficient non-blocking network and file I/O for building Node.js-like client/server applications with high performance requirements. |
| **[Promise](promise.html)** | Resembles JavaScript Promise, alternative to Java's `CompletionStage`. Allows to organize asynchronous code in convenient way by creating chains of operations wrapped in `Promise`s. `Promise` has an extremely fast single-threaded implementation with minimal overhead and memory consumption. |
| **[ByteBuf](bytebuf.html)** | More lightweight and efficient version of Java's `ByteBuffer` class with support of simultaneous input and output. Module's `ByteBufPool` significantly reduces memory consumption and GC footprint by reusing `ByteBuf`s. |

The following docs cover the components which aren't always required when creating web applications. Yet, they can be 
useful in more specific use cases:

| Component | Description |
| --- | --- | --- |
| **[Datastream](datastream.html)** | Composable asynchronous/reactive streams with powerful data processing capabilities. Useful for transferring high volumes of lightweight values. |
| **[CSP](csp.html)** | Stands for 'Communicating Sequential Processes', provides asynchronous sequential communication between processes via channels similarly to the Go language. |
| **[Net](net.html)** | Handles low-level asynchronous socket I/O (TCP/UDP) based on ByteBuf. Provides ability to create servers that utilize Eventloop and support SSL. |
| **[Serializer](serializer.html)** | Extremely fast and space-efficient serializers created with bytecode engineering. Introduces schema-less approach for best performance. |
| **[Codegen](codegen.html)** | Dynamic class and method bytecode generator on top of ObjectWeb ASM. An expression-based fluent API abstracts the complexity of direct bytecode manipulation, so you can use it to create custom classes on the fly. |
| **[Codec](codec.html)** | Contains tools for encoding/decoding primitives and objects with built-in support of conversion to/from JSON and ByteBuf. The process resembles serialization and can be used to convert objects to a custom representation. |
| **[UIKernel](uikernel.html)**| Integration with UIKernel JS frontend library: JSON serializers, grid model, basic servlets. With this module you can build a server application that will be compatible with UIKernel.|