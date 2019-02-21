## Examples overview

### Table of contents
1. [Purpose of the examples](#purpose-of-the-examples)
2. [Basic modules](#basic-modules)
    * Bytebuf
    * Promise 
    * Eventloop
    * Codegen
    * Serializer
    * Codec
    * CSP
    * Datastream
    * Boot
3. [Simple web applications](#simple-web-applications)
    * HTTP
        * HTTP "Hello World"
    * Launchers (HTTP examples)
    * UIKernel integration
4. [Advanced web applications](#advanced-web-applications)
    * Net
    * RPC
        * Remote key-value storage
    * CRDT
    * Launchers (advanced example)
5. [Global applications](#global-web-applications) 
    * Global-OT editor
    * Global-OT demo
    * Global-OT chat
    * Global-FS demo
    * Global-FS CLI
    * Global-DB demo

### Purpose of the examples
These examples aim to represent core DataKernel technologies and show how they can be used for both common and advanced 
use cases. 

If you'd like to learn more about the basic modules of DataKernel (Promise, ByteBuf etc.), please go to 
[Basic modules](#basic-modules) section. With these examples you'll understand how DataKernel's building blocks work
from within and the core principles of their efficient asynchronous performance. 

If you are interested in developing web applications, you can start with [Simple web applications](#simple-web-applications) 
section, which shows how to utilize core components' modules for this purpose. Then you may continue with 
[Advanced web applications](#advanced-web-applications) examples.

If you are up to more complicated web applications, there are [Global applications](#global-web-applications) examples - 
see how to utilize Global components for developing scalable web applications.

Please note, that to run any of the examples in your console, you should first clone the project from Github:

```
$ git clone https://github.com/softindex/datakernel.git
```

And then install DataKernel locally:
```
$ cd datakernel
$ mvn clean install -DskipTests
```
These commands are repeated in each of the examples' instructions just to make sure that everything will work correctly. 
Yet if you've entered the commands once, you can omit these three lines from now on. 

To run the examples in an IDE, you need to clone DataKernel, import Maven projects and then properly set up working 
directory. 
For IntelliJ IDEA: `Run -> Edit configurations -> [Run/Debug Configurations -> [Templates -> Application] -> [Working directory -> 
$MODULE_WORKING_DIR$]]`.

### Basic modules
If you haven't checked out [Hello World](https://github.com/softindex/datakernel/tree/master/examples/getting-started) 
getting-started example yet, you can start with it. This simple 5-minutes tutorial represents how to create a basic 
application in DataKernel-like way.

You may continue with some basic modules, most of them contain several simple examples:
* [ByteBuf examples](https://github.com/softindex/datakernel/tree/master/examples/bytebuf) - ByteBuf provides efficient 
and recyclable byte buffers. The examples will show you how to create and utilize them for different purposes.

* [Promise examples](https://github.com/softindex/datakernel/tree/master/examples/promise) - Promises were inspired by 
Node.js and allow to efficiently handle asynchronous operations. In the examples you'll see some basic functionality of 
Promises along with utilizing them for working with files.

To provide efficient working with objects, their bytecode generation, serialization, encoding or decoding, there are 
several corresponding modules:
* [Codegen examples](https://github.com/softindex/datakernel/tree/master/examples/codegen) - see how to set up dynamic 
generation of classes and methods in runtime.

* [Serializer examples](https://github.com/softindex/datakernel/tree/master/examples/serializer) - Serializer allows to 
serialize and deserialize objects extremely fast. In the examples you can learn how to use Serializer for objects of 
different complexity.

* [Codec example](https://github.com/softindex/datakernel/tree/master/examples/codec) - create efficient 
custom codecs with Codec module.

DataKernel provides efficient communications between suppliers and consumers (for example, client and server) with 
special channels and streams: 
* [CSP examples](https://github.com/softindex/datakernel/tree/master/examples/csp) - the examples demonstrate how to 
create channels, ByteBufs parsers and handle Communication Sequential Processes.

* [Datastream examples](https://github.com/softindex/datakernel/tree/master/examples/datastreams) - includes 6 examples 
of simple datastreams use cases. Pay attention to `NetworkDemoServer` and `NetworkDemoClients` examples - they illustrate 
how streams can be combined with channels.

Also, 
* [Boot examples](https://github.com/softindex/datakernel/tree/master/examples/boot) - see how to boot your projects and 
work with configs with Boot module. 

* [Launchers example - Hello World](https://github.com/softindex/datakernel/tree/master/examples/launchers#hello-world) - 
demonstrates one of the simplest launcher's usages.


### Simple web applications
* [HTTP examples](https://github.com/softindex/datakernel/tree/master/examples/http) - several examples of simple web 
applications, ranging from basic HTTP client and server to multithreaded example.
    * Also check out [HTTP Hello World](https://github.com/softindex/datakernel/tree/master/examples/http-helloworld) - 
    a detailed tutorial on how to create a simple but scalable HTTP server with multiple Worker Servers. This example 
    also demonstrates the core principles of single-threaded Eventloop module.
    
* [Launchers and HTTP examples](https://github.com/softindex/datakernel/tree/master/examples/launchers#http) - these 
examples will show you how to use launchers while developing web applications.

* [UIKernel example](https://github.com/softindex/datakernel/tree/master/examples/codegen) - see how to integrate JS front-end 
with DataKernel.


### Advanced web applications
* [Net examples](https://github.com/softindex/datakernel/tree/master/examples/net) - show how to create TCP echo servers 
and clients from scratch in a few steps.

* [Eventloop examples](https://github.com/softindex/datakernel/tree/master/examples/eventloop) - even though Eventloop 
is one of the basic modules of DataKernel and is **not** bound to web applications, these examples demonstrate how 
eventloops can be utilized with Net module for developing servers.

* [RPC example](https://github.com/softindex/datakernel/tree/master/examples/rpc) - a simple example of utilizing remote 
procedure call module.
    * [Remote key-value storage](https://github.com/softindex/datakernel/tree/master/examples/remote-key-value-storage) - 
    with this detailed guide you can create a remote key-value storage with basic operations "put" and "get" utilizing 
    RPC and Boot modules.

* [CRDT example](https://github.com/softindex/datakernel/tree/master/examples/crdt) - demonstrates how CRDT (conflict-free 
replicated data type) algorithms manage merging two replicas with conflicting states.


### Global web applications
* [Global-OT editor](https://github.com/softindex/datakernel/tree/master/examples/global-ot-editor)
* [Global-OT demo](https://github.com/softindex/datakernel/tree/master/examples/global-ot-demo)
* [Global-OT chat](https://github.com/softindex/datakernel/tree/master/examples/global-ot-chat)
* [Remote-FS](https://github.com/softindex/datakernel/tree/master/examples/remotefs)
* [Global-FS demo](https://github.com/softindex/datakernel/tree/master/examples/global-fs-demo)
* [Global-FS CLI](https://github.com/softindex/datakernel/tree/master/examples/global-fs-cli)
* [Global-DB demo](https://github.com/softindex/datakernel/tree/master/examples/global-db-demo)

