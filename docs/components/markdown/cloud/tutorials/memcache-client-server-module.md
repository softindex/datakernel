---
id: memcache-client-server-module
filename: tutorials/memcache-client-server-module
title: Memcache Client/Server Module
nav-menu: cloud
layout: cloud
keywords: java,java framework,tutorial,guide,memcache,rpc,client-server,web application
description: Create a memcache-like client-server application with RPC communication protocol using DataKernel.
---
## Introduction
In this tutorial we will create memcache client-server application that is based on RPC communication protocol and predefined
DataKernel modules.

**You can find full example sources on [GitHub](https://github.com/softindex/datakernel/tree/master/examples/cloud/rpc/src/main/java)**.

### Memcache Client and Server Modules
First of all, consider the initial DataKernel implementation of these modules, because our application will be built with 
their help:

* [MemcacheServerModule](https://github.com/softindex/datakernel/blob/master/cloud-memcache/src/main/java/io/datakernel/memcache/server/MemcacheServerModule.java)
exports a **RpcServer** that is basically able to handle *get* and *put* requests.
* [MemcacheClientModule](https://github.com/softindex/datakernel/blob/master/cloud-memcache/src/main/java/io/datakernel/memcache/client/MemcacheClientModule.java)
sets a [Rendezvous Hashing Strategy](/docs/cloud/rpc.html#rendezvous-hashing-strategy) for requests arrangement between shards of servers.

Note, this implementation covers the basic usage. You may add more features as your application requires.
 
### Create Client and Server
Now, let's write own server:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/cloud/rpc/src/main/java/MemcacheLikeServer.java tag:REGION_1 %}
{% endhighlight %}

* As for the memcache functionality - we specify the number of buffers and their capacity in the config.
* Pass with config everything that **MemcacheServerModule** needs to handle the upcoming requests.
* Then make RPC `memcacheServer` listen on the `8080` port.

Our client will create *put* and *get* requests:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/cloud/rpc/src/main/java/MemcacheLikeClient.java tag:REGION_1 %}
{% endhighlight %}

* Since **MemcacheClientModule** uses Rendezvous Hashing Strategy to achieve agreement for requests between 
shards of servers, in client we ought to provide addresses of these shards - `9010`, `9020`, `9030` ports.
* In eventloop we ask to *put* a message in the current *i* of the `bytes[i]` array, 
and *get* it back from the corresponding cell.
* So client will perform these operations asynchronously for three shards, that's why as the result we will receive
 three disordered output blocks.

## Running the application
* Launch server - run **MemcacheLikeServer** *main()* method.
* Create and run compound configuration.
For IntelliJ IDEA:
    * Add three configurations for each address `Run -> Edit configurations -> 
Add Application -> Run/Debug Configurations -> Main class:  MemcacheLikeServer -> VM options: -ea -Dconfig.server.listenAddresses=localhost:9010`.
Changing port number correspondingly.
    * Create compound configuration `Run -> Edit configurations -> Add Compound` and add three configurations from previous step.
    * Run compound configuration.
* Launch client - run **MemcacheLikeClient** *main()* method.

