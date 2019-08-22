---
id: remote-key-value-storage
filename: tutorials/remote-key-value-storage
title: Remote Key-Value Storage
nav-menu: cloud
layout: cloud
keywords: java,java framework,tutorial,guide,rpc,client-server,web application,key-value storage
description: Create a key-value storage with RPC communication protocol.
---
## Purpose
In this guide we will create a remote key-value storage using [RPC module](/docs/cloud/rpc.html). 
App will have 2 basic operations: `put` and `get` and use RPC as a communication protocol.

## Introduction
When writing distributed application the common concern is what protocol to use for communication. There are two main 
options:

* HTTP/REST
* RPC

While HTTP is more popular and well-specified, it has some overhead. When performance is a significant aspect of application, 
you should use something faster than HTTP. And for this purpose DataKernel framework has an RPC module which is based on 
fast serializers and custom optimized communication protocol, which allows to significantly improve application performance.

## What you will need:

* About 20 minutes
* IDE or terminal
* JDK 1.8+
* Maven 3.0+

## To proceed with this guide you have 2 options:

* Download and run [working example](#working-example)
* Follow [step-by-step guide](#step-by-step-guide)

## Working Example

To run the example in IDE, [clone DataKernel](https://github.com/softindex/datakernel.git) locally first:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
{% endhighlight %} 

And import it as a Maven project.

                                                                
Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then, go to [testing](#testing) section.

## Step-by-step guide
### 1. Set up the project
First, create a folder for application and build an appropriate project structure:

{% highlight bash %}
remote-key-value-storage
└── pom.xml
└── src
    └── main
        └── java
            └── GetRequest.java
            └── GetResponse.java
            └── PutRequest.java
            └── PutResponse.java
            └── KeyValueStore.java
            └── ServerModule.java
            └── ServerLauncher.java
            └── ClientModule.java
            └── ClientLauncher.java
{% endhighlight %}


Next, configure your **pom.xml** file [like this](https://github.com/softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/pom.xml)

### 2. Define basic app functionality
Since we have two basic operations to implement (`put` and `get`), let's first write down classes that will be used for 
communication between client and server. Specifically, **PutRequest**, **PutResponse**, **GetRequest** and **GetResponse**. 
Instances of these classes will be serialized using fast DataKernel Serializer, which requires some meta information 
about this classes, provided with appropriate annotations. The basic rules are:

* Use `@Serialize` annotation with order number on getter of property. Ordering provides better compatibility in case 
classes are changed.
* Use `@Deserialize` annotation with property name (which should be same as in getter) in constructor.
* Use `@SerializeNullable` on properties that can have null values.

Thereby, classes for communication should look like following:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/PutRequest.java tag:EXAMPLE %}
{% endhighlight %}

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/PutResponse.java tag:EXAMPLE %}
{% endhighlight %}

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/GetRequest.java tag:EXAMPLE %}
{% endhighlight %}

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/GetResponse.java tag:EXAMPLE %}
{% endhighlight %}

Next, let's write a simple implementation of key-value storage:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/KeyValueStore.java tag:EXAMPLE %}
{% endhighlight %}

### 3. Create client and server 

Now, let's write down an AbstractModule for RPC server using DataKernel Boot, that will handle "get" and "put" requests 


{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/ServerModule.java tag:EXAMPLE %}
{% endhighlight %}


As you can see, in order to properly create `RpcServer` we should indicate all the classes which will be sent between 
client and server, and specify appropriate `RequestHandler` for each request class.

Since Java 1.8 they can be expressed as lambdas, so they are represented as third arguments in these lines.

{% highlight java %}
.withHandler(PutRequest.class, PutResponse.class, req -> Promise.of(new PutResponse(store.put(req.getKey(), req.getValue()))))
.withHandler(GetRequest.class, GetResponse.class, req -> Promise.of(new GetResponse(store.get(req.getKey()))))
{% endhighlight %}

Next, create a launcher for RPC server:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/ServerLauncher.java tag:EXAMPLE %}
{% endhighlight %}

Now, let's write RPC client. In order to create RPC client we should again indicate all of the classes that will be used 
for communication and specify `RpcStrategy`. There is a whole bunch of strategies in RPC module (such as single-server, 
first-available, round-robin, sharding and so on). The nice thing about them is that all strategies can be combined. For 
example, if you want to dispatch requests between 2 shards, and each shard actually contains main and reserve servers, 
you can easily tell RPC client to dispatch request in a proper way using the following code:

{% highlight java %}
RpcStrategy strategy = sharding(hashFunction,
    firstAvailable(shard_1_main_server, shard_1_reserve_server),
    firstAvailable(shard_2_main_server, shard_2_reserve_server)
);
{% endhighlight %}

But since we have only one server, we will just use single-server strategy:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/ClientModule.java tag:EXAMPLE %}
{% endhighlight %}

Let's also build `ClientLauncher`. In *run()* we will consider command line arguments and make appropriate requests
to `RpcServer`.

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/rpc-kv-storage/src/main/java/ClientLauncher.java tag:EXAMPLE %}
{% endhighlight %}

As you can see, *sendRequest()* method returns a `CompletionStage`, at which we could listen for its results asynchronously 
with lambdas.

Congratulation! We've finished writing code for this app.

## Testing

**First, launch server.**

Open `ServerLauncher` class and run its *main()* method.

**Then make a "put" request.**

Open `ClientLauncher` class, which is located at **datakernel -> examples -> remote-key-value-storage**
and set up program arguments to `--put key1 value1`. For IntelliJ IDEA: `Run -> Edit configurations -> 
|Run/Debug Configurations -> |Program arguments -> --put key1 value1||`. Then run launcher's *main()* method.

You will see the following output:

{% highlight bash %}
put request was made successfully
previous value: null
{% endhighlight %}

**Finally, make a "get" request.**

Open `ClientLauncher` class again, and set up program arguments to `--get key1`. For IntelliJ IDEA: `Run ->
Edit configurations -> |Run/Debug Configurations -> |Program arguments -> --get key1||`. Then run *main()* method of the 
client launcher.

You will see the following output:

{% highlight bash %}
get request was made successfully
value: value1
{% endhighlight %}

Congratulations, you've just created a remote key-value storage with RPC communication protocol!
