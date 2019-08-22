---
id: promise
filename: promise
title: Promise Module
prev: core/eventloop.html
next: core/bytebuf.html
nav-menu: core
layout: core
toc: true
description: Node.js-inspired Promise for asynchronous Java programming. Alternative to Java Futures - 7 times faster and has more convenient API.
keywords: promise,nodejs,node js,java,async programming,asynchronous,java framework,java futures,completablefuture,completionstage,java promise
---
## Features

Promises are primary building blocks in DataKernel async programming model, and can be compared to Java Futures 
(CompletionStage to be more exact).

*If you are not familiar with the Promises concept, the following paragraph is for you.
Otherwise you can skip this part and move directly to the [next](#creating-promises) section.*

## Promises Basics
* In general, **Promise** represents the result of an operation that hasn't completed yet,
but will at some undetermined point of time in the future. It is used for deferred and asynchronous computations.
* **Promise** is a high-performance Java Future alternative. It not only represents a future result
of an asynchronous computation, but also allows to transform and process the unspecified yet result using chaining
mechanism. Moreover, such results can be combined with the help of the provided combinators.
* Unlike Java Future, **Promises** were naturally designed to work within single eventloop thread.
They are extremely [lightweight](#optimization-features) and have 
no multithreading overhead and thus capable of processing millions of calls per second.

### Creating Promises
Using DataKernel, we can primary manage **Promise**s with the basic methods:
* *of(T value)* - creates successfully completed promise, like *CompletableFuture.completedFuture()*.
* *ofException()* - creates an exceptionally completed promise.
* *complete()* - creates successfully completed Promise&lt;Void>, a shortcut to *Promise.of(null)*.
{% highlight java %}
Promise<Integer> firstNumber = Promise.of(10);
Promise.of("Hello World");
Promise.ofException(new Exception("Something went wrong"));
{% endhighlight %}

### Chaining Promises
Promise will succeed or fail at some unspecified time and you need to chain methods that will be executed in both cases:
* *then()* - returns a new Promise which, when this Promise completes successfully, is executed with this Promise
 as an argument, like *CompletionStage.thenCompose()*.
* *map()* - returns a new Promise which, when this Promise completes successfully, is executed with its result as an argument, like CompletionStage.thenApply().
* *whenResult()* - subscribes to execute given action after this Promise completes successfully,
like *CompletionStage.thenAccept()*.

In addition, to handle errors the following methods are provided:
* *thenEx()* - returns a new Promise which is executed with the Promise result as the argument when Promise completes either successfully or with an exception.
* *whenException()* - subscribe to execute given action after this Promise completes exceptionally and returns a new Promise.

When we have multiple asynchronous calls, we need to execute them in order.
So we can just chain methods together to create a sequence.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseChainExample.java tag:REGION_1%}
{% endhighlight %}

[See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseChainExample.java).
### Combine Promises
There are cases when you need to execute some Promises and combine their results. For this purpose, consider the following methods:
* *combine()* - returns a new Promise that, when both Promises are completed, is executed with the two results as arguments.
* *all()* - returns a Promise that completes when all of the provided promises are completed.
* *any()* - returns one of the first completed Promises.

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseAdvanceExample.java tag:REGION_1%}
{% endhighlight %}

[See full example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseAdvanceExample.java).

* *delay()* - delays completion of provided Promise for the defined period of time.
{% highlight java %}
Promise<String> strPromise = Promises.delay("result", Duration.seconds(10))
{% endhighlight %}

## Optimization Features

DataKernel Promises are heavily GC-optimized:
 * internal representation of typical Promise consists of 1-2 objects with 
bare minimum of fields inside
* after fulfilling, the result is passed to their subscribers and discarded afterwards

In order to optimise **Promise**s, there are several implementations of **Promise** interface:

{% mermaid %}
graph TD
Promise --> AbstractPromise
Promise --> CompleteExceptionallyPromise
Promise --> CompletePromise
AbstractPromise --> NextPromise
AbstractPromise --> SettablePromise
CompletePromise --> CompleteResultPromise
CompletePromise --> CompleteNullPromise
{% endmermaid %}

* `Promise` - root interface which represents *promises* behaviour.
* `SettablePromise` - a class which can be used as a root for chain of **Promise**s. Allows to wrap operations in **Promise**s, 
can be completed manually even before actual completion.
* `AbstractPromise`, `NextPromise` - helper classes which enable creating chains of stateless *Promises*. You can treat 
these chains as pipes which pass values through, but don't store them. 
* `CompletePromise` - an abstract class which represents a successfully completed **Promise**.
* `CompleteExceptionallyPromise`, `CompleteResultPromise`, `CompleteNullPromise` - helper classes.

You can add Promise module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-promise</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

## Benchmarks 
We've compared **DataKernel Promise** to **Java CompletableFuture** in different scenarios:
1. **DataKernel** Promise/**Java** CompletableFuture executes operations with one promise/future.
2. **DataKernel** Promise/**Java** CompletableFuture combines several promises/futures.

We used JMH as the benchmark tool and run benchmarks in *AverageTime* mode.
All the measurements are represented in nanoseconds.

{% highlight plaintext %}
DataKernel Promise oneCallMeasure 
Cnt: 10; Score: 12.952; Error: ± 0.693; Units: ns/op;

DataKernel Promise combineMeasure 
Cnt: 10; Score: 34.112; Error: ± 1.869; Units: ns/op;

Java CompletableFuture oneCallMeasure 
Cnt: 10; Score: 85.151; Error: ± 1.781; Units: ns/op;

Java CompletableFuture combineMeasure
Cnt: 10; Score: 153.645; Error: ± 4.491; Units: ns/op;
{% endhighlight %}

**You can find benchmark sources on [GitHub](https://github.com/softindex/datakernel/tree/master/examples/benchmarks/src/main/java/io/datakernel/promise).**

## Examples 

* [Promise Chain Example](#promisechainexample) 
* [Promise Advanced Example](#promiseadvanceexample) 
* [Promises Example](#promisesexamples) 
* [Async File Example](#asyncfileserviceexample) 

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project.
<br> These examples are located at <b>datakernel -> examples -> core -> promise</b>" %}

### PromiseChainExample
You can create chains of **Promise**s even before they are completed and you don't know yet if they will complete 
successfully or with an exception. In this example we have a *doSomeProcess* which returns a **Promise** that has equal 
chances to complete either successfully or with an exception. So we create a chain which will handle both cases:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseChainExample.java tag:EXAMPLE %}
{% endhighlight %}

If you run the example, you will receive either this output (if *doSomeProcess* finishes successfully):
{% highlight bash %}
Loaded data is 'Hello World'
Result of some process is 'Hello World'
The mapped result is 'hello world'
{% endhighlight %}

Or this, if it finishes with an exception:

{% highlight bash %}
Loaded data is 'Hello World'
Exception after some process is 'Something went wrong'
Something went wrong
{% endhighlight %}

Note that the first line is 
{% highlight bash %}
Loaded data is 'Hello World'
{% endhighlight %}
This is due to the 1 second delay we set up in *doSomeProcess*.

### PromiseAdvanceExample 
You can combine several **Promise**s, for example:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseAdvanceExample.java tag:REGION_1 %}
{% endhighlight %}

There are also several ways to delay **Promise**:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromiseAdvanceExample.java tag:REGION_2 %}
{% endhighlight %}

### PromisesExamples
**Promises** is a helper class which allows to efficiently manage multiple **Promise**s. This example will demonstrate 
three use cases. 

1.In the following example we use the **Promises** *loop*, which resembles Java *for* loop, but has async capabilities, 
which are provided by **Promise**:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromisesExample.java tag:REGION_2 %}
{% endhighlight %}

The output is:

{% highlight bash %}
Looping with condition:
This is iteration #1
This is iteration #2
This is iteration #3
This is iteration #4
This is iteration #5
{% endhighlight %} 


2.Another example creates a list of **Promise**s using **Promises** *toList* method:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromisesExample.java tag:REGION_3 %}
{% endhighlight %}

The  output is:
{% highlight bash %}
Collecting group of **Promises** to list of **Promises**' results:
Size of collected list: 6
List: [1, 2, 3, 4, 5, 6]
{% endhighlight %}

3.In the last example **Promises** *toArray* method is utilized, which reduces *promises* to array of provided data type (in this case, *Integers*):
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromisesExample.java tag:REGION_4 %}
{% endhighlight %}

And the final output is:
{% highlight bash %}
Collecting group of **Promises** to array of **Promises**' results:
Size of collected array: 6
Array: [1, 2, 3, 4, 5, 6]
{% endhighlight %}

 [**See full example on GitHub**](https://github.com/softindex/datakernel/blob/master/examples/core/promise/src/main/java/PromisesExample.java).

### AsyncFileServiceExample
When you run this example, you'll receive the following output, which represents content of the created file:

{% highlight bash %}
Hello
This is test file
This is the 3rd line in file
{% endhighlight %}

In this example Promise's **AsyncFile** (represents a file with asynchronous capabilities) is utilized, along with 
several methods associated with the class, such as:
* *open()* - opens file synchronously.
* *write()* - writes all bytes of provided **ByteBuf** into file asynchronously.
* *read()* - reads all bytes from file into a **ByteBuf** asynchronously.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/promise/src/main/java/AsyncFileServiceExample.java tag:REGION_1 %}
{% endhighlight %}

[**See full example on GitHub**](https://github.com/softindex/datakernel/blob/master/examples/core/promise/src/main/java/AsyncFileServiceExample.java)
