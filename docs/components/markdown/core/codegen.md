---
id: codegen
filename: codegen
title: Codegen Module
prev: core/serializer.html
next: core/codec.html
nav-menu: core
layout: core
description: Codegen is a dynamic bytecode generator with minimal I/O overhead and convenient API.
keywords: codegen,bytecode generator,runtime code generator,runtime query processing,java,java framework
---
Codegen module allows to build classes and methods in runtime without the overhead of reflection.


* Dynamically creates classes needed for runtime query processing (storing the results of computation, intermediate 
tuples, compound keys etc.)
* Implements basic relational algebra operations for individual items: aggregate functions, projections, predicates, 
ordering, group-by etc.
* Since I/O overhead is already minimal due to Eventloop module, bytecode generation ensures 
that business logic (such as innermost loops processing millions of items) is also as fast as possible.
* Easy to use API that encapsulates most of the complexity involved in working with bytecode.

You can add Codegen module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-codegen</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

## Examples 
* [CodegenExpressionsExample](#codegen-expressions-example)
* [DynamicClassCreationExample](#dynamic-class-creation-example)

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project." %}

#### **Codegen Expressions Example**
An example of implementation of *sayHello()* method:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/codegen/src/main/java/CodegenExpressionsExample.java tag:EXAMPLE %}
{% endhighlight %}

#### **Dynamic Class Creation Example**
In this example a class, which implements **Person**, is dynamically created: 
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/codegen/src/main/java/DynamicClassCreationExample.java tag:REGION_1 %}
{% endhighlight %}