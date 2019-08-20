---
id: codec
filename: codec
title: Codec Module
prev: core/codegen.html
next: core/uikernel.html
nav-menu: core
layout: core
description: Fast and efficient codec for encoding and decoding custom objects of different complexity - POJOs, collections, JSONs etc.
keywords: codec,decode,encode,java,java codec,custom codec,json codec,java framework
---

Codec module allows to encode and decode custom objects in a fast and efficient way.

You can add Codec module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-codec</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %} 

**CodecRegistry** allows to easily access pre-defined codecs based on your custom data types. Use *create()* method to 
create your **CodecRegistry**, and *with()* to setup the codec.

**StructuredCodecs** contains various implementations of **StructuredCodec**. **StructuredCodec** extends 
**StructuredEncoder** and **StructureDecoder** interfaces. It wraps classes, lists, maps and other data structures for 
encoding/decoding. 

Yet, you can create custom codecs. There are several ways to do so:

1.  **CodecRegistry** has method *get*, which returns a new **StructuredCodec**. So, you can first adjust your **CodecRegistry** 
and then use it for this purpose.
2. There are lots of predefined methods which return **StructuredCodec**.
	
## Examples
* [CodeStructuredBinaryExample](#structured-binary-example)
* [CodeStructuredCollectionExample](#structured-collection-example)
* [CodecStructuredJsonExample](#structured-json-example)

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project. 
<br> These examples are located at <b>datakernel -> examples -> core -> codec.</b>" %}


All these examples utilize two utility classes. The first one is a simple POJO **Person** with three values *id*, *name*, 
*dateOfBirth*. The second one is **Registry**, which is needed for encoding/decoding:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/codec/src/main/java/util/Registry.java tag:EXAMPLE %}
{% endhighlight %}

Let's now proceed to the examples.

#### **Structured Binary Example**
In this example we encode **Person** John to **ByteBuf** and then decode him back to **Person**. To perform such 
operations, we first need to create a **StructuredCodec** and then use it for *encode* and *decode* operations.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/codec/src/main/java/CodecStructuredBinaryExample.java tag:EXAMPLE %}
{% endhighlight %}

##### [See this example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/codec/src/main/java/CodecStructuredBinaryExample.java)

#### **Structured JSON Example**
This example represents how to encode/decode objects to/from JSONs:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/codec/src/main/java/CodecStructuredJsonExample.java tag:EXAMPLE %}
{% endhighlight %}

[See this example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/codec/src/main/java/CodecStructuredJsonExample.java)

#### **Structured Collection Example**
In this example we perform encode/decode operations on List an Map of two **Person** objects to/from JSONs:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/codec/src/main/java/CodecStructuredCollectionsExample.java tag:EXAMPLE %}
{% endhighlight %}

[See this example on GitHub](https://github.com/softindex/datakernel/blob/master/examples/core/codec/src/main/java/CodecStructuredCollectionsExample.java)