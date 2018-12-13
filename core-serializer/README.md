Serializers module is a bytecode generator of extremely fast and space efficient serializers, which are used for 
transferring data over wire or persisting it into a file system or database.

* Schema-less approach - for maximum performance and compactness (unlike other serializers, there is no overhead 
in typed values)
* Implemented using runtime bytecode generation, to be compatible with dynamically created classes (like intermediate 
POJOs created with [Codegen](/docs/modules/codegen) module)

A common usage for a serializer is to pass some serialized class instances through the network to 
remote machines for further processing. This approach is used in RPC, Dataflow and LSMT-Database modules. Serialization 
process can be configured via annotations.

## Examples

1. [Simple Object Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/SimpleObjectSerializationExample.java) - serialization and deserialization of a simple object.
2. [Complex Object Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/ComplexObjectSerializationExample.java) - serialization and deserialization of a more complex object, which contains nullable fields, map, list and a two-dimensional array.
3. [Fixed Size Fields Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/FixedSizeFieldsSerializationExample.java) -
4. [Generics & Interfaces Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/GenericsAndInterfacesSerializationExample.java) -

To run the example, you should execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/serializer
$ mvn clean compile exec:java@SimpleObjectSerializationExample
$ # OR
$ mvn clean compile exec:java@ComplexObjectSerializationExample
$ # OR
$ mvn clean compile exec:java@FixedSizeFieldsSerializationExample
$ # OR
$ mvn clean compile exec:java@GenericsAndInterfacesSerializationExample
{% endhighlight %}

If you run the first example, you'll get the following output:
{% highlight bash %}
10 10
abc abc
20 20
30 30
40 40
123 123
{% endhighlight %}
* The first column represents values of a test object 1, while the second one shows values of test object 2, which was created with the help of serialization and then deserialization of the test object 1.
* The same logic for output of Complex Object Serialization and Generics and Interfaces Serialization examples.

If you run Fixed Size Fields Serialization Example, you'll get the following output:
{% highlight bash %}
[abc, null, 123, superfluous] [abc, null, 123]
[1, 2, 3, 4] [1, 2, 3, 4]
{% endhighlight %}
Since SerializeFixedSize for String array was set at value 3, "superfluous" was removed while serialization.

## Benchmark

We have conducted a benchmark of our serializers using the methodology and data structures described [here](https://github.com/eishay/jvm-serializers).

Specifically, the test called SIMPLE/SPECIFIC has been run (described [here](https://github.com/eishay/jvm-serializers/wiki)).

Results are presented in the table below.

Table column names meaning:

* create - time required to set up objects (specifically, to copy the objects)
* ser - serialization time
* deser - deserialization time
* total - serialization + deserialization time
* size - the size of the serialized data
* +dfl - the size of the serialized data compressed with Java\'s built-in implementation of DEFLATE (zlib)

Time is in nanoseconds, size is in bytes.

<table>
  <tr>
    <th></th>
    <th>create</th>
    <th>ser</th>
    <th>deser</th>
    <th>total</th>
    <th>size</th>
    <th>+dfl</th>
  </tr>
  <tr>
  <td>datakernel serializer</td>
  <td>272</td>
  <td>1335</td>
  <td>1590</td>
  <td>2924</td>
  <td>216</td>
  <td>131</td>
  </tr>
  <tr>
    <td>protostuff</td>
    <td>305</td>
    <td>1798</td>
    <td>2624</td>
    <td>4422</td>
    <td>239</td>
    <td>150</td>
  </tr>
  <tr>
    <td>kryo-manual</td>
    <td>219</td>
    <td>2063</td>
    <td>2723</td>
    <td>4786</td>
    <td>211</td>
    <td>131</td>
  </tr>
  <tr>
    <td>protobuf/protostuff</td>
    <td>311</td>
    <td>2098</td>
    <td>2979</td>
    <td>5077</td>
    <td>239</td>
    <td>149</td>
  </tr>
  <tr>
    <td>protostuff-manual</td>
    <td>260</td>
    <td>2007</td>
    <td>3438</td>
    <td>5445</td>
    <td>239</td>
    <td>150</td>
  </tr>
  <tr>
    <td>wobly</td>
    <td>135</td>
    <td>3382</td>
    <td>2261</td>
    <td>5643</td>
    <td>251</td>
    <td>151</td>
  </tr>
  <tr>
    <td>protobuf/protostuff-runtime</td>
    <td>216</td>
    <td>2631</td>
    <td>3134</td>
    <td>5765</td>
    <td>241</td>
    <td>150</td>
  </tr>
  <tr>
    <td>protostuff-graph</td>
    <td>316</td>
    <td>2811</td>
    <td>3075</td>
    <td>5885</td>
    <td>239</td>
    <td>150</td>
  </tr>
  <tr>
    <td>kryo-opt</td>
    <td>212</td>
    <td>2725</td>
    <td>3165</td>
    <td>5890</td>
    <td>209</td>
    <td>129</td>
  </tr>
  <tr>
    <td>kryo-flat-pre</td>
    <td>215</td>
    <td>2703</td>
    <td>3209</td>
    <td>5912</td>
    <td>212</td>
    <td>132</td>
  </tr>
</table>
