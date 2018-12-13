ByteBuf module provides memory-efficient, recyclable byte buffers.

* Represents a wrapper over a byte buffer with separate read and write indices.
* Has a pooling support for better memory efficiency.
* `ByteBufQueue` class provides effective management of multiple ByteBufs.
* Utility classes manage resizing of underlying byte buffer, `String` conversions, etc.

## Examples
1. [ByteBuf Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufExample) - represents some basics of ByteBuf possiblities, such as: 
    * wrapping data in ByteBuf for writing/reading, 
    * slicing particular parts out of data,
    * conversions.
2. [ByteBuf Pool Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufPoolExample) - pools in ByteBuf and their behaviour.
3. [ByteBuf Queue Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufQueueExample) - shows how queues of ByteBufs are created and processed.

To run the examples, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/bytebuf
$ mvn clean compile exec:java@ByteBufExample
$ #or
$ mvn clean compile exec:java@ByteBufPoolExample
$ #or
$ mvn clean compile exec:java@ByteBufQueueExample
{% endhighlight %}

If you run the ByteBuf example, you'll receive the following output:
{% highlight bash %}
0
1
2
3
4
5

[0, 1, 2, 3, 4, 5]

Hello

Sliced byteBuf array: [1, 2, 3]

Array of ByteBuf converted from ByteBuffer: [1, 2, 3]
{% endhighlight %}

* The first six lines are result of wrapping byte array to ByteBufwrapper for reading and then printing it.
* The line [0, 1, 2, 3, 4, 5] is a result of converting bytes toByteBuf and wrapping them for writing.
* "Hello" line was first converted to ByteBuf and wrapped for readingand then represented as a String for output.
* The last two outputs represent some other possibilities of ByteBuf.