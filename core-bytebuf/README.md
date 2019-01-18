## ByteBuf

ByteBuf module provides memory-efficient, recyclable byte buffers. The main components of the module are:

### ByteBuf 
An extremely light-weight and efficient implementation compared to the Java NIO ByteBuffer. There are no *direct buffers* 
which simplifies and improves `ByteBuf` performance. 

ByteBuf is similar to a FIFO byte queue and has two positions: `readPosition` and `writePosition`. When you write data to your 
ByteBuf, its `writePosition` increases by amount of bytes written. Similarly, when you read data from your ByteBuf,
its `readPosition` increases by amount of bytes read. You can read bytes from ByteBuf only when `writePosition` is greater 
then `readPosition`. Also, you can write bytes to ByteBuf until `writePosition` doesn't exceed the length of the wrapped 
array. In this way, there is no need for `ByteBuffer.flip()` operations. 

ByteBuf supports concurrent processes: while some data is written to the `ByteBuf` by one process, another one can 
read it. ByteBuf also has `slice()` operation and inner ref counts.

### ByteBufPool
Allows to reuse ByteBufs, and as a result reduces Java Garbage Collector load. To make utilizing of ByteBufPool more 
convenient, there are debugging and monitoring tools for allocated ByteBufs, including their stack traces.

To return ByteBuf to the ByteBufPool, use `ByteBuf.recycle()`. This recycle is recommended but not required - if you 
forget to do so, you will only give Garbage Collector a little more work to do. 

### ByteBufQueue
`ByteBufQueue` class provides effective management of multiple ByteBufs. It creates an optimized queue of several 
ByteBufs with FIFO rules. You can simply manage your queue with the following methods:
* *takeRemaining()* - creates and returns a new ByteBuf which contains all remaining bytes from the ByteBufQueue.
* *takeExactSize()* - creates and returns a new ByteBuf which contains an exact amount of bytes if ByteBufQueue first 
ByteBuf has enough bytes. Otherwise returns a ByteBuf of exact size which contains all bytes from the ByteBufQueue.
* *takeAtLeast(int size)* - creates and returns ByteBufSlice that contains all bytes from queue's first ByteBuf
if latter contains more bytes than `size`. Otherwise creates a new ByteBuf of the `size`.
* *takeAtMost(int size)* - creates and returns ByteBufSlice that contains needed amount of bytes from queue's first ByteBuf
if latter contains enough bytes. Otherwise creates and returns ByteBuf that contains all bytes from first ByteBuf in queue.

The module also contains utility classes to manage resizing of underlying byte buffer, `String` conversions, etc.

You can wrap your byte array into `ByteBuf` in the following ways:

* *ByteBuf.empty()* - returns an empty ByteBuf with length, readPosition and writePosition all equal 0.
* *ByteBuf.wrapForWriting(byte[] bytes)* - wraps provided byte array into `ByteBuf` with `writePosition` equal to 0.
* *ByteBuf.wrapForReading(byte[] bytes)* - wraps provided byte array into `ByteBuf` with `writePosition` equal to the 
length of provided array.
* *ByteBuf.wrap(byte[] bytes, int readPosition, int writePosition)* - returns a new `ByteBuf` with custom `readPosition` 
and `writePosition`. These parameters define from which element new data can be written or read. For example, if you set 
`writePosition` as `bytes.length`, you'll create a `ByteBuf` from which bytes can only be read.

`ByteBuf` can also be used to wrap other data types:
* *ByteBufString.wrapAscii(String string)*
* *ByteBufString.wrapUTF8(String string)*
* *ByteBufString.wrapInt(int value)*
* *ByteBufString.wrapLong(long value)*

If you want to create a `ByteBuf` from scratch, you should use `ByteBufPool.allocate(int size)`. This method either creates 
a new `ByteBuf` or returns one from the pool.

The core methods of `ByteBuf` are:

* *readPosition() / readPosition(int pos)* - gets/sets index of the `ByteBuf` from which bytes can be read.
* *writePosition() / writePosition(int pos)* - gets/sets index of the `ByteBuf` from which bytes can be written.
* *limit()* - returns length of the `ByteBuf`.
* *drainTo(ByteBuf buf, int length)* - drains `ByteBuf` to another `ByteBuf`, returns the number of elements to be drained.
* *set(int index, byte b)* - sets byte `b` at the particular index of `ByteBuf`.
* *put(byte b)* - puts given data in the `ByteBuf`.
* *getArray()* - returns a byte array created from the `ByteBuf` from `readPosition` to `writePosition`.
* *asArray()* - returns a byte array created from the `ByteBuf` from `readPosition` to `writePosition` and **recycles** the 
`ByteBuf`.
* *readByte() / readBoolean() / readChar() / readDouble() / readFloat() / readInt() / readLong() / readShort() / 
readString()* - allows to read primitives and Strings from the `ByteBuf`. Returns the value of appropriate data type from 
current `readPosition` and increases `readPosition` by the amount of used by the data type bytes.
* *writeByte(byte v) / writeBoolean(boolean v) / writeChar(char v) / writeDouble(double v) / writeFloat(float v) / 
writeInt(int v) / writeLong(long v) / writeShort(short v) / writeString(String s)* - allows to write primitives and Strings 
to the `ByteBuf`. Writes the value to the current `writePosition` and then increases it by the amount of used by the data 
type bytes.
* *slice() / slice (int length) / slice (int offset, int length)* - returns a new `SliceByteBuf` which is a slice of your 
`ByteBuf`. By default, length is the number of bytes between `readPosition` and `writePosition`, offset is `readPosition`.
* *recycle()* - recycles your `ByteBuf` by returning it to `ByteBufPool`.

### You can explore ByteBuf examples [here](https://github.com/softindex/datakernel/tree/master/examples/bytebuf)
