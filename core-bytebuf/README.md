## ByteBuf

ByteBuf module provides memory-efficient, recyclable byte buffers.

* Represents a wrapper over a byte buffer with separate read and write indices.
* Has a pooling support for better memory efficiency.
* `ByteBufQueue` class provides effective management of multiple ByteBufs.
* Utility classes manage resizing of underlying byte buffer, `String` conversions, etc.

`ByteBuf` is a wrapper over byte arrays which has teo positions: `readPosition` and `writePosition`. When you write data 
to your ByteBuf, its `writePosition` increases by amount of bytes written. Similarly, when you read data from your ByteBuf,
its `readPosition` increases by amount of bytes read. You can read bytes from ByteBuf only when `writePosition` is greater 
then `readPosition`. Also, you can write bytes to ByteBuf until `writePosition` doesn't exceed the length of the wrapped 
array.

You can wrap your array into `ByteBuf` in the following ways:

* ByteBuf.empty() - returns an empty ByteBuf with length, readPosition and writePosition all equal 0.

* ByteBuf.wrapForWriting(byte[] bytes) - Wraps provided byte array into ByteBuf with writePosition equal to 0.

* ByteBuf.wrapForReading(byte[] bytes) - wraps provided byte array into ByteBuf with writePosition equal to length of 
provided array.

* ByteBuf.wrap(byte[] bytes, int readPosition, int writePosition) - returns a new ByteBuf with custom readPosition and 
writePosition. These parameters define from which element new data can be written or read. For example, if you set 
writePosition as `bytes.length`, you'll create a ByteBuf from which bytes can only be read.

ByteBuf can also be used to wrap other data types:
* ByteBufString.wrapAscii(String string)
* ByteBufString.wrapUTF8(String string)
* ByteBufString.wrapInt(int value)
* ByteBufString.wrapLong(long value)

If you want to create a ByteBuf from scratch, you should use `ByteBufPool.allocate(int size)`. This method either creates 
a new ByteBuf or returns one from the pool.

The core methods of `ByteBuf` are:

* *readPosition() / readPosition(int pos)* - gets/sets index of the ByteBuf from which bytes can be read.

* *writePosition() / writePosition(int pos)* - gets/sets index of the ByteBuf from which bytes can be written.

* *limit()* - returns length of the ByteBuf.

* *writeRemaining()* - returns amount of writable bytes.

* *readRemaining()* - returns amount of readable bytes.

* *peek() / peek(int offset)* - returns the first readable byte in the ByteBuf / the first readable byte considering offset.

* *drainTo(ByteBuf buf, int length) / drainTo(byte[] array, int offset, int length)* - drains ByteBuf to another ByteBuf 
or byte array, returns the number of elements to be drained.

* *set(int index, byte b)* - sets byte b at the particular index of ByteBuf.

* *put(byte b) / put(ByteBuf buf) / put(byte[] bytes) / put(byte[] bytes, int offset, int length)* - puts given data in 
the ByteBuf.

* *find(byte b)* - searches for a particular byte between readPosition and writePosition indexes of the ByteBuf. If 
successful, returns index of the first match, otherwise returns -1.

* *getArray()* - returns a byte array created from the ByteBuf from readPosition to writePosition.

* *asArray()* - returns a byte array created from the ByteBuf from readPosition to writePosition and recycles the ByteBuf.

* *readByte() / readBoolean() / readChar() / readDouble() / readFloat() / readInt() / readLong() / readShort() / 
readString()* - allows to read primitives and Strings from the ByteBuf. Returns the value of appropriate data type from 
current readPosition and increases readPosition by the amount of used by the data type bytes.

* *writeByte(byte v) / writeBoolean(boolean v) / writeChar(char v) / writeDouble(double v) / writeFloat(float v) / 
writeInt(int v) / writeLong(long v) / writeShort(short v) / writeString(String s)* - allows to write primitives and Strings 
to the ByteBuf. Writes the value to the current writePosition and then increases it by the amount of used by the data type 
bytes.

* *slice() / slice (int length) / slice (int offset, int length)* - returns a new SliceByteBuf which is a slice of your 
ByteBuf. By default, length is the number of bytes between readPosition and writePosition, offset is readPosition.

* *recycle()* - recycles your ByteBuf by returning it to ByteBufPool.

### You can explore ByteBuf examples [here](https://github.com/softindex/datakernel/tree/master/examples/bytebuf)
