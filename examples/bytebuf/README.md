1. [ByteBuf Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufExample.java) - 
represents some basic ByteBuf possibilities, such as: 
    * wrapping data in ByteBuf for writing/reading, 
    * slicing particular parts out of data,
    * conversions.
    
    [Launch](#bytebuf-example)
2. [ByteBuf Pool Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufPoolExample.java) - 
represents how to work with ByteBufPool. [Launch](#bytebuf-pool-example)
3. [ByteBuf Queue Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufQueueExample.java) - 
shows how queues of ByteBufs are created and processed. [Launch](#bytebuf-queue-example)

### ByteBuf Example
#### Launch 
To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/bytebuf
$ mvn exec:java@ByteBufExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ByteBufExample` class, which is located at **datakernel -> examples -> bytebuf** and run its *main()* method.

#### Explanation

When you run the example, you'll receive the following output:

```
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
```

* The first six lines are result of wrapping byte array to ByteBuf wrapper for reading and then printing it:
```java
byte[] data = new byte[]{0, 1, 2, 3, 4, 5};
ByteBuf byteBuf = ByteBuf.wrapForReading(data);
```

* The line `[0, 1, 2, 3, 4, 5]` is a result of converting an empty array of bytes to ByteBuf and wrapping them for 
writing. Then the ByteBuf was filled with bytes with the help of `while` loop:
```java
byte[] data = new byte[6];
ByteBuf byteBuf = ByteBuf.wrapForWriting(data);
byte value = 0;
while (byteBuf.canWrite()) {
	byteBuf.writeByte(value++);
}
```

* "Hello" line was first converted from String to ByteBuf and wrapped for reading, then represented as a String for 
output with the help of `byteBuf.asString()`:
```java
String message = "Hello";
ByteBuf byteBuf = ByteBuf.wrapForReading(message.getBytes(UTF_8));
String unWrappedMessage = byteBuf.asString(UTF_8);
```

* The last two outputs represent some other possibilities of ByteBuf, such as slicing:
```java
byte[] data = new byte[]{0, 1, 2, 3, 4, 5};
ByteBuf byteBuf = ByteBuf.wrap(data, 0, data.length);
//the first parameter is an offset and the second is length of slice
ByteBuf slice = byteBuf.slice(1, 3);
```
and conversions of default ByteBuffer to ByteBuf:
```java
//creating a ByteBuf instance. 
//the first parameter is the bytes to be wrapped, the second and the third are read and write positions
ByteBuf byteBuf = ByteBuf.wrap(new byte[20], 0, 0);
//Creating a ByteBuffer instance
ByteBuffer buffer = byteBuf.toWriteByteBuffer();
buffer.put((byte) 1);
buffer.put((byte) 2);
buffer.put((byte) 3);
//converting ByteBuffer to ByteBuf
byteBuf.ofWriteByteBuffer(buffer);
```

### ByteBuf Pool Example
#### Launch 
To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/bytebuf
$ mvn exec:java@ByteBufPoolExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ByteBufPoolExample` class, which is located at **datakernel -> examples -> bytebuf** and run its *main()* method.

#### Explanation
When you run the example, you'll receive the following output:
```
Length of array of allocated ByteBuf: 128
Number of ByteBufs in pool before recycling: 0
Number of ByteBufs in pool after recycling: 1
Number of ByteBufs in pool: 0

Size of ByteBuf: 4
Remaining bytes of ByteBuf after 3 bytes have been written: 1
Remaining bytes of a new ByteBuf: 5

[0, 1, 2, 3, 4, 5]
```
Let's have a look at the implementation:
```java
public class ByteBufPoolExample {
	/* Setting ByteBufPool minSize and maxSize properties here for illustrative purposes.
	 Otherwise, ByteBufs with size less than 32 would not be placed into pool
	 */
	static {
		System.setProperty("ByteBufPool.minSize", "1");
	}

	private static void allocatingBufs() {
		// Allocating a ByteBuf of 100 bytes
		ByteBuf byteBuf = ByteBufPool.allocate(100);

		// Allocated ByteBuf has an array with size equal to next power of 2, hence 128
		System.out.println("Length of array of allocated ByteBuf: " + byteBuf.writeRemaining());

		// Pool has 0 ByteBufs right now
		System.out.println("Number of ByteBufs in pool before recycling: " + ByteBufPool.getPoolItems());

		// Recycling ByteBuf to put it back to pool
		byteBuf.recycle();

		// Now pool consists of 1 ByteBuf which is the one we've just recycled
		System.out.println("Number of ByteBufs in pool after recycling: " + ByteBufPool.getPoolItems());

		// Trying to allocate another ByteBuf
		ByteBuf anotherByteBuf = ByteBufPool.allocate(123);

		// Pool is now empty as the only ByteBuf in pool has just been taken from the pool
		System.out.println("Number of ByteBufs in pool: " + ByteBufPool.getPoolItems());
		System.out.println();
	}

	private static void ensuringWriteRemaining() {
		ByteBuf byteBuf = ByteBufPool.allocate(3);

		// Size is equal to the power of 2 that is larger than 3, hence 4
		System.out.println("Size of ByteBuf: " + byteBuf.writeRemaining());

		byteBuf.write(new byte[]{0, 1, 2});

		// After writing 3 bytes into ByteBuf we have only 1 spare byte in ByteBuf
		System.out.println("Remaining bytes of ByteBuf after 3 bytes have been written: " + byteBuf.writeRemaining());

		// We need to write 3 more bytes so we have to ensure that there are 3 spare bytes in ByteBuf
		// and if there are not - create new ByteBuf with enough room for 3 bytes (old ByteBuf will get recycled)
		ByteBuf newByteBuf = ByteBufPool.ensureWriteRemaining(byteBuf, 3);

		// As we need to write 3 more bytes, we need a ByteBuf that can hold 6 bytes.
		// The next power of 2 is 8, so considering 3 bytes that have already been written, new ByteBuf
		// can store (8-3=5) more bytes
		System.out.println("Remaining bytes of a new ByteBuf: " + newByteBuf.writeRemaining());

		// Recycling a new ByteBuf (remember, the old one has already been recycled)
		newByteBuf.recycle();
		System.out.println();
	}

	private static void appendingBufs() {
		ByteBuf bufOne = ByteBuf.wrapForReading(new byte[]{0, 1, 2});
		ByteBuf bufTwo = ByteBuf.wrapForReading(new byte[]{3, 4, 5});

		ByteBuf appendedBuf = ByteBufPool.append(bufOne, bufTwo);

		// Appended ByteBuf consists of two ByteBufs, you don't have to worry about allocating ByteBuf
		// with enough capacity or how to properly copy bytes, ByteBufPool will handle it for you
		System.out.println(Arrays.toString(appendedBuf.asArray()));
		System.out.println();
	}

	public static void main(String[] args) {
		allocatingBufs();
		ensuringWriteRemaining();
		appendingBufs();
	}
}
```

### ByteBuf Queue Example
#### Launch 
To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/bytebuf
$ mvn exec:java@ByteBufQueueExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ByteBufQueueExample` class, which is located at **datakernel -> examples -> bytebuf** and run its *main()* method.

#### Explanation
When you run the example, you'll receive the following output:
```
bufs:2 bytes:7

Buf taken from queue: [0, 1, 2, 3]

Buf taken from queue: [3, 4, 5, 6, 7, 8]

[1, 2, 3, 4]
[5, 6, 7, 8]
Queue is empty? true
```
The first line represents our queue after we added two bufs: `[0, 1, 2, 3]` and `[3, 4, 5]` with `QUEUE.add()` method.
Then method `QUEUE.take()` is applied and the first added buf, which is `[0, 1, 2, 3]`, is taken from the queue.
The next line represents the consequence of two operations: adding a new `[6, 7, 8]` buf and then applying 
`QUEUE.takeRemaining()` which takes all remaining bufs from the queue.

Finally, the last three lines represent the following operations:

* Creating two bufs: `[1, 2, 3, 4]` and `[5, 6, 7, 8]`.
* Draining the queue to consumer which prints the bufs.
* Then we check if the queue is empty now.