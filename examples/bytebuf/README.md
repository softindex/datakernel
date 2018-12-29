1. [ByteBuf Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufExample) - represents some basics of ByteBuf possiblities, such as: 
    * wrapping data in ByteBuf for writing/reading, 
    * slicing particular parts out of data,
    * conversions.
2. [ByteBuf Pool Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufPoolExample) - pools in ByteBuf and their behaviour.
3. [ByteBuf Queue Example](https://github.com/softindex/datakernel/tree/master/examples/bytebuf/src/main/java/io/datakernel/examples/ByteBufQueueExample) - shows how queues of ByteBufs are created and processed.

To run the examples, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/bytebuf
$ mvn clean compile exec:java@ByteBufExample
$ #or
$ mvn clean compile exec:java@ByteBufPoolExample
$ #or
$ mvn clean compile exec:java@ByteBufQueueExample
```

If you run the ByteBuf example, you'll receive the following output:

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

* The first six lines are result of wrapping byte array to ByteBuf wrapper for reading and then printing it.
* The line [0, 1, 2, 3, 4, 5] is a result of converting bytes to ByteBuf and wrapping them for writing.
* "Hello" line was first converted to ByteBuf and wrapped for reading and then represented as a String for output.
* The last two outputs represent some other possibilities of ByteBuf.

If you run the ByteBuf pool example, you'll receive the following output:
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
To understand how this example works from within, let's consider its code:
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

		// Now pool consists of 1 ByteBuf that is the one we just recycled
		System.out.println("Number of ByteBufs in pool after recycling: " + ByteBufPool.getPoolItems());

		// Trying to allocate another ByteBuf
		ByteBuf anotherByteBuf = ByteBufPool.allocate(123);

		// Pool is now empty as the only ByteBuf in pool has just been taken from the pool
		System.out.println("Number of ByteBufs in pool: " + ByteBufPool.getPoolItems());
		System.out.println();
	}

	private static void ensuringWriteRemaining() {
		ByteBuf byteBuf = ByteBufPool.allocate(3);

		// Size is equal to power of 2 that is larger than 3, hence 4
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

If you run the third ByteBuf queue example, you'll receive the following output:
```
bufs:2 bytes:7

Buf taken from queue: [0, 1, 2, 3]

Buf taken from queue: [3, 4, 5, 6, 7, 8]

[1, 2, 3, 4]
[5, 6, 7, 8]
Queue is empty? true
```
The first line represents our queue after we added two bufs: `[0, 1, 2, 3]` and `[3, 4, 5]`.
Then method QUEUE.take() is applied and the first added buf is taken from the queue which is `[0, 1, 2, 3]`.
The next line represents the consequence of two operations: adding a new `[6, 7, 8]` buf and then applying 
QUEUE.takeRemaining() which takes all remaining bufs from the queue.
Finally, the last three lines represent the following operations:

* Creating two bufs: `[1, 2, 3, 4]` and `[5, 6, 7, 8]`;
* Draining the queue to consumer which prints the bufs;
* Then we check if the queue is empty now.