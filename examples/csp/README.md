1. [Byte Bufs Parser Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ByteBufsParserExample.java) - 
A simple example of processing bytes utilizing CSP and ByteBuf modules.
2. [Channel Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ChannelExample.java) - 
Basic interactions between CSP `ChannelSupplier` and `ChannelConsumers`.
3. [Channel File Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ChannelFileExample.java) - 
Represents working with files utilizing CSP `ChannelFileReader` and `ChannelFileWriter`.
4. [Communicating Process Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/CommunicatingProcessExample.java) - 
Represents communication between ChannelSupplier and `ChannelConsumer` utilizing transformation and Promise features.

To run the examples, you should enter these commands in your console:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/csp
$ mvn clean compile exec:java@ByteBufsParserExample
$ # or
$ mvn clean compile exec:java@ChannelExample
$ # or
$ mvn clean compile exec:java@ChannelFileExample
$ # or
$ mvn clean compile exec:java@CommunicatingProcessExample
```


In Byte Bufs Parser Example the following DataKernel modules are utilized:
* CSP
* ByteBuf
 
 Let's review the code:
 ```java
public class ByteBufsParserExample {
	public static void main(String[] args) {
		//creating a list of wrapped as ByteBuf "Hello" message
		List<ByteBuf> list = asList(wrapAscii("H"), wrapAscii("e"), wrapAscii("l"), wrapAscii("l"), wrapAscii("o"));
		//creating a parser which checks the amount of bytes in message
		ByteBufsParser<String> parser = bufs -> {
			if (!bufs.hasRemainingBytes(5)) {
				System.out.println("Not enough bytes to parse message");
				return null;
			}
			return bufs.takeExactSize(5).asString(UTF_8);
		};
		//BinaryChannelSupplier processes our "Hello" message. When parser receives a result, it is printed.
		BinaryChannelSupplier.of(ChannelSupplier.ofIterable(list)).parse(parser)
				.whenResult(System.out::println);
	}
}
```

In Channel Example the following DataKernel modules are utilized:
* CSP
* Eventloop

The main methods of the example are:
```java
private static void supplierOfValues() {
	//passing Supplier five Strings which are streamed to Consumer and then printed in accordance to 
	//ofConsumer() setup.
	//ChannelSupplier.of() defines what data will be passed and .streamTo() to which ChannelConsumer
	//ChannelConsumer.ofConsumer() defines consumer behaviour when it receives data
	ChannelSupplier.of("1", "2", "3", "4", "5")
			.streamTo(ChannelConsumer.ofConsumer(System.out::println));
}
        
private static void supplierOfList(List<String> list) {
	//ofIterable() allows supplier to pass values from list to consumer one by one.
	ChannelSupplier.ofIterable(list)
			.streamTo(ChannelConsumer.ofConsumer(System.out::println));
}

private static void map() {
	//transforms Integers to needed format and then passes modified values to consumer one by one
	ChannelSupplier.of(1, 2, 3, 4, 5)
			.map(integer -> integer + " times 10 = " + integer * 10)
			.streamTo(ChannelConsumer.ofConsumer(System.out::println));
}

private static void toCollector() {
	ChannelSupplier.of(1, 2, 3, 4, 5)
			.toCollector(Collectors.toList())
			.whenResult(System.out::println);
}

private static void filter() {
	//filter() allows you to filter data which passes to your ChannelConsumer
	ChannelSupplier.of(1, 2, 3, 4, 5, 6)
			.filter(integer -> integer % 2 == 0)
			.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}
```

Channel File Example demonstrates how to work with files in asynchronous approach.

Communicating Process Example Represents an AsyncProcess between suppliers and consumers.
