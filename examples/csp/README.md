1. [ByteBufs Parser Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ByteBufsParserExample.java) - 
a simple example of processing bytes utilizing CSP and ByteBuf modules. [Launch](#1-bytebufs-parser-example)
2. [Channel Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ChannelExample.java) - 
basic interactions between CSP `ChannelSupplier` and `ChannelConsumers`. [Launch](#2-channel-example)
3. [Channel File Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ChannelFileExample.java) - 
represents working with files utilizing CSP `ChannelFileReader` and `ChannelFileWriter`. [Launch](#3-channel-file-example)
4. [Communicating Process Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/CommunicatingProcessExample.java) - 
represents communication between `ChannelSupplier` and `ChannelConsumer` utilizing `AbstractCommunicatingProcess`. [Launch](#4-communicating-process-example)

### 1. ByteBufs Parser Example
#### Launch
To run the example in console, you should enter these commands in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/csp
$ mvn exec:java@ByteBufsParserExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ByteBufsParserExample` class, which is located at **datakernel -> examples -> csp** and run its *main()* method.

#### Explanation 

This example shows how to process bytes with CSP and ByteBuf modules and produces a `Hello` message.

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
		//BinaryChannelSupplier processes our "Hello" message. When parsing completes, its result is printed out
		BinaryChannelSupplier.of(ChannelSupplier.ofIterable(list)).parse(parser)
				.accept(System.out::println);
	}
}
```

### 2. Channel Example
#### Launch
To run the example in console, you should enter these commands in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/csp
$ mvn exec:java@ChannelExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ChannelExample` class, which is located at **datakernel -> examples -> csp** and run its *main()* method.

#### Explanation
**Channel Example** shows interaction between suppliers and consumers:
```java
private static void supplierOfValues() {
	
	//ChannelSupplier.of() defines what data will be provided and .streamTo() - to what ChannelConsumer
	//ChannelConsumer.ofConsumer() defines consumer behaviour when it receives data
	//streamTo() streams all supplier's data to consumer
	ChannelSupplier.of("1", "2", "3", "4", "5")
			.streamTo(ChannelConsumer.ofConsumer(System.out::println));
}
        
private static void supplierOfList(List<String> list) {
	
	//ofIterable() allows to wrap an Iterable in ChannelSupplier and stream it to consumer.
	ChannelSupplier.ofIterable(list)
			.streamTo(ChannelConsumer.ofConsumer(System.out::println));
}

private static void map() {
	
	//transforms Integers to needed format and then passes modified values to consumer
	ChannelSupplier.of(1, 2, 3, 4, 5)
			.map(integer -> integer + " times 10 = " + integer * 10)
			.streamTo(ChannelConsumer.ofConsumer(System.out::println));
}

private static void toCollector() {
	ChannelSupplier.of(1, 2, 3, 4, 5)
	
	        //collects the provided Integers to List
			.toCollector(Collectors.toList())
			.accept(System.out::println);
}

private static void filter() {
	
	//filter() allows to filter supplier's data
	ChannelSupplier.of(1, 2, 3, 4, 5, 6)
		.filter(integer -> integer % 2 == 0)
	    .streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}
```
Thus, if you run this example, you'll receive the following output:
```
1
2
3
4
5
One
Two
Three
1 times 10 = 10
2 times 10 = 20
3 times 10 = 30
4 times 10 = 40
5 times 10 = 50
[1, 2, 3, 4, 5]
2
4
6
```

### 3. Channel File Example  
#### Launch
To run the example in console, you should enter these commands in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/csp
$ mvn exec:java@ChannelFileExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ChannelFileExample` class, which is located at **datakernel -> examples -> csp** and run its *main()* method.

#### Explanation

**Channel File Example** demonstrates how to work with files with asynchronous approach using Promises and CSP built-in 
consumers and suppliers. This example writes two lines to the file with `ChannelFileWriter`, and then reads and prints 
them out utilizing `ChannelFileReader`. If you run the example, you'll see the content of the created file:
```
Hello, this is example file
This is the second line of file
```

### 4. Communicating Process Example
#### Launch
To run the example in console, you should enter these commands in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/csp
$ mvn exec:java@CommunicatingProcessExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `CommunicatingProcessExample` class, which is located at **datakernel -> examples -> csp** and run its *main()* method.

#### Explanation
This example represents an `AsyncProcess` between `ChannelSupplier` and `ChannelConsumer`. This 
process takes a string, sets it to upper-case and adds string's length in parentheses:
```
HELLO(5)
WORLD(5)
NICE(4)
TO(2)
SEE(3)
YOU(3)
```
In this example `ChannelSupplier` represents an input and `ChannelConsumer` - output. *doProcess()* represents the main 
process of the example. In order to transform a ChannelSupplier with described process and print out the result, the 
following lines are executed: 

```java
CommunicatingProcessExample process = new CommunicatingProcessExample();
	ChannelSupplier.of("hello", "world", "nice", "to", "see", "you")
		//transforms this supplier with the described process
		.transformWith(process)
		.streamTo(ChannelConsumer.ofConsumer(System.out::println));
```
