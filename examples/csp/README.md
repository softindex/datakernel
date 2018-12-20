## Examples
1. [Byte Bufs Parser Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ByteBufsParserExample.java) - 
A simple example of processing bytes utilizing CSP and ByteBuf modules.
2. [Channel Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ChannelExample.java) - 
Basic interactions between CSP `ChannelSupplier` and `ChannelConsumers`.
3. [Channel File Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/ChannelFileExample.java) - 
Represents working with files utilizing CSP `ChannelFileReader` and `ChannelFileWriter`.
4. [Communicating Process Example](https://github.com/softindex/datakernel/blob/master/examples/csp/src/main/java/io/datakernel/examples/CommunicatingProcessExample.java) - 
Represents communication between ChannelSupplier and `ChannelConsumer` utilizing transformation and Promise features.
For each of the examples you can download and run a [working example](#1-working-example) and see its
[explanation](#2-explanation). 

## 1. Working Example
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

## 2. Explanation
#### 1. Byte Bufs Parser Example
In this example the following DataKernel modules are utilized:
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

#### 2. Channel Example
In this example the following DataKernel modules are utilized:
* CSP
* Eventloop

```java
public class ChannelExample {
	private static void supplierOfValues() {
		//passing Supplier five Strings which are streamed to Consumer and then printed in accordance to 
		// #ofConsumer() setup.
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

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		supplierOfValues();
		supplierOfList(asList("One", "Two", "Three"));
		map();
		toCollector();
		filter();
		eventloop.run();
	}
}
```

#### 4. Communicating Process Example

```java
/**
 * Transforming AsyncProcess that takes a string, sets it to upper-case and adds string's length in parentheses
 */
public class CommunicatingProcessExample extends AbstractCommunicatingProcess implements WithChannelTransformer<CommunicatingProcessExample, String, String> {
	private ChannelSupplier<String> input;
	private ChannelConsumer<String> output;

//Checking if input and output equal null. If they are not - starting the doProcess()
	@Override
	public ChannelOutput<String> getOutput() {
		return output -> {
			this.output = output;
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	public ChannelInput<String> getInput() {
		return input -> {
			this.input = input;
			if (this.input != null && this.output != null) startProcess();
			return getProcessResult();
		};
	}

	@Override
	protected void doProcess() {
		//Returns a Promise
		input.get()
		    //When ChannelSupplier receives data, whenComplete() is triggered
			.whenComplete((data, e) -> {
				//If data is null, the process completes. This means that Supplier has no more data left
				//Otherwise data is formatted and doProcess() runs again
				if (data == null) {
					output.accept(null)
							.whenResult($ -> completeProcess());
				} else {
					data = data.toUpperCase() + '(' + data.length() + ')';
					output.accept(data)
							.whenResult($ -> doProcess());
				}
			});
	}

	@Override
	protected void doClose(Throwable e) {
		System.out.println("Process has been closed with exception: " + e);
		//closes processes with exception
		input.close(e);
		output.close(e);
	}

	public static void main(String[] args) {
		CommunicatingProcessExample process = new CommunicatingProcessExample();
		ChannelSupplier.of("hello", "world", "nice", "to", "see", "you")
				.transformWith(process)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}
}
```