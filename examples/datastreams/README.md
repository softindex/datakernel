### Datastreams
1. [Simple Supplier](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/SupplierExample.java) - 
shows how to create a simple *Supplier* and stream some data to *Consumer*. [Launch](#1-simple-supplier)
2. [Simple Consumer](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/ConsumerExample.java) - 
shows how to create a simple custom *Consumer*. [Launch](#2-simple-consumer)
3. [Custom Transformer](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/TransformerExample.java) - 
shows how to create a custom *StreamTransformer*, which takes strings and transforms them to their length if it is less than *MAX_LENGTH*. [Launch](#3-custom-transformer)
4. [Built-in Stream Nodes Example](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/BuiltinStreamNodesExample.java) - 
demonstrates some of built-in Datastream possibilities, such as filtering, sharding and mapping. [Launch](#4-built-in-stream-nodes)

Very simple implementation (less then 100 lines of code!) of inter-server stream which demonstrates Datasteams and CSP 
compatibility:
1. [Network Demo Server](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/NetworkDemoServer.java)
2. [Network Demo Client](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/NetworkDemoClient.java)

[Launch](#5-datasteams-and-csp-compatibility-example)

### 1. Simple Supplier
#### Launch

To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/datastreams
$ mvn exec:java@SupplierExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `SupplierExample` class, which is located at **datakernel -> examples -> datastreams**, and run its *main()* 
method.

#### Explanation
 
When you run **SupplierExample**, you'll see the following output:
```
Consumer received: [0, 1, 2, 3, 4]
```

Let's have a look at the implementation:
```java
public static void main(String[] args) {
	
	//creating an eventloop for streams operations
	Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
	
	//creating a supplier of some numbers
	StreamSupplier<Integer> supplier = StreamSupplier.of(0, 1, 2, 3, 4);
	
	//creating a consumer for our supplier
	StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
    
	//streaming supplier's numbers to consumer
	supplier.streamTo(consumer);

	//when stream completes, the streamed data is printed out
	consumer.getResult().accept(result -> System.out.println("Consumer received: " + result));
	eventloop.run();
	}
```

### 2. Simple Consumer
#### Launch

To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/datastreams
$ mvn exec:java@ConsumerExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ConsumerExample` class, which is located at **datakernel -> examples -> datastreams**, and run its *main()* 
method.

#### Explanation

When you run **ConsumerExample**, you'll see the following output:
```
received: 1
received: 2
received: 3
End of stream received
```
`ConsumerExample` extends `AbstractStreamConsumer` and just prints out received data. The stream process is managed with 
overridden methods *onStarted()*, *onEndOfStream()* and *onError()*.

### 3. Custom Transformer
#### Launch
To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/datastreams
$ mvn exec:java@TransformerExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `TransformerExample` class, which is located at **datakernel -> examples -> datastreams**, and run its *main()* 
method.

#### Explanation

**TransformerExample** shows how to create a custom `StreamTransformer` which takes strings from input stream and 
transforms them to their length if it is less than defined MAX_LENGTH. If you run the example, you'll receive the 
following output:
```
[8, 9]
```
This is the result of transforming `StreamSupplier.of("testdata", "testdata1", "testdata1000").`

### 4. Built-in Stream Nodes
#### Launch

To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/datastreams
$ mvn exec:java@BuitinStreamNodesExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `BuiltinStreamNodesExample` class, which is located at **datakernel -> examples -> datastreams**, and run its *main()* 
method.

#### Explanation

**BuiltinStreamNodesExample** demonstrates some simple examples of utilizing built-in datastream nodes. If you 
run the example, you'll receive the following output:
```
[1 times ten = 10, 2 times ten = 20, 3 times ten = 30, 4 times ten = 40, 5 times ten = 50, 6 times ten = 60, 7 times ten = 70, 8 times ten = 80, 9 times ten = 90, 10 times ten = 100]
third: [2, 5, 8]
second: [1, 4, 7, 10]
first: [3, 6, 9]
[1, 3, 5, 7, 9]
```
The first line is a result of `StreamMapper`:
```java
private static void mapper() {
	//creating a supplier of 10 numbers
	StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
	//creating a mapper for the numbers
	StreamMapper<Integer, String> simpleMap = StreamMapper.create(x -> x + " times ten = " + x * 10);
        
	//creating a consumer which converts received values to list
	StreamConsumerToList<String> consumer = StreamConsumerToList.create();
        
	//applying the mapper to supplier and streaming the result to consumer
	supplier.transformWith(simpleMap).streamTo(consumer);
        
	//when consumer completes receiving values, the result is printed out
	consumer.getResult().accept(System.out::println);
}
```

The next three lines of the output are results of utilizing `StreamSharder`:
```java
private static void sharder() {
	
	//creating a supplier of 10 numbers
	StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //creating a sharder of three parts for three consumers
	StreamSharder<Integer> sharder = StreamSharder.create(new HashSharder<>(3));

	//creating 3 consumers which convert received values to list
	StreamConsumerToList<Integer> first = StreamConsumerToList.create();
	StreamConsumerToList<Integer> second = StreamConsumerToList.create();
	StreamConsumerToList<Integer> third = StreamConsumerToList.create();

	//streaming supplier's numbers to sharder
	supplier.streamTo(sharder.getInput());

	//streaming sharder's shareded supplier's numbers to consumers
	sharder.newOutput().streamTo(first);
	sharder.newOutput().streamTo(second);
	sharder.newOutput().streamTo(third);


	//when consumers complete receiving values, the result is printed out
	first.getResult().accept(x -> System.out.println("first: " + x));
	second.getResult().accept(x -> System.out.println("second: " + x));
	third.getResult().accept(x -> System.out.println("third: " + x));
```

The last line of the output is a result of utilizing `StreamFilter`:
```java
private static void filter() {
		
	//creating a supplier of 10 numbers
	StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

	//creating a filter which selects only odd numbers
	StreamFilter<Integer> filter = StreamFilter.create(input -> input % 2 == 1);

	//creating a consumer which converts received values to list
	StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

	//applying filter to supplier's numbers and streaming the result to consumer
	supplier.transformWith(filter).streamTo(consumer);

   	//when consumer completes receiving values, the result is printed out
	consumer.getResult().accept(System.out::println);
}
```


### 5. Datasteams and CSP compatibility example
#### Launch

To run the example in console, you should execute these lines in appropriate folder in the order given here:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/datastreams
$ mvn exec:java@NetworkDemoServer
$ # then
$ mvn exec:java@NetworkDemoClient
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Open `NetworkDemoServer` class, which is located at **datakernel -> examples -> datastreams**, and run its *main()* 
method. Then open `NetworkDemoClient` class, which is located at the same directory, and also run its *main()* method.

#### Explanation
Example's stream graph is illustrated in the picture below:
<img src="http://www.plantuml.com/plantuml/png/dPH1RiCW44Ntd694Dl72aT83LBb3J-3QqmJLPYmO9qghtBrGspME0uwwPHwVp_-2W-N2SDVKmZAPueWWtz2SqS1cB-5R0A1cnLUGhQ6gAn6KPYk3TOj65RNwGk0JDdvCy7vbl8DqrQy2UN67WaQ-aFaCCOCbghDN8ei3_s6eYV4LJgVtzE_nbetInvc1akeQInwK1y3HK42jB4jnMmRmCWzWDFTlM_V9bTIq7Kzk1ablqADWgS4JNHw7FLqXcdUOuZBrcn3RiDCCylmLjj4wCv6OZNkZBMT29CUmspc1TCHUOuNeVIJoTxT8JVlzJnRZj9ub8U_QURhB_cO1FnXF6YlT_cMTXEQ9frvSc7kI6nscdsMyWX4OTLOURIOExfRkx_e1">

This transformations of datastreams on Server #2 are implemented in the following way:

```java
//ofSocket() returns ChannelSupplier of ByteBufs that are received from network
ChannelSupplier.ofSocket(socket)

    //ChannelDeserializer transfroms data from received ByteBufs to integers
	.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
	
	//StreamDecorator receives a specified type of data and streams set of function's result
	.transformWith(StreamDecorator.create(x -> x + " times 10 = " + x * 10))
	
	//ChannelSerializer transforms data to ByteBufs
	.transformWith(ChannelSerializer.create(UTF8_SERIALIZER))
	
	//streams data to ChannelConsumer
	.streamTo(ChannelConsumer.ofSocket(socket));
```
Let's see how **Server #1** interacts with **Server #2**:
```java
public static void main(String[] args) {
	//createing an Eventloop for connecting to our server	
	Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
    
		eventloop.connect(new InetSocketAddress("localhost", PORT), new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, null);
				
				//StreamSupplier allows to asynchronously stream send streams of data. of() defines what values will be sent
				StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
				    
				    //Transforms our data to ByteBufs
					.transformWith(ChannelSerializer.create(INT_SERIALIZER))
					
					//Streams ByteBufs to our Consumer - NetworkDemoServer
					.streamTo(ChannelConsumer.ofSocket(socket));
				
				//Creating a consumer for processing responses from NetworkDemoServer
				StreamConsumerToList<String> consumer = StreamConsumerToList.create();
				
				//Receives ByteBufs from the network
				ChannelSupplier.ofSocket(socket)
				
				    //Deserializes ByteBufs
					.transformWith(ChannelDeserializer.create(UTF8_SERIALIZER))
					
					//Streams the result of deserialization to our consumer
					.streamTo(consumer);
                
				//When consumer gets a result, it prints it out
				consumer.getResult().accept(list -> list.forEach(System.out::println));
			}
    }
}
```
Please note that this example is very simple. Big graphs can span over numerous servers and process a lot of data in 
various ways.




