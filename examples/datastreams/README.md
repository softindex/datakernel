Very simple implementation (less then 100 lines of code!) of interserver stream:
1. [Network Demo Server](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/NetworkDemoServer.java)
2. [Network Demo Client](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/NetworkDemoClient.java)

To run the examples, you should execute these lines in the console in appropriate folder in the order given here:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/datastreams
$ mvn exec:java@NetworkDemoServer
$ # and then
$ mvn exec:java@NetworkDemoClient
```

Example's stream graph is illustrated in the picture below:
<img src="http://www.plantuml.com/plantuml/png/dPH1RiCW44Ntd694Dl72aT83LBb3J-3QqmJLPYmO9qghtBrGspME0uwwPHwVp_-2W-N2SDVKmZAPueWWtz2SqS1cB-5R0A1cnLUGhQ6gAn6KPYk3TOj65RNwGk0JDdvCy7vbl8DqrQy2UN67WaQ-aFaCCOCbghDN8ei3_s6eYV4LJgVtzE_nbetInvc1akeQInwK1y3HK42jB4jnMmRmCWzWDFTlM_V9bTIq7Kzk1ablqADWgS4JNHw7FLqXcdUOuZBrcn3RiDCCylmLjj4wCv6OZNkZBMT29CUmspc1TCHUOuNeVIJoTxT8JVlzJnRZj9ub8U_QURhB_cO1FnXF6YlT_cMTXEQ9frvSc7kI6nscdsMyWX4OTLOURIOExfRkx_e1">

This transformations of datastreams on Server #2 are implemented in the following way:

```java
//ofSocket() returns ChannelSupplier of ByteBufs that are received from network
ChannelSupplier.ofSocket(socket)
    //ChannelDeserializer transfroms data from received ByteBufs to some other types
	.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
	//StreamDecorator receives a specified type of data and streams set of function's result
	.transformWith(StreamDecorator.create(x -> x + " times 10 = " + x * 10))
	//ChannelSerializer transforms data to ByteBufs
	.transformWith(ChannelSerializer.create(UTF8_SERIALIZER))
	//streams data to ChannelConsumer
	.streamTo(ChannelConsumer.ofSocket(socket));
```
Let's see how Server #1 interacts with Server #2:
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
				//transforms our data to ByteBufs
					.transformWith(ChannelSerializer.create(INT_SERIALIZER))
					//streams ByteBufs to our Consumer - NetworkDemoServer
					.streamTo(ChannelConsumer.ofSocket(socket));
				
                //Creating a consumer for processing responses from NetworkDemoServer
				StreamConsumerToList<String> consumer = StreamConsumerToList.create();

                //Receives ByteBufs from the network
				ChannelSupplier.ofSocket(socket)
				    //Deserializes ByteBufs
					.transformWith(ChannelDeserializer.create(UTF8_SERIALIZER))
					//streams the result of deserialization to our consumer
					.streamTo(consumer);
                
				//when consumer gets a result, it prints it out
				consumer.getResult().whenResult(list -> list.forEach(System.out::println));
			}
    }
}
```

Please note that this example is very simple. Big graphs can span over numerous servers and process a lot of data in 
various ways.

Here are some other examples of creating stream nodes:

1. [Simple Supplier](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/SupplierExample.java) - 
shows how to create a simple Supplier and stream some data to Consumer.
2. [Simple Consumer](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/ConsumerExample.java) - 
shows how to create a simple custom Consumer.
3. [Custom Transformer](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/TransformerExample.java) - 
shows how to create a custom StreamTransformer, which takes strings and transforms them to their length if it is less than MAX_LENGTH.
4. [Builtin Stream Nodes Example](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/BuiltinStreamNodesExample.java) - 
demonstrates some of builtin Stream possibilities, such as filtering, sharding and mapping.

To run the examples, you should execute these lines in the console in the appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/datastreams
$ mvn exec:java@SupplierExample
$ # or
$ mvn exec:java@ConsumerExample
$ # or
$ mvn exec:java@TransformerExample
$ # or
$ mvn exec:java@BuiltinStreamNodesExample
```


