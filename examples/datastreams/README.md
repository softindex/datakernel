
Very simple implementation (less then 100 lines of code!) of interserver stream:
1. [Network Demo Client](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/NetworkDemoClient.java)
2. [Network Demo Server](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/NetworkDemoServer.java)

To run the examples, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/datastreams
$ mvn clean compile exec:java@NetworkDemoServer
$ # in another console
$ mvn clean compile exec:java@NetworkDemoClient
```

Example's stream graph is illustrated in the picture below:
<img src="http://www.plantuml.com/plantuml/png/dPH1RiCW44Ntd694Dl72aT83LBb3J-3QqmJLPYmO9qghtBrGspME0uwwPHwVp_-2W-N2SDVKmZAPueWWtz2SqS1cB-5R0A1cnLUGhQ6gAn6KPYk3TOj65RNwGk0JDdvCy7vbl8DqrQy2UN67WaQ-aFaCCOCbghDN8ei3_s6eYV4LJgVtzE_nbetInvc1akeQInwK1y3HK42jB4jnMmRmCWzWDFTlM_V9bTIq7Kzk1ablqADWgS4JNHw7FLqXcdUOuZBrcn3RiDCCylmLjj4wCv6OZNkZBMT29CUmspc1TCHUOuNeVIJoTxT8JVlzJnRZj9ub8U_QURhB_cO1FnXF6YlT_cMTXEQ9frvSc7kI6nscdsMyWX4OTLOURIOExfRkx_e1">

Please note that this example is very simple. Big graphs can span over numerous servers and process a lot of data in various ways.

Here are some other examples of creating stream nodes:

1. [Simple Supplier](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/SupplierExample.java) - represents how supplier provides consumer with some data (in the example - with 5 numbers)
2. [Simple Consumer](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/ConsumerExample.java) - represents how consumer receives information (in the example - 3 numbers)
3. [Custom Transformer](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/TransformerExample.java) - this example shows transformation of Strings to integer in accordance to their length and discarding those Strings which are longer then 10 symbols
4. [Builtin Stream Nodes Example](https://github.com/softindex/datakernel/blob/master/examples/datastreams/src/main/java/io/datakernel/examples/BuiltinStreamNodesExample.java) - demonstrates some of Stream functionalities

To run them, you should execute these lines in the console in the appropriate folder:
```
$ git clone https://github.com/softindex/datakernel-examples.git
$ cd datakernel-examples/examples/datastreams
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.SupplierExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.ConsumerExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.TransformerExample
$ # OR
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.BuiltinStreamNodesExample
```

Note that for network demo you should first launch the server and then the client.