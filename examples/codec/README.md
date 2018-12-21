1. [Structured Codec Example](https://github.com/softindex/datakernel/blob/master/examples/codec/src/main/java/io/datakernel/examples/StructuredCodecsExample.java)- 
converting a LocalDate object to JSON string and then recovering it back to LocalDate object with Gson Adapter.

To run the example, you should first execute these lines in the console in appropriate folder:

```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/json
$ mvn clean compile exec:java@StructuredCodecsExample
```