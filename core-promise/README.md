## Promise

`Promise` is an efficient replacement of default Java `CompletionStage` interface and resembles JavaScript `Promise`, 
representing partial and possibly asynchronous computations of a large one. DataKernel *promises* are faster and better 
optimized, with minimal overhead, memory consumption and Garbage Collector load.

You can add this module to your project by inserting the following dependency to **pom.xml**:
```xml
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-promise</artifactId>
    <version>3.0.0-SNAPSHOT</version>
</dependency>
```

### To find out more visit [our site](https://datakernel.io/docs/components/core/promise.html).
### You can explore Promise examples [here](https://github.com/softindex/datakernel/tree/master/examples/promise) 
These examples represent how to utilize [`Promises`](https://github.com/softindex/datakernel/blob/master/core-promise/src/main/java/io/datakernel/async/Promises.java) 
and [`AsyncFile`](https://github.com/softindex/datakernel/blob/master/core-promise/src/main/java/io/datakernel/file/AsyncFile.java) 
utility classes. `AsyncFile` allows you to work with files I/O asynchronously while `Promises` includes handy methods for 
*Promises* managing.