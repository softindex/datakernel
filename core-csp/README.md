## CSP

CSP (stands for Communicating Sequential Process) provides I/O communication between channels and was inspired by Golang 
approach. 

CSP features:
* High performance and throughput speed
* Optimized for working with medium-sized objects (e.g. ByteBufs) 
* CSP has a reach DSL, which provides a simple programming model
* Has an async back pressure management

CSP communication is conducted with `ChannelSupplier` and `ChannelConsumer`, which provide and accept some data 
respectively. Each consecutive request to these channels should be called only after the previous request finishes, and 
to ensure its completion [Promise](https://github.com/softindex/datakernel/tree/master/core-promise) module is utilized.

`ChannelSupplier` has a `get()` method which returns a provided value wrapped in `Promise`. Until this `Promise` is 
completed either with a result or with an exception, it shouldn't be called again. Also note, that if `get()` returns 
`Promise` of `null`, this represents end of stream and no additional data should be requested from this supplier.

`ChannelConsumer` has an `accept(@Nullable T value)` method which returns a `Promise` of `null` as a marker of 
completion of the accepting. Until this `Promise` is completed, `accept()` method shouldn’t called again. By analogy 
with the `ChannelSupplier`, if `null` value is accepted, it represents end of stream.

Another important concept of CSP is `ChannelQueue` interface and its implementations: `ChannelBuffer` and 
`ChannelZeroBuffer`. They provide communication between Consumers and Suppliers and allow to create chains of these 
pipes if needed. Basically, these buffers pass objects which were consumed by Consumer to Supplier as soon as the queue 
gets a free space. This process is controlled by `Promise`s. You can manually set the size for `ChannelBuffer`, while 
`ChannelZeroBuffer` doesn’t store any values but simply passes them one by one from Consumer to Supplier. 

For example, a communication pipe might look as follows:
```
ChannelSupplier<T> -> ChannelConsumer<T> -> [Queue] -> ChannelSupplier<T> -> ChannelConsumer <R>
```

*`ChannelConsumer` and `ChannelSupplier` have `ChannelFileReader` and `ChannelFileWriter` wrappers optimized to 
asynchronously read/write binary data from/to files*

CSP has a lot in common with [Datastream](https://github.com/softindex/datakernel/tree/master/core-datastream) module. 
Even though they both were designed for I/O processing, there are several important distinctions:

| | Datastream | CSP |
| --- | --- | --- |
| **Overhead** | Extremely low: stream can be started with 1 virtual call, short-circuit evaluation optimizes performance | No short-circuit evaluation, overhead is higher |
| **Throughput speed** | Extremely fast | Fast, but slower than Datastream |
| **Optimized for** | Small pieces of data | Medium-sized objects, ByteBufs |
| **Programming model** | More complicated | Simple and convenient |

To provide maximum efficiency, our framework widely utilizes combinations of CSP and Datastream. For this purpose, 
`ChannelSupplier`, `ChannelConsumer`, `StreamSupplier` and `StreamConsumer` have `transformWith()` methods and special 
Transformer interfaces. Using them, you can seamlessly transform channels into other channels or datastreams and vice 
versa, creating chains of such transformations.


### You can explore CSP examples [here](https://github.com/softindex/datakernel/tree/master/examples/csp).