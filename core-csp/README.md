## CSP

CSP provides communication between processes with Go-like approach. This means that CSP creates channels which can be 
treated as a pipe that connects some processes. A value can be sent to a channel via `ChannelSupplier` and received 
from a channel by `ChannelConsumer`. 

When `ChannelConsumer` receives some data, it returns a `Promise` so that new object isn't 
passed until the previous one is consumed. 

`ChannelSuplier` gets data and then streams it to the next consumer. Both operations also return a Promise, as suppliers 
work only with one object at a time.

CSP utilizes Promise module which enables optimisation while communicating in tough conditions (for example, slow 
Internet connection).

The main advantages of CSP are:
* Supports file I/O using channels.
* Ability to handle exceptions and appropriately close channels, releasing resources and propagating exceptions through 
communication pipeline.
* Rich DSL that allows to write concise easy-to-read code.

A core concept of CSP is queue and its modifications: `ChannelBuffer` and `ChannelZeroBuffer`. They provide communication 
between Consumers and Suppliers and allow to create chains of these pipes if needed. Basically, these buffers pass objects 
which were consumed by Consumer to Supplier as soon as the queue gets a free space. This process is controlled by Promises.

`ChannelBuffer` can have a fixed sized of elements waiting in the queue, whereas `ChannelZeroBuffer` has a zero-sized queue, 
so that an element simply passes from Consumer to Supplier one by one.

`ChannelConsumer` and `ChannelSupplier` have `ChannelFileReader` and `ChannelFileWriter` wrappers optimized 
to asynchronously read/write binary data from/to files.

Another important tool for convenient usage of CSP is Transformer. It allows to transform data in suitable for Supplier way.
Typically, it transforms one channel into another, but can extend the approach and convert channel to any object needed.
For this purpose transformers utilize `ChannelBuffer`s. 

### You can explore CSP examples [here](https://github.com/softindex/datakernel/tree/master/examples/csp).