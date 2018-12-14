Datastream Module is useful for intra- and inter-server communication and asynchronous data processing.

It is an important building block for other DataKernel modules (OLAP Cube, Aggregation, RPC etc.)

DataStream is:
* Modern implementation of async reactive streams (unlike streams in Java 8 and traditional thread-based blocking streams)
* Asynchronous with extremely efficient congestion control, to handle natural imbalance in speed of data sources
* Composable stream operations (mappers, reducers, filters, sorters, mergers/splitters, compression, serialization)
* Stream-based network and file I/O on top of Eventloop module
* Compatibility with CSP module.

### You can explore Datastream examples [here](https://github.com/softindex/datakernel/tree/master/examples/datastreams)


## Stream Primitives

There are dozens of builtin primitives which you can simply wire to each other (as illustrated in the picture above).

Here is a list of them with short descriptions:

### Suppliers:
  * StreamSupplier.idle() - returns supplier which does nothing - neither sends any data nor closes itself.
  * StreamSupplier.closing() - returns supplier which closes itself immediately after binding.
  * StreamSupplier.closingWithError(Throwable) - closes itseslf with given error after binding.
  * StreamSupplier.of(values...) - sends given values and then closes.
  * StreamSupplier.ofIterator(iterator) - sends values from given iterator.
  * StreamSupplier.ofIterable(iterable) - same as above.
  * StreamSupplier.ofStream(stream) - sends values from stream iterator.
  * StreamSupplier.ofSupplier(supplier) - produces items from a given lambda.
  * StreamSupplier.ofSerialSuppliers(suppliers) - 
  * StreamSupplier.withLateBinding() - 
  * StreamSupplier.ofPromise(promise) - a wrapper which unwraps supplier from a CompletionStage (starts sending data from supplier from promise when promise is completed).
  * StreamSupplier.withEndOfStream(function < Promise, Promise >) - returns end of stream wrapper when stream closes.
  * StreamSupplier.withResult(StreamSupplier, CompletionStage) - wrapper which assigns given CompletionStage as a result to given supplier.
  * StreamSupplier.concat(StreamSupplier... /List StreamSupplier / Iterator < StreamSupplier >) - wrapper which concatenates given suppliers.
  * StreamSupplier.asSerialSupplier() - supplier which allows to non-blockingly write data to file.


### Consumers:
  * StreamConsumer.idle() - does nothing, when wired supplier finishes, it sets consumer's status as finished too.
  * StreamConsumer.skip() - 
  * StreamConsumer.closingWithError(error) - closes itself with given error.
  * StreamConsumer.ofSerialConsumer(consumer) -
  * StreamConsumer.ofSupplier(supplier) - 
  * StreamConsumer.withLateBinding() - 
  * StreamConsumer.ofPromise(promise) - 
  * StreamConsumer.withAcknowledgement(function) - 
  * StreamConsumer.asSerialConsumer() - consumer which allows to non-blockingly read data from file.

### Transformers:
  * StreamBuffer - 
  * StreamDecorator -  allows to apply function before sending data to the destination.
  * StreamFilter - passes through only those items which matched given predicate.
  * StreamJoin - complicated transfomer which joins more than one supplier into one consumer with strategies and mapping functions.
  * StreamLateBinder - stores a data receiver from consumer produce request if it is not wired and when it is actually wired request his new supplier to produce into that stored receiver.
  * StreamMap - smarter version of StreamFunction which can transform one item into various number of other items (equivalent of the flatMap operation).
  * StreamMapSplitter - 
  * StreamMerger - Merges streams sorted by keys and streams their sorted union.
  * StreamReducer - Performs aggregative functions on the elements from input streams sorted by keys. Searches key of item with key function, selects elements with some key, reduces it and streams the results sorted by key.
  * StreamReducers - static utility methods pertaining, contains primary ready for use reducers.
  * StreamReducerSimple - Performs a reduction on the elements of input streams using the key function.
  * StreamSharder - Divides input stream into groups with some key function, and sends obtained streams to consumers.
  * StreamSorter - Receives data and saves it, and on end of stream sorts it and streams to the destination.
  * StreamSplitter - Sends received items into multiple consumers at once.
  * StreamUnion - Unions all input streams and streams their items in order of receiving them to the destination.

## Benchmark

We have measured the performance of our streams under various use scenarios.

Results are shown in the table below.

In every scenario supplier generates 1 million numbers from 1 to 1,000,000.

Columns describe the different behaviour of the consumer (backpressure): whether it suspends and how often.

Numbers denote how many items has been processed by each stream graph per second (on a single core).

<table>
    <tr>
        <th rowspan="2">Use case</th>
        <th colspan="3">Consumer suspends</th>
    </tr>
    <tr>
        <th>after each item</th>
        <th>after every 10 items</th>
        <th>does not suspend <a href="#footnote-streams-benchmark">*</a></th>
    </tr>
    <tr>
        <td>supplier -> consumer</td>
        <td>18M</td>
        <td>38M</td>
        <td>43M</td>
    </tr>
    <tr>
        <td>supplier -> filter -> consumer (filter passes all items)</td>
        <td>16M</td>
        <td>36M</td>
        <td>42M</td>
    </tr>
    <tr>
        <td>supplier -> filter -> ... -> filter -> consumer (10 filters in chain that pass all items)</td>
        <td>9M</td>
        <td>20M</td>
        <td>24M</td>
    </tr>
    <tr>
        <td>supplier -> filter -> consumer (filter passes odd numbers)</td>
        <td>24M</td>
        <td>38M</td>
        <td>42M</td>
    </tr>
    <tr>
        <td>supplier -> filter -> transformer -> consumer (filter passes all items, transformer returns an input number)</td>
        <td>16M</td>
        <td>34M</td>
        <td>40M</td>
    </tr>
    <tr>
        <td>supplier -> splitter (2) -> consumer (2) (splitter splits an input stream into two streams)</td>
        <td>10M</td>
        <td>30M</td>
        <td>31M</td>
    </tr>
    <tr>
        <td>supplier -> splitter (2) -> union (2) -> consumer (splitter first splits an input stream into two streams; union then merges this two streams back into a single stream)</td>
        <td>8M</td>
        <td>24M</td>
        <td>31M</td>
    </tr>
    <tr>
        <td>supplier -> map -> map -> consumer (first mapper maps an input number into the key-value pair, the second one extracts back the value)</td>
        <td>16M</td>
        <td>31M</td>
        <td>36M</td>
    </tr>
    <tr>
        <td>supplier -> map -> reducer -> map -> consumer (first mapper maps an input number into the key-value pair, reducer sums values by key (buffer size = 1024), the second mapper extracts back the values)</td>
        <td>12M</td>
        <td>19M</td>
        <td>20M</td>
    </tr>
</table>

<a name="footnote-streams-benchmark">\*</a> Typically, suspend/resume occurs very infrequently, only when consumers are 
saturated or during network congestions. In most cases intermediate buffering alleviates the suspend/resume cost and 
brings amortized complexity of your data processing pipeline to maximum throughput figures shown here.
