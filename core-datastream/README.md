## Datastream

Datastream Module is useful for intra- and inter-server communication and asynchronous data processing.

It is an important building block for other DataKernel modules.

DataStream is:
* Modern implementation of async reactive streams (unlike streams in Java 8 and traditional thread-based blocking streams)
* Asynchronous with extremely efficient congestion control, to handle natural imbalance in speed of data sources
* Composable stream operations (mappers, reducers, filters, sorters, mergers/splitters, compression, serialization)
* Stream-based network and file I/O on top of Eventloop module
* Compatibility with [CSP module](https://github.com/softindex/datakernel/tree/master/core-csp)


Datastream has a lot in common with [CSP](https://github.com/softindex/datakernel/tree/master/core-csp) module. 
Although they both were designed for I/O processing, there are several important distinctions:

| | Datastream | CSP |
| --- | --- | --- |
| **Overhead:** | Extremely low: stream can be started with 1 virtual call, short-circuit evaluation optimizes performance | No short-circuit evaluation, overhead is higher |
| **Throughput speed:** | Extremely fast | Fast, but slower than Datastream |
| **Optimized for:** | Small pieces of data | Medium-sized objects, ByteBufs |
| **Programming model:** | More complicated | Simple and convenient |

To provide maximum efficiency, our framework widely utilizes combinations of CSP and Datastream. For this purpose, 
`ChannelSupplier`, `ChannelConsumer`, `StreamSupplier` and `StreamConsumer` have *transformWith()* methods and special 
Transformer interfaces. Using them, you can seamlessly transform channels into other channels or datastreams and vice 
versa, creating chains of such transformations.

See an example of CSP and Datastreams compatibility 
[here](https://github.com/softindex/datakernel/tree/master/examples/datastreams#5-datasteams-and-csp-compatibility-example).

### You can explore more Datastream examples [here](https://github.com/softindex/datakernel/tree/master/examples/datastreams)

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
