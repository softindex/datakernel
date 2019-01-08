## Dataflow

Dataflow is a distributed stream-based batch processing engine for Big Data applications.
You can write tasks to be executed on a dataset. The task is then compiled into execution graphs and passed as 
JSON commands to corresponding worker servers to be executed.

* Data graph consists of nodes that correspond to certain operations (e.g. Download, Filter, Map, Reduce, Sort, etc).
* User can define custom predicates, mapping functions, reducers to be executed on datasets.
* Nodes in a data graph have inputs and outputs which are identified by a unique StreamId. This allows inter-partition 
data computation.
* Since nodes are stateless by itself, computation is similar to passing data items through a pipeline, applying certain 
operations during the process.