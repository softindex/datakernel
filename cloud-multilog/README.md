## Multilog

* Utilizes FS module to operate on logs stored on different partitions.
* Log data is transferred using Datastream module which is perfect for large amount of lightweight items (just like logs).
* Uses LZ4 compression algorithm which is fast and allows to save storage space.

`Multilog` interface manages persistence of logs and `MultilogImpl` is an example implementation of the interface. It has 
the following core operations:
* *create()* - returns a new MultilogImpl.
* *write()*
* *read()*
