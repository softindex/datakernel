* Utilizes FS module to operate on logs stored on different partitions.
* Log data is transferred using Datastream module which is perfect for large amount of lightweight items (just like logs).
* Uses LZ4 compression algorithm which is fast and allows to save storage space.