A channel can be treated as a pipe that connects some processes. A value can be 
sent to a channel via `ChannelConsumer` and received from a channel by `ChannelSupplier`.

* Supports file I/O using channels.
* Ability to handle exceptions and appropriately close channels, releasing resources and propagating exceptions through 
communication pipeline.
* Rich DSL that allows to write concise easy-to-read code.