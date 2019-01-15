## Net

Net module abstracts async sockets and channels using [Eventloop](https://github.com/softindex/datakernel/tree/master/core-eventloop) 
and [ByteBuf](https://github.com/softindex/datakernel/tree/master/core-bytebuf) modules.

Core classes and interfaces: 

* `EventloopServer` interface represents non-blocking server which listens new connection and accepts it asynchronous.

* `AbstractServer` - non-blocking server which works in Eventloop and implements `EventloopServer` interface. There are 
also `PrimaryServer` and `SimpleServer` classes in the module which extend the `AbstractServer`.

* `AsyncTcpSocket` is a common interface for connection-oriented transport protocols. Module contains `AsyncTcpSocketImpl` 
class which implements the interface.

* `UdpPacket` represents a UDP packet. Each message routed from one machine to another is solely based on information 
contained within that packet

### You can explore Net examples [here](https://github.com/softindex/datakernel/tree/master/examples/net)