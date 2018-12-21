## Net

Net module abstracts async sockets and channels using CSP implementations and Eventloop module.

Core classes: 
* `EventloopServer` - represents non-blocking server which listens new connection and accepts it asynchronous.
* `AbstractServer` - non-blocking server which works in eventloop and implements `EventloopServer`.
* `AsyncTcpSocket` - common interface for connection-oriented transport protocols.
* `UdpPacket` - represents a UDP packet.

### You can explore Net examples [here](https://github.com/softindex/datakernel/tree/master/examples/net)