Net module abstracts async sockets and schannels using CSP implementations.

## Examples
1. [TCP Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/TcpEchoServerExample.java)
2. [TCP Client](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/TcpClientExample.java)
3. [TCP Multi Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/MultiEchoServerExample.java)
4. [TCP Ping Pong Socket Connection](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/MultiEchoServerExample.java)

To run the examples, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@TcpEchoServerExample
$ # OR
$ mvn clean compile exec:java@MultiEchoServerExample
{% endhighlight %}

This will start your echo server. Now try connecting to it by executing these lines in appropriate folder:
{% highlight bash %}
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@TcpClientExample
$ # OR
$ mvn clean compile exec:java@PingPongSocketConnection
{% endhighlight %}

Running TcpClientExample allows you to send messages to server and receive them back. If you started Multi Echo Server, 
feel free to connect multiple Tcp Clients and check how it works. 

If you just want to check whether your server works or no, run PingPongSocketConnection. It will try to send a message 
to your server and get a response from it if everything works correctly.