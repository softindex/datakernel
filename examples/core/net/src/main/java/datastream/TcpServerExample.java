package datastream;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.datastream.processor.StreamMapper;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.net.ServerSocketSettings;
import io.datakernel.net.AsyncTcpSocketImpl;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;

/**
 * Demonstrates server ("Server #2" from the picture) which receives some data from clients,
 * computes it in a certain way and sends back the result.
 */
//[START EXAMPLE]
public final class TcpServerExample {

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create();

		eventloop.listen(new InetSocketAddress("localhost", TcpClientExample.PORT), ServerSocketSettings.create(100), channel -> {
			AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, channel, null);

			try {
				System.out.println("Client connected: " + channel.getRemoteAddress());
			} catch (IOException e) {
				e.printStackTrace();
			}

			ChannelSupplier.ofSocket(socket)
					.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
					.transformWith(StreamMapper.create(x -> x + " times 10 = " + x * 10))
					.transformWith(ChannelSerializer.create(UTF8_SERIALIZER))
					.streamTo(ChannelConsumer.ofSocket(socket));
		});

		System.out.println("Connect to the server by running datastream.TcpClientExample");

		eventloop.run();
	}
}
//[END EXAMPLE]
