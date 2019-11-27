package datastream;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.net.AsyncTcpSocketNio;

import java.net.InetSocketAddress;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

/**
 * Demonstrates client ("Server #1" from the picture) which sends some data to other server
 * and receives some computed result.
 * Before running, you should launch {@link TcpServerExample} first!
 */
//[START EXAMPLE]
public final class TcpClientExample {
	public static final int PORT = 9922;

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		eventloop.connect(new InetSocketAddress("localhost", PORT), (socketChannel, e) -> {
			if (e == null) {
				AsyncTcpSocket socket = AsyncTcpSocketNio.wrapChannel(eventloop, socketChannel, null);

				StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
						.transformWith(ChannelSerializer.create(INT_SERIALIZER))
						.streamTo(ChannelConsumer.ofSocket(socket));

				StreamConsumerToList<String> consumer = StreamConsumerToList.create();

				ChannelSupplier.ofSocket(socket)
						.transformWith(ChannelDeserializer.create(UTF8_SERIALIZER))
						.streamTo(consumer);

				consumer.getResult()
						.whenResult(list -> list.forEach(System.out::println));

			} else {
				System.out.printf("Could not connect to server, make sure it is started: %s\n", e);
			}
		});

		eventloop.run();
	}
}
//[END EXAMPLE]
