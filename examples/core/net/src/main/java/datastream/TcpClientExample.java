package datastream;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelDeserializer;
import io.datakernel.csp.process.ChannelSerializer;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;

/**
 * Demonstrates client ("Server #1" from the picture) which sends some data to other server
 * and receives some computed result.
 * Before running, you should launch {@link TcpServerExample} first!
 */
public final class TcpClientExample {
	public static final int PORT = 9922;
	static {
		LoggerConfigurer.enableLogging();
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		eventloop.connect(new InetSocketAddress("localhost", PORT), new ConnectCallback() {
			@Override
			public void onConnect(@NotNull SocketChannel socketChannel) {
				AsyncTcpSocketImpl socket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel, null);

				StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
						.transformWith(ChannelSerializer.create(INT_SERIALIZER))
						.streamTo(ChannelConsumer.ofSocket(socket));

				StreamConsumerToList<String> consumer = StreamConsumerToList.create();

				ChannelSupplier.ofSocket(socket)
						.transformWith(ChannelDeserializer.create(UTF8_SERIALIZER))
						.streamTo(consumer);

				consumer.getResult()
						.whenResult(list -> list.forEach(System.out::println));
			}

			@Override
			public void onException(@NotNull Throwable e) {
				System.out.printf("Could not connect to server, make sure it is started: %s\n", e);
			}
		});

		eventloop.run();
	}

}
