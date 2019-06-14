package csp;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.NotNull;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple TCP console client.
 * By default this client connects to the same address as the server in the previous example.
 */
public final class TcpClientExample {
	private final Eventloop eventloop = Eventloop.create();
	private AsyncTcpSocket socket;
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	/* Thread, which sends characters and prints received responses to the console. */
	private Thread getScannerThread() {
		return new Thread(() -> {
			Scanner scanIn = new Scanner(System.in);
			while (true) {
				String line = scanIn.nextLine();
				if (line.isEmpty()) {
					break;
				}
				ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(line + "\r\n"));
				eventloop.execute(() -> socket.write(buf));
			}
			eventloop.execute(socket::close);
		});
	}

	private void run() {
		System.out.println("Connecting to server at localhost (port 9922)...");
		eventloop.connect(new InetSocketAddress("localhost", 9922), new ConnectCallback() {
			@Override
			public void onConnect(@NotNull SocketChannel socketChannel) {
				System.out.println("Connected to server, enter some text and send it by pressing 'Enter'.");
				socket = AsyncTcpSocket.ofSocketChannel(socketChannel);

				BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
						.parseStream(ByteBufsParser.ofCrlfTerminatedBytes())
						.streamTo(ChannelConsumer.ofConsumer(buf -> System.out.println(buf.asString(UTF_8))));

				getScannerThread().start();
			}

			@Override
			public void onException(@NotNull Throwable e) {
				System.out.printf("Could not connect to server, make sure it is started: %s\n", e);
			}
		});

		eventloop.run();
	}

	public static void main(String[] args) {
		new TcpClientExample().run();
	}
}
