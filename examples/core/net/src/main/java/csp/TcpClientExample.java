package csp;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.net.AsyncTcpSocketNio;

import java.net.InetSocketAddress;
import java.util.Scanner;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple TCP console client.
 * By default this client connects to the same address as the server in the previous example.
 */
public final class TcpClientExample {
	private final Eventloop eventloop = Eventloop.create();

	/* Thread, which sends characters and prints received responses to the console. */
	private void startCommandLineInterface(AsyncTcpSocket socket) {
		Thread thread = new Thread(() -> {
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
		thread.start();
	}

	//[START REGION_1]
	private void run() {
		System.out.println("Connecting to server at localhost (port 9922)...");
		eventloop.connect(new InetSocketAddress("localhost", 9922), (socketChannel, e) -> {
			if (e == null) {
				System.out.println("Connected to server, enter some text and send it by pressing 'Enter'.");
				AsyncTcpSocket socket = AsyncTcpSocketNio.wrapChannel(getCurrentEventloop(), socketChannel, null);

				BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
						.parseStream(ByteBufsParser.ofCrlfTerminatedBytes())
						.streamTo(ChannelConsumer.ofConsumer(buf -> System.out.println(buf.asString(UTF_8))));

				startCommandLineInterface(socket);
			} else {
				System.out.printf("Could not connect to server, make sure it is started: %s\n", e);
			}
		});
		eventloop.run();
	}

	public static void main(String[] args) {
		new TcpClientExample().run();
	}
	//[END REGION_1]
}
