package csp;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsDecoder;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SimpleServer;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple echo server.
 */
public final class TcpServerExample {
	private static final int PORT = 9922;
	private static final byte[] CRLF = {CR, LF};

	/* Run server in an event loop. */
	//[START REGION_1]
	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		SimpleServer server = SimpleServer.create(socket ->
				BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
						.parseStream(ByteBufsDecoder.ofCrlfTerminatedBytes())
						.peek(buf -> System.out.println("client:" + buf.getString(UTF_8)))
						.map(buf -> {
							ByteBuf serverBuf = ByteBufStrings.wrapUtf8("Server> ");
							return ByteBufPool.append(serverBuf, buf);
						})
						.map(buf -> ByteBufPool.append(buf, CRLF))
						.streamTo(ChannelConsumer.ofSocket(socket)))
				.withListenPort(PORT);

		server.listen();

		System.out.println("Server is running");
		System.out.println("You can connect from telnet with command: telnet localhost 9922 or by running csp.TcpClientExample");

		eventloop.run();
	}
	//[END REGION_1]
}


