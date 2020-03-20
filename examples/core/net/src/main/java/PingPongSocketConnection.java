/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsDecoder;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AsyncTcpSocketNio;
import io.datakernel.net.SimpleServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.promise.Promises.loop;
import static io.datakernel.promise.Promises.repeat;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PingPongSocketConnection {
	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);
	private static final int ITERATIONS = 3;
	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final ByteBufsDecoder<String> DECODER = ByteBufsDecoder.ofFixedSize(4)
			.andThen(buf -> buf.asString(UTF_8));

	//[START REGION_1]
	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		SimpleServer server = SimpleServer.create(
				socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
					repeat(() ->
							bufsSupplier.parse(DECODER)
									.whenResult(x -> System.out.println(x))
									.then(() -> socket.write(wrapAscii(RESPONSE_MSG))))
							.whenComplete(socket::close);
				})
				.withListenAddress(ADDRESS)
				.withAcceptOnce();

		server.listen();

		AsyncTcpSocketNio.connect(ADDRESS)
				.whenResult(socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
					loop(0,
							i -> i < ITERATIONS,
							i -> socket.write(wrapAscii(REQUEST_MSG))
									.then(() -> bufsSupplier.parse(DECODER)
											.whenResult(x -> System.out.println(x))
											.map($2 -> i + 1)))
							.whenComplete(socket::close);
				})
				.whenException(e -> { throw new RuntimeException(e); });

		eventloop.run();
	}
	//[END REGION_1]
}
