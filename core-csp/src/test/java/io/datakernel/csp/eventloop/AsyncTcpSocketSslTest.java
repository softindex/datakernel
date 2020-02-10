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

package io.datakernel.csp.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.net.AsyncTcpSocketNio;
import io.datakernel.net.AsyncTcpSocketSsl;
import io.datakernel.net.SimpleServer;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.ActivePromisesRule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static io.datakernel.async.process.Cancellable.CLOSE_EXCEPTION;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class AsyncTcpSocketSslTest {
	private static final String KEYSTORE_PATH = "./src/test/resources/keystore.jks";
	private static final String KEYSTORE_PASS = "testtest";
	private static final String KEY_PASS = "testtest";

	private static final String TRUSTSTORE_PATH = "./src/test/resources/truststore.jks";
	private static final String TRUSTSTORE_PASS = "testtest";

	private static final String TEST_STRING = "Hello world";

	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", getFreePort());

	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(TEST_STRING.length())
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	public static final int LARGE_STRING_SIZE = 10_000;
	public static final int SMALL_STRING_SIZE = 1000;
	private static final int LENGTH = LARGE_STRING_SIZE + TEST_STRING.length() * SMALL_STRING_SIZE;

	private static final ByteBufsParser<String> PARSER_LARGE = ByteBufsParser.ofFixedSize(LENGTH)
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private Executor executor;
	private SSLContext sslContext;
	private StringBuilder sentData;

	@Before
	public void setUp() throws Exception {
		executor = Executors.newSingleThreadExecutor();
		sslContext = createSslContext();
		sentData = new StringBuilder();
	}

	@Test
	public void testWrite() throws IOException {
		startServer(sslContext, sslSocket -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
				.parse(PARSER)
				.whenComplete(sslSocket::close)
				.whenComplete(assertComplete(result -> assertEquals(TEST_STRING, result))));

		await(AsyncTcpSocketNio.connect(ADDRESS)
				.map(socket -> AsyncTcpSocketSsl.wrapClientSocket(socket, sslContext, executor))
				.then(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING))
								.whenComplete(sslSocket::close)));
	}

	@Test
	public void testRead() throws IOException {
		startServer(sslContext, sslSocket ->
				sslSocket.write(wrapAscii(TEST_STRING))
						.whenComplete(assertComplete()));

		String result = await(AsyncTcpSocketNio.connect(ADDRESS)
				.map(socket -> AsyncTcpSocketSsl.wrapClientSocket(socket, sslContext, executor))
				.then(sslSocket -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
						.parse(PARSER)
						.whenComplete(sslSocket::close)));

		assertEquals(TEST_STRING, result);
	}

	@Test
	public void testLoopBack() throws IOException {
		startServer(sslContext, serverSsl -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(serverSsl))
				.parse(PARSER)
				.then(result -> serverSsl.write(wrapAscii(result)))
				.whenComplete(serverSsl::close)
				.whenComplete(assertComplete()));

		String result = await(AsyncTcpSocketNio.connect(ADDRESS)
				.map(socket -> AsyncTcpSocketSsl.wrapClientSocket(socket, sslContext, executor))
				.then(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING))
								.then($ -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
										.parse(PARSER))
								.whenComplete(sslSocket::close)));

		assertEquals(TEST_STRING, result);
	}

	@Test
	public void testLoopBackWithEmptyBufs() throws IOException {
		int halfLength = TEST_STRING.length() / 2;
		String TEST_STRING_PART_1 = TEST_STRING.substring(0, halfLength);
		String TEST_STRING_PART_2 = TEST_STRING.substring(halfLength);
		startServer(sslContext, serverSsl -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(serverSsl))
				.parse(PARSER)
				.then(result -> serverSsl.write(wrapAscii(result)))
				.whenComplete(serverSsl::close)
				.whenComplete(assertComplete()));

		String result = await(AsyncTcpSocketNio.connect(ADDRESS)
				.map(socket -> AsyncTcpSocketSsl.wrapClientSocket(socket, sslContext, executor))
				.then(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING_PART_1))
								.then($ -> sslSocket.write(ByteBuf.empty()))
								.then($ -> sslSocket.write(wrapAscii(TEST_STRING_PART_2)))
								.then($ -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
										.parse(PARSER))
								.whenComplete(sslSocket::close)));

		assertEquals(TEST_STRING, result);
	}


	@Test
	public void sendsLargeAmountOfDataFromClientToServer() throws IOException {
		startServer(sslContext, serverSsl -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(serverSsl))
				.parse(PARSER_LARGE)
				.whenComplete(serverSsl::close)
				.whenComplete(assertComplete(result -> assertEquals(result, sentData.toString()))));

		await(AsyncTcpSocketNio.connect(ADDRESS)
				.map(socket -> AsyncTcpSocketSsl.wrapClientSocket(socket, sslContext, executor))
				.whenResult(sslSocket ->
						sendData(sslSocket)
								.whenComplete(sslSocket::close)));
	}

	@Test
	public void sendsLargeAmountOfDataFromServerToClient() throws IOException {
		startServer(sslContext, serverSsl ->
				sendData(serverSsl)
						.whenComplete(serverSsl::close)
						.whenComplete(assertComplete()));

		String result = await(AsyncTcpSocketNio.connect(ADDRESS)
				.map(socket -> AsyncTcpSocketSsl.wrapClientSocket(socket, sslContext, executor))
				.then(sslSocket -> BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket))
						.parse(PARSER_LARGE)
						.whenComplete(sslSocket::close)));

		assertEquals(sentData.toString(), result);
	}

	@Test
	public void testCloseAndOperationAfterClose() throws IOException {
		startServer(sslContext, socket ->
				socket.write(wrapAscii("He"))
						.whenComplete(socket::close)
						.then($ -> socket.write(wrapAscii("ello")))
						.whenComplete(($, e) -> assertSame(CLOSE_EXCEPTION, e)));

		Throwable e = awaitException(AsyncTcpSocketNio.connect(ADDRESS)
				.map(socket -> AsyncTcpSocketSsl.wrapClientSocket(socket, sslContext, executor))
				.then(sslSocket -> {
					BinaryChannelSupplier supplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(sslSocket));
					return supplier.parse(PARSER)
							.whenException(supplier::close);
				}));

		assertSame(CLOSE_EXCEPTION, e);
	}

	static void startServer(SSLContext sslContext, Consumer<AsyncTcpSocket> logic) throws IOException {
		SimpleServer.create(logic)
				.withSslListenAddress(sslContext, Executors.newSingleThreadExecutor(), ADDRESS)
				.withAcceptOnce()
				.listen();
	}

	static SSLContext createSslContext() throws Exception {
		SSLContext instance = SSLContext.getInstance("TLSv1.2");

		KeyStore keyStore = KeyStore.getInstance("JKS");
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		try (InputStream input = new FileInputStream(new File(KEYSTORE_PATH))) {
			keyStore.load(input, KEYSTORE_PASS.toCharArray());
		}
		kmf.init(keyStore, KEY_PASS.toCharArray());

		KeyStore trustStore = KeyStore.getInstance("JKS");
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		try (InputStream input = new FileInputStream(new File(TRUSTSTORE_PATH))) {
			trustStore.load(input, TRUSTSTORE_PASS.toCharArray());
		}
		tmf.init(trustStore);

		instance.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
		return instance;
	}

	public static String generateLargeString(int size) {
		StringBuilder builder = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < size; i++) {
			int randNumber = random.nextInt(3);
			if (randNumber == 0) {
				builder.append('a');
			} else if (randNumber == 1) {
				builder.append('b');
			} else if (randNumber == 2) {
				builder.append('c');
			}
		}
		return builder.toString();
	}

	private Promise<?> sendData(AsyncTcpSocket socket) {
		String largeData = generateLargeString(LARGE_STRING_SIZE);
		ByteBuf largeBuf = wrapAscii(largeData);
		sentData.append(largeData);

		return socket.write(largeBuf)
				.then($ -> Promises.loop(SMALL_STRING_SIZE,
						i -> i != 0,
						i -> {
							sentData.append(TEST_STRING);
							return socket.write(wrapAscii(TEST_STRING))
									.async()
									.map($2 -> i - 1);
						}));
	}
	// endregion
}
