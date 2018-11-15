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

package io.datakernel.eventloop;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static io.datakernel.async.Cancellable.CLOSE_EXCEPTION;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(DatakernelRunner.class)
public final class AsyncSslSocketTest {
	private static final String KEYSTORE_PATH = "./src/test/resources/keystore.jks";
	private static final String KEYSTORE_PASS = "testtest";
	private static final String KEY_PASS = "testtest";

	private static final String TRUSTSTORE_PATH = "./src/test/resources/truststore.jks";
	private static final String TRUSTSTORE_PASS = "testtest";

	private static final String TEST_STRING = "Hello world";

	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 37832);

	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(TEST_STRING.length())
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	private static final int LENGTH = 10_000 + TEST_STRING.length() * 1_000;

	private static final ByteBufsParser<String> PARSER_LARGE = ByteBufsParser.ofFixedSize(LENGTH)
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	private ExecutorService executor;
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
		startServer(sslContext, sslSocket ->
				PARSER.parse(ByteBufsSupplier.of(SerialSupplier.ofSocket(sslSocket)))
						.whenComplete(($, e) -> sslSocket.close())
						.whenComplete(assertComplete(result -> assertEquals(TEST_STRING, result))));

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenApply(tcpSocket -> AsyncSslSocket.wrapClientSocket(tcpSocket, sslContext, executor))
				.thenCompose(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING))
								.whenComplete(($, e) -> sslSocket.close()))
				.whenComplete(assertComplete());
	}

	@Test
	public void testRead() throws IOException {
		startServer(sslContext, sslSocket ->
				sslSocket.write(wrapAscii(TEST_STRING))
						.whenComplete(assertComplete()));

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenApply(tcpSocket -> AsyncSslSocket.wrapClientSocket(tcpSocket, sslContext, executor))
				.thenCompose(sslSocket ->
						PARSER.parse(ByteBufsSupplier.of(SerialSupplier.ofSocket(sslSocket)))
								.whenComplete(($, e) -> sslSocket.close()))
				.whenComplete(assertComplete(result -> assertEquals(TEST_STRING, result)));
	}

	@Test
	public void testLoopBack() throws IOException {
		startServer(sslContext, serverSsl ->
				PARSER.parse(ByteBufsSupplier.of(SerialSupplier.ofSocket(serverSsl)))
						.thenCompose(result -> serverSsl.write(wrapAscii(result)))
						.whenComplete(($, e) -> serverSsl.close())
						.whenComplete(assertComplete()));

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenApply(tcpSocket -> AsyncSslSocket.wrapClientSocket(tcpSocket, sslContext, executor))
				.whenResult(sslSocket ->
						sslSocket.write(wrapAscii(TEST_STRING))
								.thenCompose($ -> PARSER.parse(ByteBufsSupplier.of(SerialSupplier.ofSocket(sslSocket))))
								.whenComplete(($, e) -> sslSocket.close())
								.whenComplete(assertComplete(result -> assertEquals(TEST_STRING, result))));
	}

	@Test
	public void sendsLargeAmountOfDataFromClientToServer() throws IOException {
		startServer(sslContext, serverSsl ->
				PARSER_LARGE.parse(ByteBufsSupplier.of(SerialSupplier.ofSocket(serverSsl)))
						.whenComplete(($, e) -> serverSsl.close())
						.whenComplete(assertComplete(result -> assertEquals(result, sentData.toString()))));

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenApply(tcpSocket -> AsyncSslSocket.wrapClientSocket(tcpSocket, sslContext, executor))
				.whenResult(sslSocket ->
						sendData(sslSocket)
								.whenComplete(($, e) -> sslSocket.close())
								.whenComplete(assertComplete()));
	}

	@Test
	public void sendsLargeAmountOfDataFromServerToClient() throws IOException {
		startServer(sslContext, serverSsl ->
				sendData(serverSsl)
						.whenComplete(($, e) -> serverSsl.close())
						.whenComplete(assertComplete()));

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenApply(tcpSocket -> AsyncSslSocket.wrapClientSocket(tcpSocket, sslContext, executor))
				.thenCompose(sslSocket -> PARSER_LARGE.parse(ByteBufsSupplier.of(SerialSupplier.ofSocket(sslSocket)))
						.whenComplete(($, e) -> sslSocket.close()))
				.whenComplete(assertComplete(result -> assertEquals(result, sentData.toString())));
	}

	@Test
	public void testCloseAndOperationAfterClose() throws IOException {
		startServer(sslContext, socket ->
				socket.write(wrapAscii("He"))
						.whenComplete(($, e) -> socket.close())
						.whenComplete(assertComplete())
						.thenCompose($ -> socket.write(wrapAscii("ello")))
						.whenComplete(assertFailure(e -> assertSame(CLOSE_EXCEPTION, e))));

		AsyncTcpSocketImpl.connect(ADDRESS)
				.thenApply(tcpSocket -> AsyncSslSocket.wrapClientSocket(tcpSocket, sslContext, executor))
				.whenResult(sslSocket -> {
					ByteBufsSupplier supplier = ByteBufsSupplier.of(SerialSupplier.ofSocket(sslSocket));
					PARSER.parse(supplier)
							.whenComplete(assertFailure(e -> {
								supplier.getBufs().recycle();
								assertSame(CLOSE_EXCEPTION, e);
							}));
				});
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

	private Promise<Void> sendData(AsyncTcpSocket socket) {
		String largeData = generateLargeString(10_000);
		ByteBuf largeBuf = wrapAscii(largeData);
		sentData.append(largeData);

		return socket.write(largeBuf)
				.thenCompose($ -> Promises.loop(1000, i -> i != 0,
						i -> {
							sentData.append(TEST_STRING);
							return socket.write(wrapAscii(TEST_STRING))
									.async()
									.thenApply($2 -> i - 1);
						}));
	}
	// endregion
}
