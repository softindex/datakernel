package io.datakernel.eventloop;

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

import static io.datakernel.async.Cancellable.CLOSE_EXCEPTION;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serial.ByteBufsSupplier.of;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AsyncSslSocketTest {
	// region fields
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();
	private static final String KEYSTORE_PATH = "./src/test/resources/keystore.jks";
	private static final String KEYSTORE_PASS = "testtest";
	private static final String KEY_PASS = "testtest";

	private static final String TRUSTSTORE_PATH = "./src/test/resources/truststore.jks";
	private static final String TRUSTSTORE_PASS = "testtest";
	private static final String TEST_STRING = "Hello world";

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 37832);
	private SimpleServer server;
	private SSLContext sslContext;
	private static final ByteBufsParser<String> PARSER = ByteBufsParser.ofFixedSize(TEST_STRING.length())
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);

	public static final int LENGTH = 10_000 + TEST_STRING.length() * 1_000;
	private static final ByteBufsParser<String> PARSER_LARGE = ByteBufsParser.ofFixedSize(LENGTH)
			.andThen(ByteBuf::asArray)
			.andThen(ByteBufStrings::decodeAscii);
	private StringBuilder sentData;

	// endregion

	// region initialization
	@Before
	public void init() throws Exception {
		KeyManager[] keyManagers = createKeyManagers(new File(KEYSTORE_PATH), KEYSTORE_PASS, KEY_PASS);
		TrustManager[] trustManagers = createTrustManagers(new File(TRUSTSTORE_PATH), TRUSTSTORE_PASS);
		sslContext = createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom());
		sentData = new StringBuilder();
	}
	// endregion

	@Test
	public void testWrite() throws IOException {
		server = SimpleServer.create(eventloop,
				serverSsl -> PARSER.parse(of(serverSsl.reader()))
						.whenComplete(assertComplete(result -> {
							System.out.println(result);
							assertEquals(TEST_STRING, result);
						}))
						.thenRunEx(serverSsl::close))
				.withSslListenAddress(sslContext, new ExecutorServiceStub(), ADDRESS)
				.withAcceptOnce(true);

		server.listen();

		eventloop.connect(ADDRESS)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl clientTcp = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					AsyncSslSocket clientSsl = AsyncSslSocket.wrapClientSocket(clientTcp, sslContext, Runnable::run);

					clientSsl.write(wrapAscii(TEST_STRING))
							.thenRunEx(clientSsl::close);
				});

		eventloop.run();
	}

	@Test
	public void testRead() throws IOException {
		server = SimpleServer.create(eventloop,
				serverSsl -> serverSsl.write(wrapAscii(TEST_STRING))
						.whenComplete(assertComplete()))
				.withSslListenAddress(sslContext, new ExecutorServiceStub(), ADDRESS)
				.withAcceptOnce(true);

		server.listen();

		eventloop.connect(ADDRESS)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl clientTcp = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					AsyncSslSocket clientSsl = AsyncSslSocket.wrapClientSocket(clientTcp, sslContext, Runnable::run);

					ByteBufsSupplier supplier = of(clientSsl.reader());
					PARSER.parse(supplier)
							.whenComplete(assertComplete(result -> {
								System.out.println(result);
								assertEquals(TEST_STRING, result);
							}))
							.thenRunEx(clientSsl::close);
				});

		eventloop.run();
	}

	@Test
	public void testLoopBack() throws IOException {
		server = SimpleServer.create(eventloop,
				serverSsl -> PARSER.parse(of(serverSsl.reader()))
						.thenCompose(result -> {
							assertEquals(TEST_STRING, result);
							return serverSsl.write(wrapAscii(result));
						})
						.whenComplete(assertComplete())
						.thenRunEx(serverSsl::close))
				.withSslListenAddress(sslContext, new ExecutorServiceStub(), ADDRESS)
				.withAcceptOnce(true);

		server.listen();

		eventloop.connect(ADDRESS)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl clientTcp = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					AsyncSslSocket clientSsl = AsyncSslSocket.wrapClientSocket(clientTcp, sslContext, Runnable::run);

					clientSsl.write(wrapAscii(TEST_STRING))
							.thenCompose($ -> {
								ByteBufsSupplier supplier = of(clientSsl.reader());
								return PARSER.parse(supplier);
							})
							.whenComplete(assertComplete(result -> {
								System.out.println(result);
								assertEquals(TEST_STRING, result);
							}))
							.thenRunEx(clientSsl::close);

				});

		eventloop.run();
	}

	@Test
	public void sendsLargeAmountOfDataFromClientToServer() throws IOException {
		server = SimpleServer.create(eventloop,
				serverSsl -> PARSER_LARGE.parse(of(serverSsl.reader()))
						.whenComplete(assertComplete(result -> {
							assertEquals(LENGTH, result.length());
							assertEquals(result, sentData.toString());
						}))
						.thenRunEx(serverSsl::close))
				.withSslListenAddress(sslContext, new ExecutorServiceStub(), ADDRESS)
				.withAcceptOnce(true);

		server.listen();

		eventloop.connect(ADDRESS)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl clientTcp = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					AsyncSslSocket clientSsl = AsyncSslSocket.wrapClientSocket(clientTcp, sslContext, Runnable::run);

					sendData(clientSsl)
							.whenComplete(assertComplete())
							.thenRunEx(clientSsl::close);
				});

		eventloop.run();
	}

	@Test
	public void sendsLargeAmountOfDataFromServerToClient() throws IOException {
		server = SimpleServer.create(eventloop,
				serverSsl -> sendData(serverSsl)
						.whenComplete(assertComplete())
						.thenRunEx(serverSsl::close))
				.withSslListenAddress(sslContext, new ExecutorServiceStub(), ADDRESS)
				.withAcceptOnce(true);

		server.listen();

		eventloop.connect(ADDRESS)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl clientTcp = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					AsyncSslSocket clientSsl = AsyncSslSocket.wrapClientSocket(clientTcp, sslContext, Runnable::run);
					ByteBufsSupplier supplier = of(clientSsl.reader());
					PARSER_LARGE.parse(supplier)
							.whenComplete(assertComplete(result -> {
								assertEquals(LENGTH, result.length());
								assertEquals(result, sentData.toString());
							}))
							.thenRunEx(clientSsl::close);
				});

		eventloop.run();
	}

	@Test
	public void testClose() throws IOException {
		server = SimpleServer.create(eventloop,
				socket -> socket.write(wrapAscii("He"))
						.thenRun(socket::close))
				.withSslListenAddress(sslContext, new ExecutorServiceStub(), ADDRESS)
				.withAcceptOnce(true);

		server.listen();

		eventloop.connect(ADDRESS)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl clientTcp = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					AsyncSslSocket clientSsl = AsyncSslSocket.wrapClientSocket(clientTcp, sslContext, Runnable::run);

					ByteBufsSupplier supplier = ByteBufsSupplier.of(clientSsl.reader());
					PARSER.parse(supplier)
							.whenComplete(assertFailure(e -> {
								supplier.closeWithError(e);
								assertSame(CLOSE_EXCEPTION, e);
							}));
				});

		eventloop.run();
	}

	// region helper methods
	public static TrustManager[] createTrustManagers(File path, String pass) throws Exception {
		KeyStore trustStore = KeyStore.getInstance("JKS");

		try (InputStream trustStoreIS = new FileInputStream(path)) {
			trustStore.load(trustStoreIS, pass.toCharArray());
		}
		TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustFactory.init(trustStore);
		return trustFactory.getTrustManagers();
	}

	public static KeyManager[] createKeyManagers(File path, String storePass, String keyPass) throws Exception {
		KeyStore store = KeyStore.getInstance("JKS");
		try (InputStream is = new FileInputStream(path)) {
			store.load(is, storePass.toCharArray());
		}
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(store, keyPass.toCharArray());
		return kmf.getKeyManagers();
	}

	public static SSLContext createSslContext(String algorithm, KeyManager[] keyManagers, TrustManager[] trustManagers,
			SecureRandom secureRandom) throws NoSuchAlgorithmException, KeyManagementException {
		SSLContext instance = SSLContext.getInstance(algorithm);
		instance.init(keyManagers, trustManagers, secureRandom);
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

	private Stage<Void> sendData(AsyncTcpSocket socket) {
		String largeData = generateLargeString(10_000);
		ByteBuf largeBuf = wrapAscii(largeData);
		sentData.append(largeData);

		return socket.write(largeBuf)
				.thenCompose($ -> Stages.loop(1000, i -> i != 0,
						i -> {
							sentData.append(TEST_STRING);
							return socket.write(wrapAscii(TEST_STRING))
									.async()
									.thenApply($2 -> i - 1);
						}));
	}
	// endregion

	// region ExecutorServiceStub
	@SuppressWarnings({"ReturnOfNull", "ConstantConditions"}) // stub class
	private static class ExecutorServiceStub extends AbstractExecutorService {
		@Override
		public void execute(Runnable command) {
			command.run();
		}

		// NOP below
		@Override
		public void shutdown() {}

		@Override
		public List<Runnable> shutdownNow() {return null;}

		@Override
		public boolean isShutdown() {return false;}

		@Override
		public boolean isTerminated() {return false;}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) {return false;}
	}
	// endregion
}
