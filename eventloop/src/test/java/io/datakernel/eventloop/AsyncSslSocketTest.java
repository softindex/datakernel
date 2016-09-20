/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.Executor;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

public class AsyncSslSocketTest {
	// <editor-fold desc="fields">
	private static final String KEYSTORE_PATH = "./src/test/resources/keystore.jks";
	private static final String KEYSTORE_PASS = "testtest";
	private static final String KEY_PASS = "testtest";

	private static final String TRUSTSTORE_PATH = "./src/test/resources/truststore.jks";
	private static final String TRUSTSTORE_PASS = "testtest";

	private Eventloop eventloop = Eventloop.create();
	private AsyncSslSocket serverSslSocket;
	private AsyncSslSocket clientSslSocket;

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private EventHandler clientEventHandler = context.mock(EventHandler.class, "clientEventHandler");
	private EventHandler serverEventHandler = context.mock(EventHandler.class, "serverEventHandler");
	private AsyncTcpSocketStub clientSocketStub;
	private AsyncTcpSocketStub serverSocketStub;
	// </editor-fold>

	// <editor-fold desc="initialization">
	@Before
	public void init() throws Exception {
		Executor executor = new ExecutorStub();
		KeyManager[] keyManagers = createKeyManagers(new File(KEYSTORE_PATH), KEYSTORE_PASS, KEY_PASS);
		TrustManager[] trustManagers = createTrustManagers(new File(TRUSTSTORE_PATH), TRUSTSTORE_PASS);
		SSLContext sslContext = createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom());

		SSLEngine serverSSLEngine = sslContext.createSSLEngine();
		serverSSLEngine.setUseClientMode(false);
		serverSocketStub = new AsyncTcpSocketStub("server", eventloop);
		serverSslSocket = AsyncSslSocket.create(eventloop, serverSocketStub, serverSSLEngine, executor);
		serverSocketStub.setEventHandler(serverSslSocket);
		serverSslSocket.setEventHandler(serverEventHandler);

		SSLEngine clientSSLEngine = sslContext.createSSLEngine();
		clientSSLEngine.setUseClientMode(true);
		clientSocketStub = new AsyncTcpSocketStub("client", eventloop);
		clientSslSocket = AsyncSslSocket.create(eventloop, clientSocketStub, clientSSLEngine, executor);
		clientSocketStub.setEventHandler(clientSslSocket);
		clientSslSocket.setEventHandler(clientEventHandler);

		// connect client and server stub sockets directly
		clientSocketStub.connect(serverSocketStub);
	}
	// </editor-fold>

	// <editor-fold desc="tests">
	@Test
	public void performsSimpleMessageExchange() throws NoSuchAlgorithmException {
		context.checking(new Expectations() {{
			oneOf(serverEventHandler).onRead(with(bytebufOfMessage("Hello")));
			oneOf(clientEventHandler).onRead(with(bytebufOfMessage("World")));

			allowing(serverEventHandler).onReadEndOfStream();
			allowing(serverEventHandler).onWrite();
			allowing(clientEventHandler).onReadEndOfStream();
			allowing(clientEventHandler).onWrite();

			allowing(serverEventHandler).onRegistered();
			allowing(clientEventHandler).onRegistered();
		}});

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				serverSslSocket.onRegistered();
				clientSslSocket.onRegistered();

				serverSslSocket.read();
				clientSslSocket.read();

				clientSslSocket.write(createByteBufFromString("Hello"));
				serverSslSocket.write(createByteBufFromString("World"));
			}
		});

		eventloop.run();

		System.out.println("created: " + getCreatedItems());
		System.out.println("in pool: " + getPoolItems());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void sendsLargeAmountOfDataFromClientToServer() {
		final StringBuilder sentData = new StringBuilder();
		EventHandlerDataAccumulator serverDataAccumulator = new EventHandlerDataAccumulator(serverSslSocket);
		serverSslSocket.setEventHandler(serverDataAccumulator);

		context.checking(new Expectations() {{
			ignoring(clientEventHandler);
		}});

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				serverSslSocket.onRegistered();
				clientSslSocket.onRegistered();

				serverSslSocket.read();

				// send large message
				String largeMessage = generateLargeString(100_000);
				sentData.append(largeMessage);
				clientSslSocket.write(createByteBufFromString(largeMessage));

				// send lots of small messages
				String smallMsg = "data_012345";
				for (int i = 0; i < 25_000; i++) {
					sentData.append(smallMsg);
					clientSslSocket.write(createByteBufFromString(smallMsg));
				}
			}
		});

		eventloop.run();

		assertThat("received bytes amount", serverDataAccumulator.getAccumulatedData().length(), greaterThan(0));
		assertEquals(sentData.toString(), serverDataAccumulator.getAccumulatedData());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void sendsLargeAmountOfDataFromServerToClient() {
		final StringBuilder sentData = new StringBuilder();
		EventHandlerDataAccumulator clientDataAccumulator = new EventHandlerDataAccumulator(clientSslSocket);
		clientSslSocket.setEventHandler(clientDataAccumulator);

		context.checking(new Expectations() {{
			ignoring(serverEventHandler);
		}});

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				serverSslSocket.onRegistered();
				clientSslSocket.onRegistered();

				clientSslSocket.read();

				// send large message
				String largeMessage = generateLargeString(100_000);
				sentData.append(largeMessage);
				serverSslSocket.write(createByteBufFromString(largeMessage));

				// send lots of small messages
				String smallMsg = "data_012345";
				for (int i = 0; i < 25_000; i++) {
					sentData.append(smallMsg);
					serverSslSocket.write(createByteBufFromString(smallMsg));
				}
			}
		});

		eventloop.run();

		assertTrue(clientDataAccumulator.getAccumulatedData().length() > 0);
		assertEquals(sentData.toString(), clientDataAccumulator.getAccumulatedData());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void getsSSLExceptionWhenOtherSideWasClosedWithoutSpecifiedHandshakeMessage() {
		context.checking(new Expectations() {{
			// check first messages
			oneOf(clientEventHandler).onRead(with(bytebufOfMessage("World")));
			oneOf(serverEventHandler).onRead(with(bytebufOfMessage("Hello")));
			// check error
			oneOf(clientEventHandler).onClosedWithError(with(any(SSLException.class)));

			allowing(serverEventHandler).onWrite();
			allowing(clientEventHandler).onWrite();

			allowing(clientEventHandler).onRegistered();
			allowing(serverEventHandler).onRegistered();
			allowing(serverEventHandler).onClosedWithError(with(any(Exception.class)));
		}});

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				serverSslSocket.onRegistered();
				clientSslSocket.onRegistered();

				serverSslSocket.read();
				clientSslSocket.read();

				clientSslSocket.write(createByteBufFromString("Hello"));
				serverSslSocket.write(createByteBufFromString("World"));

				eventloop.schedule(eventloop.currentTimeMillis() + 100, new Runnable() {
					@Override
					public void run() {
						// write endOfStream directly to client stub socket
						clientSocketStub.onReadEndOfStream();
					}
				});
			}
		});

		eventloop.run();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void otherSideEventHandler_ReceivesEndOfStream_InCaseOfProperClosing() {
		context.checking(new Expectations() {{
			// check first messages
			oneOf(serverEventHandler).onRead(with(bytebufOfMessage("Hello")));
			oneOf(clientEventHandler).onRead(with(bytebufOfMessage("World")));
			// check error
			oneOf(serverEventHandler).onReadEndOfStream();

			allowing(clientEventHandler).onRegistered();
			allowing(serverEventHandler).onRegistered();
			allowing(clientEventHandler).onWrite();
			allowing(serverEventHandler).onWrite();
		}});

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				serverSslSocket.onRegistered();
				clientSslSocket.onRegistered();

				serverSslSocket.read();
				clientSslSocket.read();

				clientSslSocket.write(createByteBufFromString("Hello"));
				serverSslSocket.write(createByteBufFromString("World"));
				eventloop.schedule(eventloop.currentTimeMillis() + 100, new Runnable() {
					@Override
					public void run() {
						clientSslSocket.close();
					}
				});
			}
		});

		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}
	// </editor-fold>

	// <editor-fold desc="stub classes">
	public static final class AsyncTcpSocketStub implements AsyncTcpSocket {
		private String desc;

		private Eventloop eventloop;

		private AsyncTcpSocketStub otherSide;
		private EventHandler downstreamEventHandler;

		private boolean writeEndOfStream = false;

		public void connect(AsyncTcpSocketStub otherSide) {
			this.otherSide = otherSide;
			otherSide.otherSide = this;
		}

		public AsyncTcpSocketStub(String desc, Eventloop eventloop) {
			this.desc = desc;
			this.eventloop = eventloop;
		}

		public void onRead(ByteBuf buf) {
			downstreamEventHandler.onRead(buf);
		}

		public void onReadEndOfStream() {
			downstreamEventHandler.onReadEndOfStream();
		}

		@Override
		public void setEventHandler(EventHandler eventHandler) {
			this.downstreamEventHandler = eventHandler;
		}

		@Override
		public void read() {
		}

		@Override
		public void write(final ByteBuf buf) {
			assert !writeEndOfStream;

			if (otherSide == null) {
				buf.recycle();
				return;
			}

			final AsyncTcpSocketStub cached = otherSide;

			eventloop.postLater(new Runnable() {
				@Override
				public void run() {
					cached.onRead(buf);
				}
			});
			downstreamEventHandler.onWrite();
		}

		@Override
		public void writeEndOfStream() {
			assert !writeEndOfStream;

			final AsyncTcpSocketStub cached = otherSide;
			writeEndOfStream = true;
			eventloop.postLater(new Runnable() {
				@Override
				public void run() {
					cached.onReadEndOfStream();
				}
			});
		}

		@Override
		public void close() {
			if (otherSide != null) {
				otherSide.otherSide = null;
				otherSide = null;
			}
		}

		@Override
		public InetSocketAddress getRemoteSocketAddress() {
			return null;
		}

		@Override
		public String toString() {
			return "desc: " + desc;
		}
	}

	public static final class ExecutorStub implements Executor {

		@Override
		public void execute(Runnable command) {
			command.run();
		}
	}

	public static final class EventHandlerDataAccumulator implements EventHandler {
		StringBuilder data = new StringBuilder();
		AsyncTcpSocket upstream;

		public EventHandlerDataAccumulator(AsyncTcpSocket upstream) {
			this.upstream = upstream;
		}

		@Override
		public void onRegistered() {

		}

		@Override
		public void onRead(ByteBuf buf) {
			data.append(extractMessageFromByteBuf(buf));
			upstream.read();
		}

		@Override
		public void onReadEndOfStream() {

		}

		@Override
		public void onWrite() {

		}

		@Override
		public void onClosedWithError(Exception e) {

		}

		public String getAccumulatedData() {
			return data.toString();
		}
	}
	// </editor-fold>

	// <editor-fold desc="helper methods">
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

	public static ByteBuf createByteBufFromString(String message) {
		return ByteBuf.wrapForReading(message.getBytes());
	}

	public static String extractMessageFromByteBuf(ByteBuf buf) {
		String result = new String(buf.array(), buf.head(), buf.headRemaining());
		buf.recycle();
		return result;
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
	// </editor-fold>

	// <editor-fold desc="custom matchers">
	public static Matcher<ByteBuf> bytebufOfMessage(final String message) {
		return new BaseMatcher<ByteBuf>() {
			@Override
			public boolean matches(Object item) {
				String extractedMessage = extractMessageFromByteBuf((ByteBuf) item);
				return extractedMessage.equals(message);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("Message: " + message);
			}
		};
	}
	// </editor-fold>
}
