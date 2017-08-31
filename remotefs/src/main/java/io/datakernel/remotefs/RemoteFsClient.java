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

package io.datakernel.remotefs;

import com.google.gson.Gson;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public final class RemoteFsClient implements IRemoteFsClient {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop;
	protected final SocketSettings socketSettings = SocketSettings.create();

	// SSL
	protected final SSLContext sslContext;
	protected final ExecutorService sslExecutor;

	private final InetSocketAddress address;

	private MessagingSerializer<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> serializer = ofGson(getResponseGson(), RemoteFsResponses.FsResponse.class, getCommandGSON(), RemoteFsCommands.FsCommand.class);

	// creators & builders
	protected RemoteFsClient(Eventloop eventloop, InetSocketAddress address,
	                         SSLContext sslContext, ExecutorService sslExecutor) {
		this.eventloop = eventloop;
		this.address = address;
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
	}

	public static RemoteFsClient create(Eventloop eventloop, InetSocketAddress address) {
		return new RemoteFsClient(eventloop, address, null, null);
	}

	public RemoteFsClient withSslEnabled(SSLContext sslContext, ExecutorService sslExecutor) {
		return new RemoteFsClient(eventloop, address, sslContext, sslExecutor);
	}
	// endregion

	@Override
	public CompletionStage<Void> upload(final StreamProducer<ByteBuf> producer, final String fileName) {
		return connect(address).thenCompose(doUpload(producer, fileName));
	}

	private Function<MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand>, CompletionStage<Void>> doUpload(StreamProducer<ByteBuf> producer, String fileName) {
		return messaging -> {
			messaging.send(new RemoteFsCommands.Upload(fileName));
			return messaging.sendBinaryStreamFrom(producer).thenCompose($ -> {
				final SettableStage<Void> stage = SettableStage.create();
				logger.trace("send all bytes for {}", fileName);
				messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
					@Override
					public void onReceive(RemoteFsResponses.FsResponse msg) {
						logger.trace("received {}", msg);
						if (msg instanceof RemoteFsResponses.Acknowledge) {
							stage.setResult(null);
						} else if (msg instanceof RemoteFsResponses.Err) {
							stage.setError(new RemoteFsException(((RemoteFsResponses.Err) msg).msg));
						} else {
							stage.setError(new RemoteFsException("Invalid message received: " + msg));
						}
					}

					@Override
					public void onReceiveEndOfStream() {
						logger.warn("received unexpected end of stream");
						stage.setError(new RemoteFsException("Unexpected end of stream for: " + fileName));
					}

					@Override
					public void onException(Exception e) {
						stage.setError(e);
					}
				});
				return stage;
			}).whenComplete((aVoid, throwable) -> messaging.close());
		};
	}

	@Override
	public CompletionStage<StreamProducer<ByteBuf>> download(final String fileName, final long startPosition) {
		return connect(address).thenCompose(doDownload(fileName, startPosition));
	}

	private Function<MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand>, CompletionStage<StreamProducer<ByteBuf>>> doDownload(String fileName, long startPosition) {
		return messaging -> {
			final RemoteFsCommands.Download msg = new RemoteFsCommands.Download(fileName, startPosition);



			return messaging.send(msg).thenCompose(aVoid -> {
				logger.trace("command to download {} send", fileName);
				final SettableStage<StreamProducer<ByteBuf>> stage = SettableStage.create();
				messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
					@Override
					public void onReceive(RemoteFsResponses.FsResponse msg) {
						logger.trace("received {}", msg);
						if (msg instanceof RemoteFsResponses.Ready) {
							long size = ((RemoteFsResponses.Ready) msg).size;
							StreamForwarderWithCounter forwarder = StreamForwarderWithCounter.create(eventloop, size - startPosition);
							messaging.receiveBinaryStreamTo(forwarder.getInput()).whenComplete(($, throwable1) -> messaging.close());
							stage.setResult(forwarder.getOutput());
						} else if (msg instanceof RemoteFsResponses.Err) {
							stage.setError(new RemoteFsException(((RemoteFsResponses.Err) msg).msg));
						} else {
							stage.setError(new RemoteFsException("Invalid message received: " + msg));
						}
					}

					@Override
					public void onReceiveEndOfStream() {
						logger.warn("received unexpected end of stream");
						stage.setError(new RemoteFsException("Unexpected end of stream for: " + fileName));
					}

					@Override
					public void onException(Exception e) {
						stage.setError(new RemoteFsException(e));
					}
				});
				return stage;
			}).whenComplete((byteBufStreamProducer, throwable) -> {
				if (throwable != null) messaging.close();
			});
		};
	}

	@Override
	public CompletionStage<Void> delete(String fileName) {
		return connect(address).thenCompose(messaging -> doDelete(fileName, messaging));
	}

	public CompletionStage<Void> doDelete(final String fileName, final MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging) {
		final SettableStage<Void> stage = SettableStage.create();
		messaging.send(new RemoteFsCommands.Delete(fileName)).whenComplete(($, throwable) -> {
			if (throwable == null) {
				logger.trace("command to delete {} send", fileName);
				messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
					@Override
					public void onReceive(RemoteFsResponses.FsResponse msg) {
						logger.trace("received {}", msg);
						if (msg instanceof RemoteFsResponses.Ok) {
							messaging.close();
							stage.setResult(null);
						} else if (msg instanceof RemoteFsResponses.Err) {
							messaging.close();
							stage.setError(new RemoteFsException(((RemoteFsResponses.Err) msg).msg));
						} else {
							messaging.close();
							stage.setError(new RemoteFsException("Invalid message received: " + msg));
						}
					}

					@Override
					public void onReceiveEndOfStream() {
						logger.warn("received unexpected end of stream");
						messaging.close();
						stage.setError(new RemoteFsException("Unexpected end of stream for: " + fileName));
					}

					@Override
					public void onException(Exception e) {
						messaging.close();
						stage.setError(e);
					}
				});
			} else {
				messaging.close();
				stage.setError(AsyncCallbacks.throwableToException(throwable));
			}
		});
		return stage;
	}

	@Override
	public CompletionStage<List<String>> list() {
		return connect(address).thenCompose(this::doList);
	}

	public CompletionStage<List<String>> doList(final MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging) {
		final SettableStage<List<String>> stage = SettableStage.create();
		messaging.send(new RemoteFsCommands.ListFiles()).whenComplete(($, throwable) -> {
			if (throwable == null) {
				logger.trace("command to list files send");
				messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
					@Override
					public void onReceive(RemoteFsResponses.FsResponse msg) {
						logger.trace("received {}", msg);
						if (msg instanceof RemoteFsResponses.ListOfFiles) {
							messaging.close();
							stage.setResult(((RemoteFsResponses.ListOfFiles) msg).files);
						} else if (msg instanceof RemoteFsResponses.Err) {
							messaging.close();
							stage.setError(new RemoteFsException(((RemoteFsResponses.Err) msg).msg));
						} else {
							messaging.close();
							stage.setError(new RemoteFsException("Invalid message received: " + msg));
						}
					}

					@Override
					public void onReceiveEndOfStream() {
						logger.warn("received unexpected end of stream");
						messaging.close();
						stage.setError(new RemoteFsException("Unexpected end of stream while trying to list files"));
					}

					@Override
					public void onException(Exception e) {
						messaging.close();
						stage.setError(e);
					}
				});
			} else {
				messaging.close();
				stage.setError(AsyncCallbacks.throwableToException(throwable));
			}
		});
		return stage;
	}

	private CompletionStage<MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand>> connect(InetSocketAddress address) {
		return eventloop.connect(address).thenApply(socketChannel -> {
			AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
			AsyncTcpSocket asyncTcpSocket = sslContext != null
					? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor)
					: asyncTcpSocketImpl;
			MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging =
					MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocketImpl.register();
			return messaging;
		});
	}

	protected Gson getCommandGSON() {
		return RemoteFsCommands.commandGSON;
	}

	protected Gson getResponseGson() {
		return RemoteFsResponses.responseGson;
	}

}