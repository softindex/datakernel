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
import io.datakernel.async.*;
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
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ExecutorService;

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

	@Override
	public void upload(final String fileName, final StreamProducer<ByteBuf> producer,
	                   final CompletionCallback callback) {
		connect(address, new MessagingConnectCallback() {
			@Override
			public void onConnect(final MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging) {
				final RemoteFsCommands.Upload upload = new RemoteFsCommands.Upload(fileName);
				messaging.send(upload, IgnoreCompletionCallback.create());
				messaging.sendBinaryStreamFrom(producer, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.trace("send all bytes for {}", fileName);
						messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
							@Override
							public void onReceive(RemoteFsResponses.FsResponse msg) {
								logger.trace("received {}", msg);
								if (msg instanceof RemoteFsResponses.Acknowledge) {
									messaging.close();
									callback.setComplete();
								} else if (msg instanceof RemoteFsResponses.Err) {
									messaging.close();
									callback.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).msg));
								} else {
									messaging.close();
									callback.setException(new RemoteFsException("Invalid message received: " + msg));
								}
							}

							@Override
							public void onReceiveEndOfStream() {
								logger.warn("received unexpected end of stream");
								messaging.close();
								callback.setException(new RemoteFsException("Unexpected end of stream for: " + fileName));
							}

							@Override
							public void onException(Exception e) {
								messaging.close();
								callback.setException(e);
							}
						});
					}

					@Override
					public void onException(Exception e) {
						messaging.close();
						callback.setException(e);
					}
				});
			}

			@Override
			public void onException(Exception exception) {
				callback.setException(exception);
			}
		});
	}

	@Override
	public void download(final String fileName, final long startPosition,
	                     final ResultCallback<StreamProducer<ByteBuf>> callback) {
		connect(address, new MessagingConnectCallback() {
			@Override
			public void onConnect(final MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging) {
				messaging.send(new RemoteFsCommands.Download(fileName, startPosition), new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.trace("command to download {} send", fileName);
						messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
							@Override
							public void onReceive(RemoteFsResponses.FsResponse msg) {
								logger.trace("received {}", msg);
								if (msg instanceof RemoteFsResponses.Ready) {
									long size = ((RemoteFsResponses.Ready) msg).size;
									StreamForwarderWithCounter forwarder = StreamForwarderWithCounter.create(eventloop, size - startPosition);
									messaging.receiveBinaryStreamTo(forwarder.getInput(), new CompletionCallback() {
										@Override
										public void onComplete() {
											messaging.close();
										}

										@Override
										public void onException(Exception e) {
											messaging.close();
										}
									});
									callback.setResult(forwarder.getOutput());
								} else if (msg instanceof RemoteFsResponses.Err) {
									messaging.close();
									RemoteFsException exception = new RemoteFsException(((RemoteFsResponses.Err) msg).msg);
									callback.setException(exception);
								} else {
									messaging.close();
									RemoteFsException exception = new RemoteFsException("Invalid message received: " + msg);
									callback.setException(exception);
								}
							}

							@Override
							public void onReceiveEndOfStream() {
								logger.warn("received unexpected end of stream");
								messaging.close();
								RemoteFsException exception = new RemoteFsException("Unexpected end of stream for: " + fileName);
								callback.setException(exception);
							}

							@Override
							public void onException(Exception e) {
								messaging.close();
								callback.setException(new RemoteFsException(e));
							}

						});
					}

					@Override
					public void onException(Exception e) {
						messaging.close();
						callback.setException(new RemoteFsException(e));
					}
				});
			}

			@Override
			public void onException(Exception e) {
				callback.setException(e);
			}
		});
	}

	@Override
	public void delete(String fileName, CompletionCallback callback) {
		connect(address, new DeleteConnectCallback(fileName, callback));
	}

	@Override
	public void list(final ResultCallback<List<String>> callback) {
		connect(address, new ListConnectCallback(callback));
	}

	private abstract class MessagingConnectCallback extends ExceptionCallback {
		final void setConnect(MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging) {
			CallbackRegistry.complete(this);
			onConnect(messaging);
		}

		protected abstract void onConnect(MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging);
	}

	private void connect(InetSocketAddress address, final MessagingConnectCallback callback) {
		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
				AsyncTcpSocket asyncTcpSocket = sslContext != null ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
				MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocketImpl.register();
				callback.setConnect(messaging);
			}

			@Override
			public void onException(Exception exception) {
				callback.setException(exception);
			}
		});
	}

	protected Gson getCommandGSON() {
		return RemoteFsCommands.commandGSON;
	}

	protected Gson getResponseGson() {
		return RemoteFsResponses.responseGson;
	}

	private class DeleteConnectCallback extends MessagingConnectCallback {
		private final String fileName;
		private final CompletionCallback callback;

		DeleteConnectCallback(String fileName, CompletionCallback callback) {
			this.fileName = fileName;
			this.callback = callback;
		}

		@Override
		public void onConnect(final MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging) {
			messaging.send(new RemoteFsCommands.Delete(fileName), new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.trace("command to delete {} send", fileName);
					messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
						@Override
						public void onReceive(RemoteFsResponses.FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof RemoteFsResponses.Ok) {
								messaging.close();
								callback.setComplete();
							} else if (msg instanceof RemoteFsResponses.Err) {
								messaging.close();
								callback.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).msg));
							} else {
								messaging.close();
								callback.setException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							callback.setException(new RemoteFsException("Unexpected end of stream for: " + fileName));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							callback.setException(e);
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.setException(e);
				}
			});
		}

		@Override
		public void onException(Exception e) {
			callback.setException(e);
		}
	}

	private class ListConnectCallback extends MessagingConnectCallback {
		private final ResultCallback<List<String>> callback;

		ListConnectCallback(ResultCallback<List<String>> callback) {
			this.callback = callback;
		}

		@Override
		public void onConnect(final MessagingWithBinaryStreaming<RemoteFsResponses.FsResponse, RemoteFsCommands.FsCommand> messaging) {
			messaging.send(new RemoteFsCommands.ListFiles(), new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.trace("command to list files send");
					messaging.receive(new ReceiveMessageCallback<RemoteFsResponses.FsResponse>() {
						@Override
						public void onReceive(RemoteFsResponses.FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof RemoteFsResponses.ListOfFiles) {
								messaging.close();
								callback.setResult(((RemoteFsResponses.ListOfFiles) msg).files);
							} else if (msg instanceof RemoteFsResponses.Err) {
								messaging.close();
								callback.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).msg));
							} else {
								messaging.close();
								callback.setException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							callback.setException(new RemoteFsException("Unexpected end of stream while trying to list files"));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							callback.setException(e);
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.setException(e);
				}
			});
		}

		@Override
		public void onException(Exception e) {
			callback.setException(e);
		}
	}

}