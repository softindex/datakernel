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

package io.datakernel;

import com.google.gson.Gson;
import io.datakernel.FsCommands.*;
import io.datakernel.FsResponses.*;
import io.datakernel.async.CallbackRegistry;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ExceptionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.ConnectCallback;
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

import static io.datakernel.FsCommands.*;
import static io.datakernel.FsResponses.*;
import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public abstract class FsClient<S extends FsClient<S>> {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop;
	protected final SocketSettings socketSettings = SocketSettings.create();

	// SSL
	protected final SSLContext sslContext;
	protected final ExecutorService sslExecutor;

	private MessagingSerializer<FsResponse, FsCommand> serializer = ofGson(getResponseGson(), FsResponse.class, getCommandGSON(), FsCommand.class);

	// creators & builders
	public FsClient(Eventloop eventloop, SSLContext sslContext, ExecutorService sslExecutor) {
		this.eventloop = eventloop;
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
	}

	// api
	public abstract void upload(String destinationFileName, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	public abstract void download(String fileName, long startPosition, ResultCallback<StreamProducer<ByteBuf>> callback);

	public abstract void list(ResultCallback<List<String>> callback);

	public abstract void delete(String fileName, CompletionCallback callback);

	// transport code
	protected final void doUpload(InetSocketAddress address, final String fileName, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		connect(address, new MessagingConnectCallback() {
			@Override
			public void onConnect(final MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging) {
				final Upload upload = new Upload(fileName);
				messaging.send(upload, ignoreCompletionCallback());
				messaging.sendBinaryStreamFrom(producer, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.trace("send all bytes for {}", fileName);
						messaging.receive(new ReceiveMessageCallback<FsResponse>() {
							@Override
							public void onReceive(FsResponse msg) {
								logger.trace("received {}", msg);
								if (msg instanceof Acknowledge) {
									messaging.close();
									callback.setComplete();
								} else if (msg instanceof Err) {
									messaging.close();
									callback.setException(new RemoteFsException(((Err) msg).msg));
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

	protected final void doDownload(InetSocketAddress address, final String fileName, final long startPosition,
	                                final ResultCallback<StreamProducer<ByteBuf>> callback) {
		connect(address, new MessagingConnectCallback() {
			@Override
			public void onConnect(final MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging) {
				messaging.send(new Download(fileName, startPosition), new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.trace("command to download {} send", fileName);
						messaging.receive(new ReceiveMessageCallback<FsResponse>() {
							@Override
							public void onReceive(FsResponse msg) {
								logger.trace("received {}", msg);
								if (msg instanceof Ready) {
									long size = ((Ready) msg).size;
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
								} else if (msg instanceof Err) {
									messaging.close();
									RemoteFsException exception = new RemoteFsException(((Err) msg).msg);
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

	protected final void doDelete(InetSocketAddress address, String fileName, CompletionCallback callback) {
		connect(address, new DeleteConnectCallback(fileName, callback));
	}

	protected final void doList(InetSocketAddress address, final ResultCallback<List<String>> callback) {
		connect(address, new ListConnectCallback(callback));
	}

	protected abstract class MessagingConnectCallback extends ExceptionCallback {
		public final void reportConnect(MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging) {
			CallbackRegistry.complete(this);
			onConnect(messaging);
		}

		protected abstract void onConnect(MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging);
	}

	protected void connect(InetSocketAddress address, final MessagingConnectCallback callback) {
		eventloop.connect(address, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
				AsyncTcpSocket asyncTcpSocket = sslContext != null ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
				MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging = MessagingWithBinaryStreaming.create(eventloop, asyncTcpSocket, serializer);
				asyncTcpSocket.setEventHandler(messaging);
				asyncTcpSocketImpl.register();
				callback.onConnect(messaging);
			}

			@Override
			public void onException(Exception exception) {
				callback.setException(exception);
			}
		});
	}

	protected Gson getCommandGSON() {
		return commandGSON;
	}

	protected Gson getResponseGson() {
		return responseGson;
	}

	private class DeleteConnectCallback extends MessagingConnectCallback {
		private final String fileName;
		private final CompletionCallback callback;

		DeleteConnectCallback(String fileName, CompletionCallback callback) {
			this.fileName = fileName;
			this.callback = callback;
		}

		@Override
		public void onConnect(final MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging) {
			messaging.send(new Delete(fileName), new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.trace("command to delete {} send", fileName);
					messaging.receive(new ReceiveMessageCallback<FsResponse>() {
						@Override
						public void onReceive(FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof Ok) {
								messaging.close();
								callback.setComplete();
							} else if (msg instanceof Err) {
								messaging.close();
								callback.setException(new RemoteFsException(((Err) msg).msg));
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
		public void onConnect(final MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging) {
			messaging.send(new ListFiles(), new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.trace("command to list files send");
					messaging.receive(new ReceiveMessageCallback<FsResponse>() {
						@Override
						public void onReceive(FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof ListOfFiles) {
								messaging.close();
								callback.setResult(((ListOfFiles) msg).files);
							} else if (msg instanceof Err) {
								messaging.close();
								callback.setException(new RemoteFsException(((Err) msg).msg));
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