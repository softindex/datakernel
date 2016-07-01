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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreamingConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import static io.datakernel.FsCommands.*;
import static io.datakernel.FsResponses.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public abstract class FsClient {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop;
	private MessagingSerializer<FsResponse, FsCommand> serializer = ofGson(getResponseGson(), FsResponse.class, getCommandGSON(), FsCommand.class);

	private SocketSettings socketSettings = SocketSettings.defaultSocketSettings();

	// creators & builders
	public FsClient(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	// api
	public abstract void upload(String destinationFileName, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	public abstract void download(String sourceFileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback);

	public abstract void list(ResultCallback<List<String>> callback);

	public abstract void delete(String fileName, CompletionCallback callback);

	// transport code
	protected final void doUpload(InetSocketAddress address, String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		connect(address, new UploadConnectCallback(fileName, callback, producer));
	}

	protected final void doDownload(InetSocketAddress address, String fileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback) {
		connect(address, new DownloadConnectCallback(fileName, startPosition, callback));
	}

	protected final void doDelete(InetSocketAddress address, String fileName, CompletionCallback callback) {
		connect(address, new DeleteConnectCallback(fileName, callback));
	}

	protected final void doList(InetSocketAddress address, final ResultCallback<List<String>> callback) {
		connect(address, new ListConnectCallback(callback));
	}

	protected Gson getCommandGSON() {return commandGSON;}

	protected Gson getResponseGson() {return responseGson;}

	protected MessagingWithBinaryStreamingConnection<FsResponse, FsCommand> getMessaging(AsyncTcpSocketImpl asyncTcpSocket) {
		return new MessagingWithBinaryStreamingConnection<>(eventloop, asyncTcpSocket, serializer);
	}

	protected final void connect(SocketAddress address, ConnectCallback callback) {
		eventloop.connect(address, socketSettings, 0, callback);
	}

	private class UploadConnectCallback implements ConnectCallback {
		private final String fileName;
		private final CompletionCallback callback;
		private final StreamProducer<ByteBuf> producer;

		UploadConnectCallback(String fileName, CompletionCallback callback, StreamProducer<ByteBuf> producer) {
			this.fileName = fileName;
			this.callback = callback;
			this.producer = producer;
		}

		@Override
		public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
			socketSettings.applyReadWriteTimeoutsTo(asyncTcpSocket);
			final MessagingWithBinaryStreamingConnection<FsResponse, FsCommand> messaging = getMessaging(asyncTcpSocket);
			final Upload upload = new Upload(fileName);
			messaging.send(upload, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.trace("send {}", upload);
					messaging.receive(new ReceiveMessageCallback<FsResponse>() {
						@Override
						public void onReceive(FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof Ok) {
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
													callback.onComplete();
												} else if (msg instanceof Err) {
													messaging.close();
													callback.onException(new RemoteFsException(((Err) msg).msg));
												} else {
													messaging.close();
													callback.onException(new RemoteFsException("Invalid message received: " + msg));
												}
											}

											@Override
											public void onReceiveEndOfStream() {
												logger.warn("received unexpected end of stream");
												messaging.close();
												callback.onException(new RemoteFsException("Unexpected end of stream for: " + fileName));
											}

											@Override
											public void onException(Exception e) {
												messaging.close();
												callback.onException(e);
											}
										});
									}

									@Override
									public void onException(Exception e) {
										messaging.close();
										callback.onException(e);
									}
								});
							} else {
								messaging.close();
								if (msg instanceof Err) {
									callback.onException(new RemoteFsException(((Err) msg).msg));
								} else {
									callback.onException(new RemoteFsException("Invalid message received: " + msg));
								}
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							callback.onException(new RemoteFsException("Unexpected end of stream for: " + fileName));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							callback.onException(e);
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.onException(e);
				}
			});
			return messaging;
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private class DownloadConnectCallback implements ConnectCallback {
		private final String fileName;
		private final long startPosition;
		private final ResultCallback<StreamTransformerWithCounter> callback;

		DownloadConnectCallback(String fileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback) {
			this.fileName = fileName;
			this.startPosition = startPosition;
			this.callback = callback;
		}

		@Override
		public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
			final MessagingWithBinaryStreamingConnection<FsResponse, FsCommand> messaging = getMessaging(asyncTcpSocket);
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
								StreamTransformerWithCounter stream = new StreamTransformerWithCounter(eventloop, size - startPosition);
								messaging.receiveBinaryStreamTo(stream.getInput(), new CompletionCallback() {
									@Override
									public void onComplete() {
										messaging.close();
									}

									@Override
									public void onException(Exception e) {
										messaging.close();
										callback.onException(e);
									}
								});
								callback.onResult(stream);
							} else if (msg instanceof Err) {
								messaging.close();
								callback.onException(new RemoteFsException(((Err) msg).msg));
							} else {
								messaging.close();
								callback.onException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							callback.onException(new RemoteFsException("Unexpected end of stream for: " + fileName));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							callback.onException(new RemoteFsException(e));
						}

					});
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.onException(new RemoteFsException(e));
				}
			});
			return messaging;
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private class DeleteConnectCallback implements ConnectCallback {
		private final String fileName;
		private final CompletionCallback callback;

		DeleteConnectCallback(String fileName, CompletionCallback callback) {
			this.fileName = fileName;
			this.callback = callback;
		}

		@Override
		public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
			final MessagingWithBinaryStreamingConnection<FsResponse, FsCommand> messaging = getMessaging(asyncTcpSocket);
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
								callback.onComplete();
							} else if (msg instanceof Err) {
								messaging.close();
								callback.onException(new RemoteFsException(((Err) msg).msg));
							} else {
								messaging.close();
								callback.onException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							callback.onException(new RemoteFsException("Unexpected end of stream for: " + fileName));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							callback.onException(e);
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.onException(e);
				}
			});
			return messaging;
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private class ListConnectCallback implements ConnectCallback {
		private final ResultCallback<List<String>> callback;

		ListConnectCallback(ResultCallback<List<String>> callback) {this.callback = callback;}

		@Override
		public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
			final MessagingWithBinaryStreamingConnection<FsResponse, FsCommand> messaging = getMessaging(asyncTcpSocket);
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
								callback.onResult(((ListOfFiles) msg).files);
							} else if (msg instanceof Err) {
								messaging.close();
								callback.onException(new RemoteFsException(((Err) msg).msg));
							} else {
								messaging.close();
								callback.onException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							callback.onException(new RemoteFsException("Unexpected end of stream while trying to list files"));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							callback.onException(e);
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.close();
					callback.onException(e);
				}
			});
			return messaging;
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}
}