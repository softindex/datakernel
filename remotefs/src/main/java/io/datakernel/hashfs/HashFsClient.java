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

package io.datakernel.hashfs;

import com.google.gson.Gson;
import io.datakernel.FsClient;
import io.datakernel.FsCommands.FsCommand;
import io.datakernel.FsResponses.Err;
import io.datakernel.FsResponses.FsResponse;
import io.datakernel.FsResponses.ListOfFiles;
import io.datakernel.RemoteFsException;
import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.hashfs.HashFsCommands.Alive;
import io.datakernel.hashfs.HashFsCommands.Announce;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingWithBinaryStreamingConnection;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.Preconditions.check;

public final class HashFsClient extends FsClient {
	private final List<Replica> bootstrap;

	private HashingStrategy hashing = new RendezvousHashing();
	private long baseRetryTimeout = 100L;
	private int maxRetryAttempts = 3;

	// creators & builder methods
	public HashFsClient(Eventloop eventloop, List<Replica> bootstrap) {
		super(eventloop);
		check(bootstrap != null && !bootstrap.isEmpty(), "Bootstrap list can't be empty or null");
		this.bootstrap = bootstrap;
	}

	public HashFsClient setBaseRetryTimeout(long baseRetryTimeout) {
		check(baseRetryTimeout > 0, "Base retry timeout should be positive");
		this.baseRetryTimeout = baseRetryTimeout;
		return this;
	}

	public HashFsClient setMaxRetryAttempts(int maxRetryAttempts) {
		check(maxRetryAttempts > 0, "Max retry attempts quantity should be positive");
		this.maxRetryAttempts = maxRetryAttempts;
		return this;
	}

	// establishing connection
	@Override
	protected Gson getCommandGSON() {
		return HashFsCommands.commandGSON;
	}

	@Override
	protected Gson getResponseGson() {
		return HashFsResponses.responseGSON;
	}

	// api
	@Override
	public void upload(final String fileName, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		getAliveServers(new ForwardingResultCallback<List<Replica>>(callback) {
			@Override
			public void onResult(List<Replica> result) {
				List<Replica> candidates = hashing.sortReplicas(fileName, result);
				doUpload(fileName, 0, candidates, producer, callback);
			}
		});
	}

	@Override
	public void download(final String fileName, final long startPosition, final ResultCallback<StreamTransformerWithCounter> callback) {
		getAliveServers(new ForwardingResultCallback<List<Replica>>(callback) {
			@Override
			public void onResult(List<Replica> result) {
				List<Replica> candidates = hashing.sortReplicas(fileName, result);
				doDownload(fileName, startPosition, 0, candidates, callback);
			}
		});
	}

	@Override
	public void delete(final String fileName, final CompletionCallback callback) {
		getAliveServers(new ForwardingResultCallback<List<Replica>>(callback) {
			@Override
			public void onResult(List<Replica> result) {
				List<Replica> candidates = hashing.sortReplicas(fileName, result);
				doDelete(fileName, 0, candidates, callback);
			}
		});
	}

	@Override
	public void list(final ResultCallback<List<String>> callback) {
		getAliveServers(new ForwardingResultCallback<List<Replica>>(callback) {
			@Override
			public void onResult(List<Replica> result) {
				doList(result, callback);
			}
		});
	}

	void announce(final Replica replica, final List<String> forUpload, final List<String> forDeletion, final ResultCallback<List<String>> callback) {
		connect(replica.getAddress(), new ConnectCallback() {
			@Override
			public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
				final MessagingWithBinaryStreamingConnection<FsResponse, FsCommand> messaging = getMessaging(asyncTcpSocket);
				messaging.send(new Announce(forDeletion, forUpload), new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						messaging.receive(new ReceiveMessageCallback<FsResponse>() {
							@Override
							public void onReceive(FsResponse msg) {
								logger.trace("received {}");
								if (msg instanceof ListOfFiles) {
									ListOfFiles listOfFiles = (ListOfFiles) msg;
									messaging.close();
									callback.onResult(listOfFiles.files);
								} else if (msg instanceof Err) {
									RemoteFsException e = new RemoteFsException(((Err) msg).msg);
									messaging.close();
									callback.onException(e);
								} else {
									messaging.close();
									callback.onException(new RemoteFsException("Invalid message received: " + msg));
								}
							}

							@Override
							public void onReceiveEndOfStream() {
								logger.warn("received unexpected end of stream");
								messaging.close();
								callback.onException(new RemoteFsException("Unexpected end of stream while trying to announce files"));
							}

							@Override
							public void onException(Exception e) {
								messaging.close();
								callback.onException(new RemoteFsException(e));
							}
						});
					}
				});
				return messaging;
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	void makeReplica(Replica replica, String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		super.doUpload(replica.getAddress(), fileName, producer, callback);
	}

	// inner
	private void doUpload(final String fileName, final int currentAttempt, final List<Replica> candidates,
	                      final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		Replica server = candidates.get(currentAttempt % candidates.size());
		doUpload(server.getAddress(), fileName, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				final int attempt = currentAttempt + 1;
				if (attempt < maxRetryAttempts) {
					schedule(attempt, new Runnable() {
						@Override
						public void run() {
							doUpload(fileName, attempt, candidates, producer, callback);
						}
					});
				} else {
					callback.onException(e);
				}
			}
		});
	}

	private void doDownload(final String fileName, final long startPosition, final int currentAttempt,
	                        final List<Replica> candidates, final ResultCallback<StreamTransformerWithCounter> callback) {

		Replica server = candidates.get(currentAttempt % candidates.size());
		doDownload(server.getAddress(), fileName, startPosition, new ResultCallback<StreamTransformerWithCounter>() {
			@Override
			public void onResult(StreamTransformerWithCounter result) {
				callback.onResult(result);
			}

			@Override
			public void onException(Exception e) {
				final int attempt = currentAttempt + 1;
				if (attempt < maxRetryAttempts) {
					schedule(attempt, new Runnable() {
						@Override
						public void run() {
							doDownload(fileName, startPosition, attempt, candidates, callback);
						}
					});
				} else {
					callback.onException(e);
				}
			}
		});
	}

	private void doDelete(final String fileName, final int currentAttempt, final List<Replica> candidates, final CompletionCallback callback) {
		Replica server = candidates.get(currentAttempt % candidates.size());
		doDelete(server.getAddress(), fileName, new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				final int attempt = currentAttempt + 1;
				if (attempt < maxRetryAttempts) {
					schedule(attempt, new Runnable() {
						@Override
						public void run() {
							doDelete(fileName, attempt, candidates, callback);
						}
					});
				} else {
					callback.onException(e);
				}
			}
		});
	}

	private void doList(List<Replica> servers, final ResultCallback<List<String>> callback) {
		ResultCallback<List<String>> waiter = Utils.waitAllResults(servers.size(), new Utils.Resolver<List<String>>() {
			@Override
			public void resolve(List<List<String>> results, List<Exception> e) {
				Set<String> files = new HashSet<>();
				for (List<String> fileSet : results) {
					files.addAll(fileSet);
				}
				callback.onResult(new ArrayList<>(files));
			}
		});
		for (Replica server : servers) {
			doList(server.getAddress(), waiter);
		}
	}

	void alive(InetSocketAddress address, final ResultCallback<Set<Replica>> callback) {
		connect(address, new ConnectCallback() {
			@Override
			public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
				final MessagingWithBinaryStreamingConnection<FsResponse, FsCommand> messaging = getMessaging(asyncTcpSocket);
				messaging.send(new Alive(), new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.receive(new ReceiveMessageCallback<FsResponse>() {
							@Override
							public void onReceive(FsResponse msg) {
								logger.trace("received {}", msg);
								if (msg instanceof HashFsResponses.ListOfServers) {
									callback.onResult(((HashFsResponses.ListOfServers) msg).servers);
									messaging.close();
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
								messaging.close();
								callback.onException(new RemoteFsException("Unexpected end of stream"));
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
		});
	}

	private void getAliveServers(final ResultCallback<List<Replica>> callback) {
		getAliveServers(0, callback);
	}

	private void getAliveServers(final int currentAttempt, final ResultCallback<List<Replica>> callback) {
		ResultCallback<Set<Replica>> waiter = Utils.waitAllResults(bootstrap.size(), new Utils.Resolver<Set<Replica>>() {
			@Override
			public void resolve(List<Set<Replica>> results, List<Exception> e) {
				Set<Replica> servers = new HashSet<>();
				for (Set<Replica> serverSet : results) {
					servers.addAll(serverSet);
				}
				if (servers.size() == 0 && currentAttempt < maxRetryAttempts) {
					eventloop.schedule(eventloop.currentTimeMillis() + defineRetryTime(currentAttempt), new Runnable() {
						@Override
						public void run() {
							getAliveServers(currentAttempt + 1, callback);
						}
					});
				} else if (servers.size() == 0 && currentAttempt >= maxRetryAttempts) {
					callback.onException(new Exception("Can't find working servers."));
				} else {
					callback.onResult(new ArrayList<>(servers));
				}
			}
		});
		for (Replica server : bootstrap) {
			this.alive(server.getAddress(), waiter);
		}
	}

	private long defineRetryTime(int attempt) {
		long multiplier = 2;
		for (int i = 0; i < attempt; i++) {
			multiplier *= 2;
		}
		return baseRetryTimeout * multiplier;
	}

	private void schedule(int attempt, Runnable runnable) {
		eventloop.schedule(eventloop.currentTimeMillis() + defineRetryTime(attempt), runnable);
	}
}
