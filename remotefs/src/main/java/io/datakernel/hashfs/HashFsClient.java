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
import io.datakernel.FsCommands;
import io.datakernel.FsResponses;
import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingExceptionHandler;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
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

	private class AliveConnectCallback implements ConnectCallback {
		private final ResultCallback<Set<Replica>> callback;

		AliveConnectCallback(ResultCallback<Set<Replica>> callback) {this.callback = callback;}

		@Override
		public void onConnect(SocketChannel socketChannel) {
			SocketConnection connection = createConnection(socketChannel)
					.addStarter(new MessagingStarter<FsCommands.FsCommand>() {
						@Override
						public void onStart(Messaging<FsCommands.FsCommand> messaging) {
							logger.trace("send command to list alive servers");
							messaging.sendMessage(new HashFsCommands.Alive());
						}
					})
					.addHandler(HashFsResponses.ListOfServers.class, new MessagingHandler<HashFsResponses.ListOfServers, FsCommands.FsCommand>() {
						@Override
						public void onMessage(HashFsResponses.ListOfServers item, Messaging<FsCommands.FsCommand> messaging) {
							logger.trace("received {} alive servers", item.servers.size());
							messaging.shutdown();
							callback.onResult(item.servers);
						}
					})
					.addHandler(FsResponses.Err.class, new MessagingHandler<FsResponses.Err, FsCommands.FsCommand>() {
						@Override
						public void onMessage(FsResponses.Err item, Messaging<FsCommands.FsCommand> messaging) {
							logger.trace("failed to list alive servers");
							messaging.shutdown();
							Exception e = new Exception(item.msg);
							callback.onException(e);
						}
					})
					.addReadExceptionHandler(new MessagingExceptionHandler() {
						@Override
						public void onException(Exception e) {
							logger.trace("caught exception while trying to list alive servers");
							callback.onException(e);
						}
					});
			connection.register();
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
	}

	private class OfferConnectCallback implements ConnectCallback {
		private final List<String> forDeletion;
		private final List<String> forUpload;
		private final ResultCallback<List<String>> callback;

		OfferConnectCallback(List<String> forDeletion, List<String> forUpload, ResultCallback<List<String>> callback) {
			this.forDeletion = forDeletion;
			this.forUpload = forUpload;
			this.callback = callback;
		}

		@Override
		public void onConnect(SocketChannel socketChannel) {
			SocketConnection connection = createConnection(socketChannel)
					.addStarter(new MessagingStarter<FsCommands.FsCommand>() {
						@Override
						public void onStart(Messaging<FsCommands.FsCommand> messaging) {
							logger.trace("send offer to download: {} files, to delete: {} files", forUpload.size(), forDeletion.size());
							messaging.sendMessage(new HashFsCommands.Announce(forDeletion, forUpload));
						}
					})
					.addHandler(FsResponses.ListOfFiles.class, new MessagingHandler<FsResponses.ListOfFiles, FsCommands.FsCommand>() {
						@Override
						public void onMessage(FsResponses.ListOfFiles item, Messaging<FsCommands.FsCommand> messaging) {
							logger.trace("received response for file offer: {} files required", item.files.size());
							messaging.shutdown();
							callback.onResult(item.files);
						}
					})
					.addHandler(FsResponses.Err.class, new MessagingHandler<FsResponses.Err, FsCommands.FsCommand>() {
						@Override
						public void onMessage(FsResponses.Err item, Messaging<FsCommands.FsCommand> messaging) {
							logger.trace("failed to receive response for file offer");
							messaging.shutdown();
							Exception e = new Exception(item.msg);
							callback.onException(e);
						}
					})
					.addReadExceptionHandler(new MessagingExceptionHandler() {
						@Override
						public void onException(Exception e) {
							logger.trace("caught exception while trying to offer files");
							callback.onException(e);
						}
					});
			connection.register();
		}

		@Override
		public void onException(Exception e) {
			callback.onException(e);
		}
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

	void announce(Replica replica, List<String> forUpload, List<String> forDeletion, ResultCallback<List<String>> callback) {
		connect(replica.getAddress(), new OfferConnectCallback(forDeletion, forUpload, callback));
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

	void alive(InetSocketAddress address, ResultCallback<Set<Replica>> callback) {
		connect(address, new AliveConnectCallback(callback));
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
