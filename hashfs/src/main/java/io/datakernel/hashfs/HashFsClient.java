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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.RfsConfig;
import io.datakernel.remotefs.ServerInfo;
import io.datakernel.remotefs.protocol.ClientProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HashFsClient implements FsClient {
	private final NioEventloop eventloop;

	private final ClientProtocol protocol;
	private final HashingStrategy hashing;

	private final List<ServerInfo> bootstrap;
	private final long baseRetryTimeout;
	private final int maxRetryAttempts;

	private HashFsClient(NioEventloop eventloop, ClientProtocol protocol, HashingStrategy hashing,
	                     List<ServerInfo> bootstrap, long baseRetryTimeout, int maxRetryAttempts) {
		this.eventloop = eventloop;
		this.protocol = protocol;
		this.hashing = hashing;
		this.bootstrap = bootstrap;
		this.baseRetryTimeout = baseRetryTimeout;
		this.maxRetryAttempts = maxRetryAttempts;
	}

	public static FsClient createInstance(NioEventloop eventloop, ClientProtocol protocol, HashingStrategy hashing,
	                                      List<ServerInfo> bootstrap, RfsConfig config) {
		return new HashFsClient(eventloop, protocol, hashing, bootstrap,
				config.getBaseRetryTimeout(),
				config.getMaxRetryAttempts());
	}

	@Override
	public void upload(final String destinationFileName, final StreamProducer<ByteBuf> producer,
	                   final CompletionCallback callback) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				List<ServerInfo> candidates = hashing.sortServers(destinationFileName, result);
				upload(destinationFileName, 0, candidates, producer, callback);
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void download(final String sourceFileName, final StreamConsumer<ByteBuf> consumer) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				List<ServerInfo> candidates = hashing.sortServers(sourceFileName, result);
				download(sourceFileName, 0, candidates, consumer);
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
			}
		});
	}

	@Override
	public void delete(final String fileName, final CompletionCallback callback) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				List<ServerInfo> candidates = hashing.sortServers(fileName, result);
				delete(fileName, 0, candidates, callback);
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void list(final ResultCallback<List<String>> callback) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				list(result, callback);
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	private void upload(final String fileName, final int currentAttempt, final List<ServerInfo> candidates,
	                    final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		ServerInfo server = candidates.get(currentAttempt % candidates.size());
		protocol.upload(server, fileName, producer, new CompletionCallback() {
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
							upload(fileName, attempt, candidates, producer, callback);
						}
					});
				} else {
					callback.onException(e);
				}
			}
		});
	}

	private void download(final String fileName, final int currentAttempt, final List<ServerInfo> candidates,
	                      final StreamConsumer<ByteBuf> consumer) {
		final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		ServerInfo server = candidates.get(currentAttempt % candidates.size());
		protocol.download(server, fileName, forwarder.getInput(), new CompletionCallback() {
			@Override
			public void onComplete() {
				forwarder.getOutput().streamTo(consumer);
			}

			@Override
			public void onException(Exception e) {
				final int attempt = currentAttempt + 1;
				if (attempt < maxRetryAttempts) {
					schedule(attempt, new Runnable() {
						@Override
						public void run() {
							download(fileName, attempt, candidates, consumer);
						}
					});
				} else {
					StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
				}
			}
		});
	}

	private void delete(final String fileName, final int currentAttempt, final List<ServerInfo> candidates,
	                    final CompletionCallback callback) {
		ServerInfo server = candidates.get(currentAttempt % candidates.size());
		protocol.delete(server, fileName, new CompletionCallback() {
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
							delete(fileName, attempt, candidates, callback);
						}
					});
				} else {
					callback.onException(e);
				}
			}
		});
	}

	private void list(List<ServerInfo> servers, final ResultCallback<List<String>> callback) {
		ResultCallback<Set<String>> waiter = Util.waitAnyResults(servers.size(), new Util.Resolver<Set<String>>() {
			@Override
			public void resolve(List<Set<String>> results, List<Exception> exceptions) {
				Set<String> files = new HashSet<>();
				for (Set<String> fileSet : results) {
					files.addAll(fileSet);
				}
				callback.onResult(new ArrayList<>(files));
			}
		});
		for (ServerInfo server : servers) {
			protocol.list(server, waiter);
		}
	}

	private void getAliveServers(final ResultCallback<List<ServerInfo>> callback) {
		getAliveServers(0, callback);
	}

	private void getAliveServers(final int currentAttempt, final ResultCallback<List<ServerInfo>> callback) {
		ResultCallback<Set<ServerInfo>> waiter = Util.waitAnyResults(bootstrap.size(), new Util.Resolver<Set<ServerInfo>>() {
			@Override
			public void resolve(List<Set<ServerInfo>> results, List<Exception> exceptions) {
				Set<ServerInfo> servers = new HashSet<>();
				for (Set<ServerInfo> serverSet : results) {
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
		for (ServerInfo server : bootstrap) {
			protocol.alive(server, waiter);
		}
	}

	private long defineRetryTime(int attempt) {
		long multiplier = 2;
		for (int i = 0; i < attempt; i++) {
			multiplier *= multiplier;
		}
		return baseRetryTimeout * multiplier;
	}

	private void schedule(int attempt, Runnable runnable) {
		eventloop.schedule(eventloop.currentTimeMillis() + defineRetryTime(attempt), runnable);
	}
}
