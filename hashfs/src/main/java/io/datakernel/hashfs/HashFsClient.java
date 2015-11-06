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
import io.datakernel.hashfs.protocol.ClientProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class HashFsClient implements FsClient {
	private final NioEventloop eventloop;

	private final ClientProtocol protocol;
	private final HashingStrategy hashing;

	private final List<ServerInfo> bootstrap;
	private final long baseRetryTimeout;
	private final int maxRetryAttempts;

	public HashFsClient(NioEventloop eventloop, ClientProtocol protocol, HashingStrategy hashing,
	                    List<ServerInfo> bootstrap, long baseRetryTimeout, int maxRetryAttempts) {
		this.eventloop = eventloop;
		this.protocol = protocol;
		this.hashing = hashing;
		this.bootstrap = bootstrap;
		this.baseRetryTimeout = baseRetryTimeout;
		this.maxRetryAttempts = maxRetryAttempts;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public void upload(final String destinationFileName, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
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
				StreamProducers.closingWithError(eventloop, e);
			}
		});
	}

	@Override
	public void delete(final String fileName, final CompletionCallback callback) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				List<ServerInfo> candidates = hashing.sortServers(fileName, result);
				deleteFile(fileName, 0, candidates, callback);
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void list(final ResultCallback<List<String>> callback) {
		ResultCallback<Set<ServerInfo>> waiter = Util.waitAllResults(bootstrap.size(), new Util.Resolver<Set<ServerInfo>>() {
			@Override
			public void resolve(List<Set<ServerInfo>> results, List<Exception> exceptions) {
				final Set<ServerInfo> servers = new HashSet<>();
				for (Set<ServerInfo> serverSet : results) {
					servers.addAll(serverSet);
				}
				list(servers, callback);
			}
		});
		for (ServerInfo server : bootstrap) {
			protocol.alive(server, waiter);
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private void upload(final String filePath, final int currentAttempt, final List<ServerInfo> candidates,
	                    final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {

		ServerInfo server = candidates.get(currentAttempt % candidates.size());
		protocol.upload(server, filePath, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				if (currentAttempt < maxRetryAttempts) {
					upload(filePath, currentAttempt + 1, candidates, producer, callback);
				} else {
					callback.onException(e);
				}
			}
		});
	}

	private void download(final String filePath, final int currentAttempt, final List<ServerInfo> candidates, final StreamConsumer<ByteBuf> consumer) {
		final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		ServerInfo server = candidates.get(currentAttempt % candidates.size());
		protocol.download(server, filePath, forwarder.getInput(), new CompletionCallback() {
			@Override
			public void onComplete() {
				forwarder.getOutput().streamTo(consumer);
			}

			@Override
			public void onException(Exception e) {
				if (currentAttempt < maxRetryAttempts) {
					download(filePath, currentAttempt + 1, candidates, consumer);
				} else {
					StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
				}
			}
		});
	}

	private void deleteFile(final String filePath, final int currentAttempt, final List<ServerInfo> candidates, final CompletionCallback callback) {
		ServerInfo server = candidates.get(currentAttempt % candidates.size());
		protocol.delete(server, filePath, new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				if (currentAttempt < maxRetryAttempts) {
					deleteFile(filePath, currentAttempt + 1, candidates, callback);
				} else {
					callback.onException(e);
				}
			}
		});
	}

	private void list(Set<ServerInfo> servers, final ResultCallback<List<String>> callback) {
		ResultCallback<Set<String>> waiter = Util.waitAllResults(servers.size(), new Util.Resolver<Set<String>>() {
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
		ResultCallback<Set<ServerInfo>> waiter = Util.waitAllResults(bootstrap.size(), new Util.Resolver<Set<ServerInfo>>() {
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
		// TODO temporary
		return (long) (baseRetryTimeout * Math.pow(2.0, attempt));
	}
}
