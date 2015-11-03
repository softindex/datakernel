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

package io.datakernel.hashfs2;

import com.google.common.collect.Lists;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs2.protocol.ClientProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HashFsClient implements FsClient {
	private final NioEventloop eventloop;

	private final ClientProtocol protocol;
	private final Hashing hashing;

	private final List<ServerInfo> bootstrap;
	private final long baseRetryTimeout;
	private final int maxRetryAttempts;

	public HashFsClient(NioEventloop eventloop, ClientProtocol protocol, Hashing hashing,
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
	public void upload(final String filePath, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				List<ServerInfo> candidates = hashing.sortServers(result, filePath);
				upload(filePath, 0, candidates, producer, callback);
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void download(final String filePath, final StreamConsumer<ByteBuf> consumer) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				List<ServerInfo> candidates = hashing.sortServers(result, filePath);
				download(filePath, 0, candidates, consumer);
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.closingWithError(eventloop, e);
			}
		});
	}

	@Override
	public void deleteFile(final String filePath, final CompletionCallback callback) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				List<ServerInfo> candidates = hashing.sortServers(result, filePath);
				deleteFile(filePath, 0, candidates, callback);
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void listFiles(final ResultCallback<List<String>> callback) {
		final int[] pendingAliveRequests = new int[1];
		final Set<ServerInfo> servers = new HashSet<>();
		for (final ServerInfo server : bootstrap) {
			pendingAliveRequests[0]++;
			protocol.alive(server, new ResultCallback<Set<ServerInfo>>() {
				@Override
				public void onResult(Set<ServerInfo> result) {
					pendingAliveRequests[0]--;
					servers.addAll(result);
					if (pendingAliveRequests[0] == 0) {
						list(servers, callback);
					}
				}

				@Override
				public void onException(Exception ignored) {
					pendingAliveRequests[0]--;
					if (pendingAliveRequests[0] == 0) {
						list(servers, callback);
					}
				}
			});
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
		final int[] pendingListRequests = new int[1];
		final Set<String> files = new HashSet<>();
		for (ServerInfo server : servers) {
			pendingListRequests[0]++;
			protocol.list(server, new ResultCallback<Set<String>>() {
				@Override
				public void onResult(Set<String> result) {
					pendingListRequests[0]--;
					files.addAll(result);
					if (pendingListRequests[0] == 0) {
						callback.onResult(Lists.newArrayList(files));
					}
				}

				@Override
				public void onException(Exception e) {
					pendingListRequests[0]--;
					if (pendingListRequests[0] == 0) {
						callback.onResult(Lists.newArrayList(files));
					}
				}
			});
		}
	}

	private void getAliveServers(final ResultCallback<List<ServerInfo>> callback) {
		getAliveServers(0, callback);
	}

	private void getAliveServers(final int currentAttempt, final ResultCallback<List<ServerInfo>> callback) {
		ServerInfo server = bootstrap.get(currentAttempt % bootstrap.size());
		protocol.alive(server, new ResultCallback<Set<ServerInfo>>() {
			@Override
			public void onResult(Set<ServerInfo> result) {
				callback.onResult(Lists.newArrayList(result));
			}

			@Override
			public void onException(Exception ignored) {
				if (currentAttempt < maxRetryAttempts) {
					eventloop.schedule(eventloop.currentTimeMillis() + baseRetryTimeout, new Runnable() {
						@Override
						public void run() {
							getAliveServers(currentAttempt + 1, callback);
						}
					});
				} else {
					callback.onException(new Exception("Can't find working servers."));
				}
			}
		});
	}
}
