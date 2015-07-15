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

import static io.datakernel.stream.processor.StreamLZ4Compressor.fastCompressor;

import java.util.List;

import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.protocol.HashFsClientProtocol;
import io.datakernel.hashfs.protocol.gson.HashFsGsonClientProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamByteChunker;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashFsImpl implements HashFs {
	private static final Logger logger = LoggerFactory.getLogger(HashFsImpl.class);

	public interface StreamLZ4CompressorFactory {
		StreamLZ4Compressor getInstance(Eventloop eventloop);
	}

	public static HashFsImpl createHashClient(NioEventloop eventloop, List<ServerInfo> connectServers) {
		return createHashClient(eventloop, connectServers, 128 * 1024);
	}

	public static HashFsImpl createHashClient(NioEventloop eventloop, List<ServerInfo> connectServers, int bufferSize) {
		return createHashClient(eventloop, connectServers, bufferSize, new StreamLZ4CompressorFactory() {
			@Override
			public StreamLZ4Compressor getInstance(Eventloop eventloop) {
				return fastCompressor(eventloop);
			}
		});
	}

	public static HashFsImpl createHashClient(NioEventloop eventloop, List<ServerInfo> connectServers, int bufferSize,
	                                          StreamLZ4CompressorFactory compressorFactory) {
		Configuration configuration = new Configuration();
		return createHashClient(eventloop, connectServers, configuration, bufferSize, compressorFactory);
	}

	public static HashFsImpl createHashClient(NioEventloop eventloop, List<ServerInfo> connectServers, Configuration configuration, int bufferSize,
	                                          StreamLZ4CompressorFactory compressorFactory) {
		HashFsGsonClientProtocol clientTransport = new HashFsGsonClientProtocol(eventloop, connectServers);
		return new HashFsImpl(eventloop, clientTransport, connectServers, configuration, bufferSize, compressorFactory);
	}

	private final Eventloop eventloop;
	private final HashFsClientProtocol protocol;
	private final List<ServerInfo> bootstrapServers;
	private final Configuration configuration;
	private int currentBootstrapServer = 0;

	private final int bufferSize;

	private final StreamLZ4CompressorFactory compressorFactory;

	private HashFsImpl(Eventloop eventloop, HashFsClientProtocol protocol, List<ServerInfo> bootstrapServers, Configuration configuration,
	                   int bufferSize, StreamLZ4CompressorFactory compressorFactory) {
		this.eventloop = eventloop;
		this.protocol = protocol;
		this.bootstrapServers = bootstrapServers;
		this.configuration = configuration;
		this.bufferSize = bufferSize;
		this.compressorFactory = compressorFactory;
	}

	private void getAliveServers(final ResultCallback<List<ServerInfo>> callback) {
		getAliveServers(0, callback);
	}

	private void getAliveServers(final int currentAttempt, final ResultCallback<List<ServerInfo>> callback) {
		if (bootstrapServers.isEmpty() || currentAttempt >= configuration.getMaxRetryAttempts()) {
			callback.onException(new Exception("Can't find working servers."));
			return;
		}

		ServerInfo server = bootstrapServers.get(currentBootstrapServer % bootstrapServers.size());
		protocol.getAliveServers(server, new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				callback.onResult(result);
			}

			@Override
			public void onException(Exception exception) {
				currentBootstrapServer++;
				eventloop.schedule(eventloop.currentTimeMillis() + 1000L, new Runnable() {
					@Override
					public void run() {
						getAliveServers(currentAttempt + 1, callback);
					}
				});
			}
		});
	}

	private void upload(final String filename, final List<ServerInfo> orderedServers, final int currentAttempt, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		if (orderedServers.isEmpty() || currentAttempt >= configuration.getMaxRetryAttempts()) {
			callback.onException(new Exception("Can't find working servers."));
			return;
		}

		ServerInfo server = orderedServers.get(currentAttempt % orderedServers.size());
		protocol.upload(server, filename, new ResultCallback<StreamConsumer<ByteBuf>>() {
			@Override
			public void onResult(StreamConsumer<ByteBuf> streamConsumer) {
				StreamByteChunker streamByteChunkerBefore = new StreamByteChunker(eventloop, bufferSize / 2, bufferSize);
				StreamLZ4Compressor compressor = compressorFactory.getInstance(eventloop);
				StreamByteChunker streamByteChunkerAfter = new StreamByteChunker(eventloop, bufferSize / 2, bufferSize);

				streamByteChunkerBefore.streamTo(compressor);
				compressor.streamTo(streamByteChunkerAfter);
				streamByteChunkerAfter.streamTo(streamConsumer);

				callback.onResult(streamByteChunkerBefore);
			}

			@Override
			public void onException(Exception exception) {
				eventloop.schedule(eventloop.currentTimeMillis() + 1000L, new Runnable() {
					@Override
					public void run() {
						upload(filename, orderedServers, currentAttempt + 1, callback);
					}
				});
			}
		});
	}

	private void upload(final String filename, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		getAliveServers(new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onException(Exception exception) {
				callback.onException(new Exception("Can't get alive servers."));
			}

			@Override
			public void onResult(List<ServerInfo> aliveServers) {
				List<ServerInfo> orderedServers = RendezvousHashing.sortServers(aliveServers, filename);
				upload(filename, orderedServers, 0, callback);
			}
		});
	}

	private void download(final String filename, final List<ServerInfo> orderedServers, final int currentAttempt, final ResultCallback<StreamProducer<ByteBuf>> producerResultCallback) {
		if (orderedServers.isEmpty() || currentAttempt >= configuration.getMaxRetryAttempts()) {
			String errorMessage = orderedServers.isEmpty() ? "Can't find working servers." : "File not found";
			producerResultCallback.onException(new Exception(errorMessage));
			return;
		}

		ServerInfo server = orderedServers.get(currentAttempt % orderedServers.size());
		protocol.download(server, filename, new ResultCallback<StreamProducer<ByteBuf>>() {

			@Override
			public void onResult(StreamProducer<ByteBuf> result) {
				StreamLZ4Decompressor lz4Decompressor = new StreamLZ4Decompressor(eventloop);
				result.streamTo(lz4Decompressor);
				producerResultCallback.onResult(lz4Decompressor);
			}

			@Override
			public void onException(Exception exception) {
				eventloop.schedule(eventloop.currentTimeMillis() + 1000L, new Runnable() {
					@Override
					public void run() {
						download(filename, orderedServers, currentAttempt + 1, producerResultCallback);
					}
				});
			}
		});

	}

	private void download(final String filename, final ResultCallback<StreamProducer<ByteBuf>> producerResultCallback) {
		getAliveServers(new ForwardingResultCallback<List<ServerInfo>>(producerResultCallback) {
			@Override
			public void onResult(List<ServerInfo> aliveServers) {
				List<ServerInfo> orderedServers = RendezvousHashing.sortServers(aliveServers, filename);
				download(filename, orderedServers, 0, producerResultCallback);
			}
		});
	}

	private void deleteFile(final String filename, final List<ServerInfo> orderedServers, final int currentAttempt, final ResultCallback<Boolean> resultCallback) {
		if (orderedServers.isEmpty() || currentAttempt >= configuration.getMaxRetryAttempts()) {
			resultCallback.onException(new Exception("Can't find working servers."));
			return;
		}

		ServerInfo server = orderedServers.get(currentAttempt % orderedServers.size());
		protocol.delete(server, filename, new ResultCallback<Boolean>() {
			@Override
			public void onResult(Boolean result) {
				resultCallback.onResult(result);
			}

			@Override
			public void onException(Exception exception) {
				deleteFile(filename, orderedServers, currentAttempt + 1, resultCallback);
			}
		});
	}

	@Override
	public StreamConsumer<ByteBuf> upload(final String destinationFilePath) {
		final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		upload(destinationFilePath, new ResultCallback<StreamConsumer<ByteBuf>>() {
			@Override
			public void onResult(StreamConsumer<ByteBuf> actualConsumer) {
				forwarder.streamTo(actualConsumer);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Upload file {} failed", destinationFilePath, exception);
				forwarder.streamTo(StreamConsumers.<ByteBuf>closingWithError(eventloop, exception));
			}
		});
		return forwarder;
	}

	@Override
	public StreamProducer<ByteBuf> download(final String filename) {
		final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);

		download(filename, new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> actualProducer) {
				actualProducer.streamTo(forwarder);
			}

			@Override
			public void onException(Exception exception) {
				forwarder.closeWithError(exception);
			}
		});

		return forwarder;
	}

	@Override
	public void deleteFile(final String filename, final ResultCallback<Boolean> callback) {
		getAliveServers(new ForwardingResultCallback<List<ServerInfo>>(callback) {
			@Override
			public void onResult(List<ServerInfo> aliveServers) {
				List<ServerInfo> orderedServers = RendezvousHashing.sortServers(aliveServers, filename);
				deleteFile(filename, orderedServers, 0, callback);
			}
		});
	}

}
