/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.multilog;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.AbstractCommunicatingProcess;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelInput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.remotefs.FsClient;
import io.datakernel.time.CurrentTimeProvider;

final class LogStreamChunker extends AbstractCommunicatingProcess implements ChannelInput<ByteBuf> {
	private final CurrentTimeProvider currentTimeProvider;
	private final FsClient client;
	private final LogNamingScheme namingScheme;
	private final String logPartition;

	private ChannelSupplier<ByteBuf> input;
	private ChannelConsumer<ByteBuf> currentConsumer;

	private LogFile currentChunk;

	public LogStreamChunker(CurrentTimeProvider currentTimeProvider, FsClient client, LogNamingScheme namingScheme, String logPartition) {
		this.currentTimeProvider = currentTimeProvider;
		this.client = client;
		this.namingScheme = namingScheme;
		this.logPartition = logPartition;
	}

	@Override
	public MaterializedPromise<Void> set(ChannelSupplier<ByteBuf> input) {
		this.input = sanitize(input);
		startProcess();
		return getProcessResult();
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (buf != null) {
						ensureConsumer()
								.thenCompose($ -> currentConsumer.accept(buf))
								.whenResult($ -> doProcess());
					} else {
						flush().whenResult($ -> completeProcess());
					}
				});
	}

	private Promise<Void> ensureConsumer() {
		LogFile newChunkName = namingScheme.format(currentTimeProvider.currentTimeMillis());
		return currentChunk != null && currentChunk.getName().compareTo(newChunkName.getName()) >= 0 ?
				Promise.complete() :
				startNewChunk(newChunkName);
	}

	private Promise<Void> startNewChunk(LogFile newChunkName) {
		return flush()
				.thenCompose($ -> {
					this.currentChunk = (currentChunk == null) ? newChunkName : new LogFile(newChunkName.getName(), 0);
					return client.upload(namingScheme.path(logPartition, currentChunk))
							.thenComposeEx(this::sanitize)
							.whenResult(newConsumer ->
									this.currentConsumer = sanitize(newConsumer))
							.toVoid();
				});
	}

	private Promise<Void> flush() {
		if (currentConsumer == null) {
			return Promise.complete();
		}
		return currentConsumer.accept(null)
				.whenResult($ -> currentConsumer = null);
	}

	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		if (currentConsumer != null) {
			currentConsumer.close(e);
		}
	}
}
