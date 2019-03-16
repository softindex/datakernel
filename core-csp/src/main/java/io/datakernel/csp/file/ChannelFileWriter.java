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

package io.datakernel.csp.file;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.file.AsyncFile;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.Executor;

import static io.datakernel.util.CollectionUtils.set;
import static java.nio.file.StandardOpenOption.*;

/**
 * This consumer allows you to asynchronously write binary data to a file.
 */
public final class ChannelFileWriter extends AbstractChannelConsumer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileWriter.class);

	public static final Set<OpenOption> CREATE_OPTIONS = set(WRITE, CREATE_NEW, APPEND);

	private final AsyncFile asyncFile;

	private boolean forceOnClose = false;
	private boolean forceMetadata = false;
	private long startingOffset = 0;
	private boolean started;

	// region creators
	private ChannelFileWriter(AsyncFile asyncFile) {
		this.asyncFile = asyncFile;
	}

	public static ChannelFileWriter create(Executor executor, Path path) throws IOException {
		return create(AsyncFile.open(executor, path, CREATE_OPTIONS));
	}

	public static ChannelFileWriter create(AsyncFile asyncFile) {
		return new ChannelFileWriter(asyncFile);
	}

	public ChannelFileWriter withForceOnClose(boolean forceMetadata) {
		forceOnClose = true;
		this.forceMetadata = forceMetadata;
		return this;
	}

	public ChannelFileWriter withOffset(long offset) {
		startingOffset = offset;
		return this;
	}
	// endregion

	@Override
	protected void onClosed(@NotNull Throwable e) {
		closeFile();
	}

	@Override
	protected Promise<Void> doAccept(ByteBuf buf) {
		return ensureOffset()
				.thenEx(($, e) -> {
					if (isClosed()) {
						if (buf != null) {
							buf.recycle();
						}
						return Promise.ofException(getException());
					}
					if (e != null) {
						if (buf != null) {
							buf.recycle();
						}
						close(e);
						return Promise.ofException(e);
					}
					if (buf == null) {
						return closeFile()
								.acceptEx(($1, e1) -> close());
					}
					return asyncFile.write(buf)
							.thenEx(($2, e2) -> {
								if (isClosed()) return Promise.ofException(getException());
								if (e2 != null) {
									close(e2);
								}
								return Promise.of($2, e2);
							});
				});
	}

	private Promise<Void> closeFile() {
		if (!asyncFile.isOpen()) {
			return Promise.complete();
		}
		return (forceOnClose ? asyncFile.forceAndClose(forceMetadata) : asyncFile.close())
				.acceptEx(($, e) -> {
					if (e == null) {
						logger.trace(this + ": closed file");
					} else {
						logger.error(this + ": failed to close file", e);
					}
				});
	}

	private Promise<Void> ensureOffset() {
		if (started) {
			return Promise.complete();
		}
		started = true;
		return startingOffset != 0 ? asyncFile.seek(startingOffset) : Promise.complete();
	}

	@Override
	public String toString() {
		return "ChannelFileWriter{" + asyncFile + "}";
	}
}
