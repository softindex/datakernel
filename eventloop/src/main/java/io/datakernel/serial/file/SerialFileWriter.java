/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.datakernel.serial.file;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.file.AsyncFile;
import io.datakernel.serial.AbstractSerialConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.*;

/**
 * This consumer allows you to asynchronously write binary data to a file.
 */
public final class SerialFileWriter extends AbstractSerialConsumer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(SerialFileWriter.class);

	public static final OpenOption[] CREATE_OPTIONS = new OpenOption[]{WRITE, CREATE_NEW, APPEND};

	private final AsyncFile asyncFile;

	private boolean forceOnClose = false;
	private boolean forceMetadata = false;
	private long startingOffset = -1;
	private boolean started;

	// region creators
	private SerialFileWriter(AsyncFile asyncFile) {
		this.asyncFile = asyncFile;
	}

	public static SerialFileWriter create(ExecutorService executor, Path path) throws IOException {
		return create(AsyncFile.open(executor, path, CREATE_OPTIONS));
	}

	public static SerialFileWriter create(AsyncFile asyncFile) {
		return new SerialFileWriter(asyncFile);
	}

	public SerialFileWriter withForceOnClose(boolean forceMetadata) {
		forceOnClose = true;
		this.forceMetadata = forceMetadata;
		return this;
	}

	public SerialFileWriter withOffset(long offset) {
		startingOffset = offset;
		return this;
	}
	// endregion

	@Override
	protected void onClosed(Throwable e) {
		closeFile();
	}

	@Override
	protected Stage<Void> doAccept(ByteBuf buf) {
		return start()
				.thenComposeEx(($, e) -> {
					if (isClosed()) return Stage.ofException(getException());
					if (e != null) {
						buf.recycle();
						closeWithError(e);
						return Stage.ofException(e);
					}
					if (buf == null) {
						return closeFile()
								.thenRunEx(this::close);
					}
					return asyncFile.write(buf)
							.thenComposeEx(($2, e2) -> {
								if (isClosed()) return Stage.ofException(getException());
								if (e2 != null) {
									closeWithError(e2);
								}
								return Stage.of($2, e2);
							});
				});
	}

	private Stage<Void> closeFile() {
		return (forceOnClose ? asyncFile.forceAndClose(forceMetadata) : asyncFile.close())
				.whenComplete(($, e) -> {
					if (e == null) {
						logger.trace(this + ": closed file");
					} else {
						logger.error(this + ": failed to close file", e);
					}
				});
	}

	private Stage<Void> start() {
		if (started) {
			return Stage.complete();
		}
		started = true;
		return startingOffset != -1 ? asyncFile.seek(startingOffset) : Stage.complete();
	}

	@Override
	public String toString() {
		return "{" + asyncFile + "}";
	}
}
