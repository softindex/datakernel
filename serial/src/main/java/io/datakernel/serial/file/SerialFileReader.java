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

package io.datakernel.serial.file;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.file.AsyncFile;
import io.datakernel.serial.AbstractSerialSupplier;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.nio.file.StandardOpenOption.READ;

/**
 * This supplier allows you to asynchronously read binary data from a file.
 */
public final class SerialFileReader extends AbstractSerialSupplier<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(SerialFileReader.class);

	public static final OpenOption[] READ_OPTIONS = new OpenOption[]{READ};

	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(8);

	private final AsyncFile asyncFile;

	private int bufferSize = DEFAULT_BUFFER_SIZE.toInt();
	private long position = 0;
	private long limit = Long.MAX_VALUE;
	private boolean finished = false;

	private SerialFileReader(AsyncFile asyncFile) {
		this.asyncFile = asyncFile;
	}

	public static SerialFileReader readFile(ExecutorService executor, Path path) throws IOException {
		return new SerialFileReader(AsyncFile.open(executor, path, READ_OPTIONS));
	}

	public static SerialFileReader readFile(AsyncFile asyncFile) {
		return new SerialFileReader(asyncFile);
	}

	public SerialFileReader withBufferSize(MemSize bufferSize) {
		checkArgument(bufferSize.toInt() > 0, "Buffer size cannot be less than or equal to zero");

		this.bufferSize = bufferSize.toInt();
		return this;
	}

	public SerialFileReader withOffset(long offset) {
		checkArgument(offset >= 0, "Offset cannot be negative");
		position = offset;
		return this;
	}

	public SerialFileReader withLength(long length) {
		checkArgument(length >= 0, "Length cannot be less than zero");
		this.limit = length;
		return this;
	}

	public long getPosition() {
		return position;
	}

	@Override
	protected Stage<ByteBuf> doGet() {
		if (finished) {
			close();
			return Stage.of(null);
		}
		int bufSize = (int) Math.min(bufferSize, limit);
		ByteBuf buf = ByteBufPool.allocateExact(bufSize);
		return asyncFile.read(buf, position) // reads are synchronized at least on asyncFile, so if produce() is called twice, position wont be broken (i hope)
				.thenComposeEx(($, e) -> {
					if (e != null) {
						buf.recycle();
						close(e);
						return Stage.ofException(getException());
					}
					int bytesRead = buf.readRemaining(); // bytes written (as they were read from file, thus the name) to be read by a consumer (thus the method)
					if (bytesRead == 0) { // this happens when file size is exact multiple of buffer size
						buf.recycle();
						close();
						return Stage.of(null);
					}
					position += bytesRead;
					if (limit != Long.MAX_VALUE) {
						limit -= bytesRead; // bytesRead is always <= the limit (^ see the min call)
					}
					if (limit == 0L || bytesRead < bufSize) { // AsyncFile#read finishes either if file is done or buffer is filled
						finished = true;
					}
					return Stage.of(buf);
				});
	}

	@Override
	protected void onClosed(Throwable e) {
		closeFile();
	}

	private void closeFile() {
		asyncFile.close()
				.whenComplete(($, e) -> {
					if (e == null) {
						logger.trace(this + ": closed file");
					} else {
						logger.error(this + ": failed to close file", e);
					}
				});
	}

	@Override
	public String toString() {
		return "SerialFileReader{" + asyncFile +
				", pos=" + position +
				(limit == Long.MAX_VALUE ? "" : ", len=" + limit) +
				'}';
	}
}
