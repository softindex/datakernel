/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.MultipartParser.MultipartFrame;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.Recyclable;
import io.datakernel.util.ref.Ref;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.util.MemSize.kilobytes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

/**
 * Util class that allows to parse some binary channel (mainly, the request body stream) into a channel of multipart frames.
 */
public final class MultipartParser implements ByteBufsParser<MultipartFrame> {
	private static final int MAX_META_SIZE = ApplicationSettings.getMemSize(MultipartParser.class, "maxMetaBuffer", kilobytes(4)).toInt();

	@Nullable
	private List<String> readingHeaders = null;

	private byte[] boundary;
	private byte[] lastBoundary;

	private MultipartParser(String boundary) {
		this.boundary = ("--" + boundary).getBytes(UTF_8);
		this.lastBoundary = ("--" + boundary + "--").getBytes(UTF_8);
	}

	public static MultipartParser create(String boundary) {
		return new MultipartParser(boundary);
	}

	/**
	 * Converts resulting channel of frames into a binary channel, ignoring any multipart headers.
	 */
	public ByteBufsParser<ByteBuf> ignoreHeaders() {
		return bufs -> {
			MultipartFrame frame = tryParse(bufs);
			if (frame == null || frame.isHeaders()) {
				return null;
			}
			return frame.getData();
		};
	}

	private Promise<String> getFilenameFromHeader(@Nullable String header) {
		if (header == null) {
			return Promise.ofException(new StacklessException(MultipartParser.class, "Headers had no Content-Disposition"));
		}
		int i = header.indexOf("filename=\"");
		if (i == -1) {
			return Promise.ofException(new StacklessException(MultipartParser.class, "Content-Disposition header has no filename"));
		}
		i += 10;
		int j = header.indexOf('"', i);
		if (j == -1) {
			return Promise.ofException(new StacklessException(MultipartParser.class, "Content-Disposition header filename field is missing a closing quote"));
		}
		return Promise.of(header.substring(i, j));
	}

	private Promise<Void> splitByFilesImpl(MultipartFrame headerFrame, ChannelSupplier<MultipartFrame> frames, Function<String, ChannelConsumer<ByteBuf>> consumerFunction) {
		return getFilenameFromHeader(headerFrame.getHeaders().get("content-disposition"))
				.then(filename -> {
					Ref<MultipartFrame> last = new Ref<>();
					return frames
							.until(f -> {
								boolean res = !f.isData() && getFilenameFromHeader(f.getHeaders().get("content-disposition"))
										.mapEx((x, e) -> x)
										.materialize()
										.getResult() != null; // ignoring any exceptions in this hack
								if (res) {
									last.set(f);
								}
								return res;
							})
							.filter(MultipartFrame::isData)
							.map(MultipartFrame::getData)
							.streamTo(consumerFunction.apply(filename))
							.then($ -> last.get() != null ?
									splitByFilesImpl(last.get(), frames, consumerFunction) :
									Promise.complete())
							.toVoid();
				});
	}

	/**
	 * Complex operation that streams this channel of multipart frames into multiple binary consumers, file-by-file
	 * as specified by the Content-Disposition multipart header.
	 */
	public Promise<Void> splitByFiles(ChannelSupplier<ByteBuf> source, Function<String, ChannelConsumer<ByteBuf>> consumerFunction) {
		ChannelSupplier<MultipartFrame> frames = BinaryChannelSupplier.of(source).parseStream(this);
		return frames.get()
				.then(frame ->
						frame.isHeaders() ?
								splitByFilesImpl(frame, frames, consumerFunction) :
								Promise.ofException(new StacklessException(MultipartParser.class, "First frame had no headers")));
	}

	boolean sawCrlf = true;
	boolean finished = false;

	@Nullable
	@Override
	public MultipartFrame tryParse(ByteBufQueue bufs) {
		if (finished) {
			return null;
		}
		for (int i = 0; i < bufs.remainingBytes() - 1; i++) {
			if (bufs.peekByte(i) != CR || bufs.peekByte(i + 1) != LF) {
				continue;
			}
			if (sawCrlf) {
				ByteBuf term = bufs.takeExactSize(i);
				if (readingHeaders == null) {
					if (term.isContentEqual(lastBoundary)) {
						bufs.skip(2);
						finished = true;
						term.recycle();
						return null;
					} else if (term.isContentEqual(boundary)) {
						bufs.skip(2);
						i = -1; // fix the index (so that it's 0 on next iteration) because we've taken bytes from queue
						term.recycle();
						readingHeaders = new ArrayList<>();
					} else {
						sawCrlf = false;
						return getFalseTermFrame(term);
					}
				} else {
					bufs.skip(2);
					if (i != 0) {
						i = -1; // see above comment
						readingHeaders.add(term.asString(UTF_8));
						continue;
					}
					sawCrlf = false;
					term.recycle();
					List<String> readingHeaders = this.readingHeaders;
					this.readingHeaders = null;
					if (readingHeaders.isEmpty()) {
						break;
					}
					return MultipartFrame.of(readingHeaders.stream()
							.map(s -> s.split(":\\s?", 2))
							.collect(toMap(s -> s[0].toLowerCase(), s -> s[1])));
				}
			} else {
				sawCrlf = true;
				ByteBuf tail = bufs.takeExactSize(i);
				bufs.skip(2);
				return MultipartFrame.of(tail);
			}
		}

		int remaining = bufs.remainingBytes();
		if (sawCrlf) {
			if (remaining >= MAX_META_SIZE) {
				sawCrlf = false;
				return getFalseTermFrame(bufs.takeRemaining());
			}
			return null;
		}
		int toTake = remaining == 0 ? 0 : remaining - (bufs.peekByte(remaining - 1) == CR ? 1 : 0);
		if (toTake == 0) {
			return null;
		}
		ByteBuf data = bufs.takeExactSize(toTake);
		return MultipartFrame.of(data);
	}

	@NotNull
	private MultipartFrame getFalseTermFrame(ByteBuf term) {
		ByteBuf buf = ByteBufPool.allocate(term.readRemaining() + 2);
		buf.writeByte((byte) '\r');
		buf.writeByte((byte) '\n');
		term.drainTo(buf, term.readRemaining());
		term.recycle();
		return MultipartFrame.of(buf);
	}

	public static final class MultipartFrame implements Recyclable {
		@Nullable
		private final ByteBuf data;
		@Nullable
		private final Map<String, String> headers;

		private MultipartFrame(@Nullable ByteBuf data, @Nullable Map<String, String> headers) {
			this.data = data;
			this.headers = headers;
		}

		public static MultipartFrame of(ByteBuf data) {
			return new MultipartFrame(data, null);
		}

		public static MultipartFrame of(Map<String, String> headers) {
			return new MultipartFrame(null, headers);
		}

		public boolean isData() {
			return data != null;
		}

		public ByteBuf getData() {
			assert data != null : "Trying to get data out of header frame";
			return data;
		}

		public boolean isHeaders() {
			return headers != null;
		}

		public Map<String, String> getHeaders() {
			assert headers != null : "Trying to get headers out of data frame";
			return headers;
		}

		@Override
		public void recycle() {
			if (data != null) {
				data.recycle();
			}
		}

		@Override
		public String toString() {
			return isHeaders() ? "headers" + headers : "" + data;
		}
	}
}
