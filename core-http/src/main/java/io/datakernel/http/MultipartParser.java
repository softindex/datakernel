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
import io.datakernel.csp.ChannelConsumers;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.util.MemSize.kilobytes;
import static io.datakernel.util.Utils.nullify;
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

	private Promise<Map<String, String>> getContentDispositionFields(MultipartFrame frame) {
		String header = frame.getHeaders().get("content-disposition");
		if (header == null) {
			return Promise.ofException(new StacklessException(MultipartParser.class, "Headers had no Content-Disposition"));
		}
		String[] headerParts = header.split(";");
		if (headerParts.length == 0 || !"form-data".equals(headerParts[0].trim())) {
			return Promise.ofException(new StacklessException(MultipartParser.class, "Content-Disposition type is not 'form-data'"));
		}
		return Promise.of(Arrays.stream(headerParts)
				.skip(1)
				.map(part -> part.trim().split("=", 2))
				.collect(toMap(s -> s[0], s -> {
					String value = s.length == 1 ? "" : s[1];
					// stripping double quotation
					return value.substring(1, value.length() - 1);
				})));
	}

	private Promise<Void> doSplit(MultipartFrame headerFrame, ChannelSupplier<MultipartFrame> frames,
			MultipartDataHandler dataHandler) {
		return getContentDispositionFields(headerFrame)
				.then(contentDispositionFields -> {
					String fieldName = contentDispositionFields.get("name");
					String fileName = contentDispositionFields.get("filename");
					Ref<MultipartFrame> last = new Ref<>();
					return frames
							.until(f -> {
								if (f.isHeaders()) {
									last.set(f);
									return true;
								}
								return false;
							})
							.filter(MultipartFrame::isData)
							.map(MultipartFrame::getData)
							.streamTo(ChannelConsumer.ofPromise(fileName == null ?
									dataHandler.handleField(fieldName) :
									dataHandler.handleFile(fieldName, fileName)
							))
							.then($ -> last.get() != null ?
									doSplit(last.get(), frames, dataHandler) :
									Promise.complete())
							.toVoid();
				});
	}

	/**
	 * Complex operation that streams this channel of multipart frames into multiple binary consumers,
	 * as specified by the Content-Disposition multipart header.
	 */
	public Promise<Void> split(ChannelSupplier<ByteBuf> source, MultipartDataHandler dataHandler) {
		ChannelSupplier<MultipartFrame> frames = BinaryChannelSupplier.of(source).parseStream(this);
		return frames.get()
				.then(frame -> {
					if (frame == null) {
						return Promise.of(null);
					}
					if (frame.isHeaders()) {
						return doSplit(frame, frames, dataHandler);
					}
					StacklessException e = new StacklessException(MultipartParser.class, "First frame had no headers");
					frames.close(e);
					return Promise.ofException(e);
				});
	}

	private boolean sawCrlf = true;
	private boolean finished = false;

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
		private ByteBuf data;
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
			data = nullify(data, ByteBuf::recycle);
		}

		@Override
		public String toString() {
			return isHeaders() ? "headers" + headers : "" + data;
		}
	}

	public interface MultipartDataHandler {
		Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName);

		Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName);

		static MultipartDataHandler fieldsToMap(Map<String, String> fields) {
			return fieldsToMap(fields, ($1, $2) -> Promise.of(ChannelConsumers.recycling()));
		}

		static MultipartDataHandler fieldsToMap(Map<String, String> fields,
				Function<String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return fieldsToMap(fields, ($, fileName) -> uploader.apply(fileName));
		}

		static MultipartDataHandler fieldsToMap(Map<String, String> fields,
				BiFunction<String, String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return new MultipartDataHandler() {
				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName) {
					return Promise.of(ChannelConsumer.ofSupplier(supplier -> supplier.toCollector(ByteBufQueue.collector())
							.map(value -> {
								fields.put(fieldName, value.asString(UTF_8));
								return (Void) null;
							})));
				}

				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName) {
					return uploader.apply(fieldName, fileName);
				}
			};
		}

		static MultipartDataHandler file(Function<String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return files(($, fileName) -> uploader.apply(fileName));
		}

		static MultipartDataHandler files(BiFunction<String, String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return new MultipartDataHandler() {
				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName) {
					return Promise.of(ChannelConsumers.recycling());
				}

				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName) {
					return uploader.apply(fieldName, fileName);
				}
			};
		}

	}

}
