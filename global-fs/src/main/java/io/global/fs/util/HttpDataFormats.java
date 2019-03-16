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

package io.global.fs.util;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.RecyclingChannelConsumer;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.*;
import io.datakernel.util.Tuple1;

import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.MediaTypes.OCTET_STREAM;
import static io.datakernel.remotefs.FsClient.DEFAULT_REVISION;
import static io.datakernel.remotefs.RemoteFsUtils.getErrorCode;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpDataFormats {
	public static final ParseException INVALID_RANGE_FORMAT = new ParseException(HttpDataFormats.class, "Invalid range format");
	public static final ParseException RANGE_OUT_OF_BOUNDS = new ParseException(HttpDataFormats.class, "Specified range is out of bounds");

	public static final StacklessException REVISION_NOT_HIGH_ENOUGH = new StacklessException(HttpDataFormats.class, "Revision is not high enough");

	public static final StructuredCodec<Tuple1<Integer>> ERROR_CODE_CODEC = object(Tuple1::new, "errorCode", Tuple1::getValue1, INT_CODEC);

	private HttpDataFormats() {
		throw new AssertionError("nope.");
	}

	@FunctionalInterface
	public interface HttpUploader {

		Promise<ChannelConsumer<ByteBuf>> upload(String name, long offset, long revision);
	}

	@FunctionalInterface
	public interface HttpDownloader {

		Promise<ChannelSupplier<ByteBuf>> download(long offset, long limit);
	}

	public static Promise<HttpResponse> httpUpload(HttpRequest request, HttpUploader uploader) {
		try {
			long offset = parseOffset(request);
			long revision = parseRevision(request);
			ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
			String contentType = request.getHeader(CONTENT_TYPE);
			if (!contentType.startsWith("multipart/form-data; boundary=")) {
				return Promise.ofException(HttpException.ofCode(400, "Content type is not multipart/form-data"));
			}
			String boundary = contentType.substring(30);
			if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
				boundary = boundary.substring(1, boundary.length() - 1);
			}
			return MultipartParser.create(boundary)
					.splitByFiles(bodyStream, name ->
							ChannelConsumer.ofPromise(uploader.upload(name, offset, revision)
									.then(consumer -> {
										if (!(consumer instanceof RecyclingChannelConsumer)) {
											return Promise.of(consumer);
										}
										return Promise.ofException(REVISION_NOT_HIGH_ENOUGH);
									})))
					.mapEx(errorHandler());
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}

	public static Promise<HttpResponse> httpDownload(HttpRequest request, HttpDownloader downloader, String name, long size) {
		try {
			String localName = name.substring(name.lastIndexOf('/') + 1);
			String headerRange = request.getHeaderOrNull(HttpHeaders.RANGE);
			if (headerRange == null) {
				return downloader.download(0, -1)
						.mapEx(errorHandler(HttpResponse.ok200()
								.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(OCTET_STREAM)))
								.withHeader(CONTENT_DISPOSITION, "attachment; filename=\"" + localName + "\"")
								.withHeader(ACCEPT_RANGES, "bytes")
								.withHeader(CONTENT_LENGTH, Long.toString(size))
								::withBodyStream));
			}
			if (!headerRange.startsWith("bytes=")) {
				throw HttpException.ofCode(416, "Invalid range header (not in bytes)");
			}
			headerRange = headerRange.substring(6);
			if (!headerRange.matches("(\\d+)?-(\\d+)?")) {
				throw HttpException.ofCode(416, "Only single part ranges are allowed");
			}
			String[] parts = headerRange.split("-", 2);
			long offset = parts[0].isEmpty() ? 0 : Long.parseLong(parts[0]);
			long endOffset = parts[1].isEmpty() ? -1 : Long.parseLong(parts[1]);
			if (endOffset != -1 && offset > endOffset) {
				throw HttpException.ofCode(416, "Invalid range");
			}
			long length = (endOffset == -1 ? size : endOffset) - offset + 1;
			return downloader.download(offset, length)
					.mapEx(errorHandler(HttpResponse.ok206()
							.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(OCTET_STREAM)))
							.withHeader(CONTENT_DISPOSITION, HttpHeaderValue.of("attachment; filename=\"" + localName + "\""))
							.withHeader(ACCEPT_RANGES, "bytes")
							.withHeader(CONTENT_RANGE, offset + "-" + (offset + length) + "/" + size)
							.withHeader(CONTENT_LENGTH, "" + length)
							::withBodyStream));
		} catch (HttpException e) {
			return Promise.ofException(e);
		}
	}

	public static HttpResponse getErrorResponse(Throwable e) {
		return HttpResponse.ofCode(500)
				.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentTypes.JSON_UTF_8))
				.withBody(JsonUtils.toJson(ERROR_CODE_CODEC, new Tuple1<>(getErrorCode(e))).getBytes(UTF_8));
	}

	public static <T> BiFunction<T, Throwable, HttpResponse> errorHandler() {
		return errorHandler($ -> HttpResponse.ok200().withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentTypes.PLAIN_TEXT_UTF_8)));
	}

	public static <T> BiFunction<T, Throwable, HttpResponse> errorHandler(Function<T, HttpResponse> successful) {
		return (res, e) -> e == null ? successful.apply(res) : getErrorResponse(e);
	}

	public static long[] parseRange(HttpRequest request) throws ParseException {
		String param = request.getQueryParameter("range", "");
		long[] range = {0, -1};
		String[] parts = param.split("-");
		switch (parts.length) {
			case 1:
				if (param.isEmpty()) {
					return range;
				}
				try {
					range[0] = Long.parseUnsignedLong(param);
					return range;
				} catch (NumberFormatException ignored) {
				}
			case 2:
				try {
					range[0] = Long.parseUnsignedLong(parts[0]);
					range[1] = Long.parseUnsignedLong(parts[1]) - range[0];
					if (range[1] < 0) {
						throw RANGE_OUT_OF_BOUNDS;
					}
				} catch (NumberFormatException ignored) {
					throw INVALID_RANGE_FORMAT;
				}
				return range;
			default:
				throw INVALID_RANGE_FORMAT;
		}
	}

	public static long parseOffset(HttpRequest request) throws ParseException {
		String param = request.getQueryParameterOrNull("offset");
		try {
			return param == null ? 0 : Long.parseLong(param);
		} catch (NumberFormatException e) {
			throw new ParseException(HttpDataFormats.class, "Failed to parse offset", e);
		}
	}

	public static long parseRevision(HttpRequest request) throws ParseException {
		return parseRevision(request, "revision");
	}

	public static long parseRevision(HttpRequest request, String paramName) throws ParseException {
		String param = request.getQueryParameterOrNull(paramName);
		try {
			return param != null ? Long.parseLong(param) : DEFAULT_REVISION;
		} catch (NumberFormatException e) {
			throw new ParseException(HttpDataFormats.class, "Failed to parse revision", e);
		}
	}
}
