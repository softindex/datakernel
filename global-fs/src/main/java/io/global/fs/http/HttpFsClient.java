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

package io.global.fs.http;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;

import java.util.List;
import java.util.function.Function;

import static io.datakernel.remotefs.RemoteFsUtils.KNOWN_ERRORS;
import static io.global.fs.api.FsCommand.*;
import static io.global.fs.http.RemoteFsServlet.FILE_META_LIST;
import static io.global.fs.util.HttpDataFormats.ERROR_CODE_CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpFsClient implements FsClient {
	private final IAsyncHttpClient client;
	private final String url;

	private HttpFsClient(String url, IAsyncHttpClient client) {
		this.url = url;
		this.client = client;
	}

	public static HttpFsClient create(String url, IAsyncHttpClient client) {
		return new HttpFsClient(url, client);
	}

	private static final Function<HttpResponse, Promise<HttpResponse>> checkResponse =
			response -> {
				switch (response.getCode()) {
					case 200:
						return Promise.of(response);
					case 500:
						return response.getBody()
								.thenCompose(body -> {
									try {
										int code = JsonUtils.fromJson(ERROR_CODE_CODEC, body.asString(UTF_8)).getValue1();
										return Promise.ofException(code >= 1 && code <= KNOWN_ERRORS.length ?
												KNOWN_ERRORS[code - 1] :
												HttpException.ofCode(500));
									} catch (ParseException ignored) {
										return Promise.ofException(HttpException.ofCode(500));
									}
								});
					default:
						return Promise.ofException(HttpException.ofCode(response.getCode()));
				}
			};


	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset, long revision) {
		SettablePromise<ChannelConsumer<ByteBuf>> channelPromise = new SettablePromise<>();
		SettablePromise<HttpResponse> responsePromise = new SettablePromise<>();

		UrlBuilder urlBuilder = UrlBuilder.relative().appendPathPart(DOWNLOAD).appendPath(filename);
		if (offset != 0) {
			urlBuilder.appendQuery("offset", "" + offset);
		}
		client.request(
				HttpRequest.post(
						url + urlBuilder
								.appendQuery("revision", "" + revision)
								.build())
						.withBodyStream(ChannelSupplier.ofLazyProvider(() -> {
							ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
							channelPromise.trySet(buffer.getConsumer()
									.withAcknowledgement(ack -> ack.both(responsePromise)));
							return buffer.getSupplier();
						})))
				.thenCompose(checkResponse)
				.whenException(channelPromise::trySetException)
				.whenComplete(responsePromise::trySet);

		return channelPromise;
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long length) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(DOWNLOAD)
								.appendPath(name)
								.appendQuery("range", offset + (length == -1 ? "" : ("-" + (offset + length))))
								.build()))
				.thenCompose(checkResponse)
				.thenApply(HttpMessage::getBodyStream);
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(String glob) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(LIST)
								.appendQuery("glob", glob)
								.build()))
				.thenCompose(checkResponse)
				.thenCompose(HttpMessage::getBody)
				.thenCompose(body -> {
					try {
						return Promise.of(JsonUtils.fromJson(FILE_META_LIST, body.getString(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					} finally {
						body.recycle();
					}
				});
	}

	@Override
	public Promise<Void> move(String name, String target, long targetRevision, long removeRevision) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(MOVE)
								.appendQuery("name", name)
								.appendQuery("target", target)
								.appendQuery("revision", targetRevision)
								.appendQuery("removeRevision", removeRevision)
								.build()))
				.thenCompose(checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> copy(String name, String target, long targetRevision) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(COPY)
								.appendQuery("name", name)
								.appendQuery("target", target)
								.appendQuery("revision", targetRevision)
								.build()))
				.thenCompose(checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> delete(String name, long revision) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(DELETE)
								.appendPath(name)
								.appendQuery("revision", revision)
								.build()))
				.thenCompose(checkResponse)
				.toVoid();
	}
}
