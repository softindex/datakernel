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

package io.global.fs.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import static io.datakernel.http.IAsyncHttpClient.ensureOk200;
import static io.datakernel.http.IAsyncHttpClient.ensureStatusCode;
import static io.global.fs.http.RemoteFsServlet.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HttpFsClient implements FsClient {
	private final IAsyncHttpClient client;
	private final InetSocketAddress address;

	private HttpFsClient(InetSocketAddress address, IAsyncHttpClient client) {
		this.address = address;
		this.client = client;
	}

	public static HttpFsClient create(InetSocketAddress address, IAsyncHttpClient client) {
		return new HttpFsClient(address, client);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		return Promise.of(uploader(filename, offset));
	}

	@Override
	public ChannelConsumer<ByteBuf> uploader(String filename, long offset) {
		ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
		Promise<HttpResponse> res = client.request(
				HttpRequest.put(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DOWNLOAD)
								.appendPath(filename)
								.appendQuery("offset", "" + offset)
								.build())
						.withBodyStream(buffer.getSupplier()))
				.thenCompose(ensureStatusCode(200, 201))
				.materialize();
		return buffer.getConsumer().withAcknowledgement(ack -> ack.both(res));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return client.request(
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DOWNLOAD)
								.appendPath(filename)
								.appendQuery("range", offset + (length == -1 ? "" : ("-" + (offset + length))))
								.build()))
				.thenCompose(ensureOk200())
				.thenApply(HttpMessage::getBodyStream);
	}

	@Override
	public Promise<Void> moveBulk(Map<String, String> changes) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(MOVE)
								.build())
						.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
				.thenCompose(ensureOk200())
				.toVoid();
	}

	@Override
	public Promise<Void> copyBulk(Map<String, String> changes) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(COPY)
								.build())
						.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
				.thenCompose(ensureOk200())
				.toVoid();
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(LIST)
								.appendQuery("glob", glob)
								.build()))
				.thenCompose(ensureOk200())
				.thenCompose(response -> {
					try {
						return Promise.of(JsonUtils.fromJson(FILE_META_LIST, response.getBody().asString(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public Promise<Void> deleteBulk(String glob) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DEL)
								.appendQuery("glob", glob)
								.build()))
				.thenCompose(ensureOk200())
				.toVoid();
	}
}
