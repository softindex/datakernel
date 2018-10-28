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
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpMessage;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.json.GsonAdapters;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.global.fs.http.RemoteFsServlet.*;
import static io.global.fs.util.HttpDataFormats.FILE_META_LIST;
import static io.global.fs.util.HttpDataFormats.STRING_SET;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HttpFsClient implements FsClient {
	private final AsyncHttpClient client;
	private final InetSocketAddress address;

	// region creators
	public HttpFsClient(InetSocketAddress address, AsyncHttpClient client) {
		this.address = address;
		this.client = client;
	}
	// endregion

	@Override
	public Promise<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		return Promise.of(uploadSerial(filename, offset));
	}

	@Override
	public SerialConsumer<ByteBuf> uploadSerial(String filename, long offset) {
		SerialZeroBuffer<ByteBuf> buffer = new SerialZeroBuffer<>();
		Promise<HttpResponse> res = client.request(
				HttpRequest.put(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DOWNLOAD)
								.appendPath(filename)
								.appendQuery("offset", "" + offset)
								.build())
						.withBodyStream(buffer.getSupplier()))
				.materialize();
		return buffer.getConsumer().withAcknowledgement(ack -> ack.both(res));
	}

	@Override
	public Promise<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return client.request(
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DOWNLOAD)
								.appendPath(filename)
								.appendQuery("range", offset + (length == -1 ? "" : ("-" + (offset + length))))
								.build()))
				.thenApply(HttpMessage::getBodyStream);
	}

	@Override
	public Promise<Set<String>> move(Map<String, String> changes) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(MOVE)
								.build())
						.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
				.thenCompose(response -> {
					try {
						return Promise.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public Promise<Set<String>> copy(Map<String, String> changes) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(COPY)
								.build())
						.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
				.thenCompose(response -> {
					try {
						return Promise.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
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
				.thenCompose(response -> {
					try {
						return Promise.of(GsonAdapters.fromJson(FILE_META_LIST, response.getBody().asString(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public Promise<Void> delete(String glob) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DEL)
								.appendQuery("glob", glob)
								.build()))
				.toVoid();
	}
}
