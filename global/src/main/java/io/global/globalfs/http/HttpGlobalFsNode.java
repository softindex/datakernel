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

package io.global.globalfs.http;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.global.common.RawServerId;
import io.global.globalfs.api.*;
import io.global.globalfs.transformers.FrameDecoder;
import io.global.globalfs.transformers.FrameEncoder;
import io.global.globalsync.util.BinaryDataFormats;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.http.HttpMethod.DELETE;
import static io.global.globalfs.http.GlobalFsNodeServlet.*;
import static io.global.globalfs.http.UrlBuilder.query;

public final class HttpGlobalFsNode implements GlobalFsNode {
	private final RawServerId id;
	private final InetSocketAddress address;
	private final AsyncHttpClient client;

	// region creators
	public HttpGlobalFsNode(RawServerId id, AsyncHttpClient client) {
		this.id = id;
		this.client = client;
		this.address = id.getInetSocketAddress();
	}
	// endregion

	@Override
	public RawServerId getId() {
		return id;
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsPath file, long offset, long limit) {
		return client.request(
				HttpRequest.get(
						UrlBuilder.http(DOWNLOAD)
								.withAuthority(address)
								.withQuery(query()
										.with("key", file.getPubKey().asString())
										.with("fs", file.getFsName())
										.with("path", file.getPath())
										.with("offset", offset)
										.with("limit", limit))
								.build()))
				.thenCompose(response -> {
					if (response.getCode() != 200) {
						return Stage.ofException(new GlobalFsException("response: " + response));
					}
					return Stage.of(response);
				})
				.thenApply(response -> response.getBodyStream().apply(new FrameDecoder()));
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath file, long offset) {
		SerialZeroBuffer<DataFrame> buffer = new SerialZeroBuffer<>();
		MaterializedStage<Void> request = client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
				UrlBuilder.http(UPLOAD)
						.withAuthority(address)
						.withQuery(query()
								.with("key", file.getPubKey().asString())
								.with("fs", file.getFsName())
								.with("path", file.getPath())
								.with("offset", offset))
						.build())
				.withBodyStream(buffer.getSupplier().apply(new FrameEncoder())))
				.thenCompose(response -> {
					if (response.getCode() != 200) {
						return Stage.ofException(new GlobalFsException("response: " + response));
					}
					return Stage.complete();
				})
				.materialize();
		return Stage.of(buffer.getConsumer().withAcknowledgement(ack -> ack.both(request)));
	}

	@Override
	public Stage<List<GlobalFsMetadata>> list(GlobalFsName name, String glob) {
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get(
				UrlBuilder.http(LIST)
						.withAuthority(address)
						.withQuery(query()
								.with("key", name.getPubKey().asString())
								.with("fs", name.getFsName())
								.with("glob", glob))
						.build()))
				.thenCompose(response ->
						Stage.compute(() ->
								BinaryDataFormats.readList(response.getBody(), BinaryDataFormats::readGlobalFsMetadata)));
	}

	@Override
	public Stage<Void> delete(GlobalFsName name, String glob) {
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.of(DELETE,
				UrlBuilder.http(DEL)
						.withAuthority(address)
						.withQuery(query()
								.with("key", name.getPubKey().asString())
								.with("fs", name.getFsName())
								.with("glob", glob))
						.build()))
				.toVoid();
	}

	@Override
	public Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes) {
		ByteBuf buf = ByteBufPool.allocate(BinaryDataFormats.sizeof(changes, BinaryDataFormats::sizeof, BinaryDataFormats::sizeof));
		BinaryDataFormats.writeMap(buf, changes, BinaryDataFormats::writeString, BinaryDataFormats::writeString);
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
				UrlBuilder.http(COPY)
						.withAuthority(address)
						.withQuery(query()
								.with("key", name.getPubKey().asString())
								.with("fs", name.getFsName()))
						.build())
				.withBody(buf))
				.thenCompose(response ->
						Stage.compute(() ->
								BinaryDataFormats.readSet(response.getBody(), BinaryDataFormats::readString)));
	}

	@Override
	public Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes) {
		ByteBuf buf = ByteBufPool.allocate(BinaryDataFormats.sizeof(changes, BinaryDataFormats::sizeof, BinaryDataFormats::sizeof));
		BinaryDataFormats.writeMap(buf, changes, BinaryDataFormats::writeString, BinaryDataFormats::writeString);
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
				UrlBuilder.http(MOVE)
						.withAuthority(address)
						.withQuery(query()
								.with("key", name.getPubKey().asString())
								.with("fs", name.getFsName()))
						.build())
				.withBody(buf))
				.thenCompose(response ->
						Stage.compute(() ->
								BinaryDataFormats.readSet(response.getBody(), BinaryDataFormats::readString)));
	}
}
