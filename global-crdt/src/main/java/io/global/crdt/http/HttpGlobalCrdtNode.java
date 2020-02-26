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

package io.global.crdt.http;

import io.datakernel.common.parse.ParseException;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.http.UrlBuilder;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.crdt.GlobalCrdtNode;
import io.global.crdt.RawCrdtData;

import java.util.Set;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.global.crdt.CrdtCommand.*;
import static io.global.crdt.http.GlobalCrdtNodeServlet.*;

public final class HttpGlobalCrdtNode implements GlobalCrdtNode {
	private static final String CRDT_NODE_SUFFIX = "/crdt/";

	public final String url;
	private final IAsyncHttpClient client;

	private HttpGlobalCrdtNode(String url, IAsyncHttpClient client) {
		this.url = url + CRDT_NODE_SUFFIX;
		this.client = client;
	}

	public static HttpGlobalCrdtNode create(String url, IAsyncHttpClient client) {
		return new HttpGlobalCrdtNode(url, client);
	}

	@Override
	public Promise<StreamConsumer<SignedData<RawCrdtData>>> upload(PubKey space, String repo) {
		return Promise.of(StreamConsumer.ofSupplier(supplier ->
				client.request(
						HttpRequest.post(
								url + UrlBuilder.relative()
										.appendPathPart(UPLOAD)
										.appendPathPart(space.asString())
										.appendPathPart(repo)
										.build())
								.withBodyStream(supplier.transformWith(ChannelSerializer.create(RAW_CRDT_DATA_SERIALIZER))))
						.then(response -> response.getCode() == 200 ?
								Promise.complete() :
								Promise.ofException(HttpException.ofCode(response.getCode())))));
	}

	@Override
	public Promise<StreamSupplier<SignedData<RawCrdtData>>> download(PubKey space, String repo, long revision) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(DOWNLOAD)
								.appendPathPart(space.asString())
								.appendPathPart(repo)
								.appendQuery("revision", revision)
								.build()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.map(response ->
						response.getBodyStream().transformWith(ChannelDeserializer.create(RAW_CRDT_DATA_SERIALIZER)));
	}

	@Override
	public Promise<StreamConsumer<SignedData<byte[]>>> remove(PubKey space, String repo) {
		return Promise.of(StreamConsumer.ofSupplier(supplier ->
				client.request(
						HttpRequest.post(
								url + UrlBuilder.relative()
										.appendPathPart(REMOVE)
										.appendPathPart(space.asString())
										.appendPathPart(repo)
										.build())
								.withBodyStream(supplier.transformWith(ChannelSerializer.create(SIGNED_BYTES_SERIALIZER))))
						.then(response -> response.getCode() == 200 ?
								Promise.complete() :
								Promise.ofException(HttpException.ofCode(response.getCode())))));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(LIST)
								.appendPath(space.asString())
								.build()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.then(response -> response.loadBody()
						.then(body -> {
							try {
								return Promise.of(decode(SET_STRING_CODEC, body.slice()));
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}));
	}

	@Override
	public String toString() {
		return "HttpGlobalKvNode{" + url + "}";
	}
}
