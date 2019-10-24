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

package io.global.kv.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.*;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.RawKvItem;

import java.util.Set;

import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.global.kv.api.KvCommand.*;
import static io.global.kv.http.GlobalKvNodeServlet.*;

public final class HttpGlobalKvNode implements GlobalKvNode {
	private static final String KV_NODE_SUFFIX = "/kv/";

	public final String url;
	private final IAsyncHttpClient client;

	private HttpGlobalKvNode(String url, IAsyncHttpClient client) {
		this.url = url + KV_NODE_SUFFIX;
		this.client = client;
	}

	public static HttpGlobalKvNode create(String url, IAsyncHttpClient client) {
		return new HttpGlobalKvNode(url, client);
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawKvItem>>> upload(PubKey space, String table) {
		ChannelZeroBuffer<SignedData<RawKvItem>> buffer = new ChannelZeroBuffer<>();
		Promise<HttpResponse> request = client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(UPLOAD)
								.appendPathPart(space.asString())
								.appendPathPart(table)
								.build())
						.withBodyStream(buffer.getSupplier()
								.map(signedDbItem -> encodeWithSizePrefix(KV_ITEM_CODEC, signedDbItem))))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response));
		return Promise.of(buffer.getConsumer().withAcknowledgement(ack -> ack.both(request)));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download(PubKey space, String table, long timestamp) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(DOWNLOAD)
								.appendPathPart(space.asString())
								.appendPathPart(table)
								.appendQuery("timestamp", timestamp)
								.build()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.map(response -> BinaryChannelSupplier.of(response.getBodyStream()).parseStream(KV_ITEM_PARSER));
	}

	@Override
	public Promise<SignedData<RawKvItem>> get(PubKey space, String table, byte[] key) {
		return client.request(HttpRequest.get(
				url + UrlBuilder.relative()
						.appendPathPart(GET_ITEM)
						.appendPathPart(space.asString())
						.appendPathPart(table)
						.build())
				.withBody(ByteBuf.wrapForReading(key)))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.then(response -> response.loadBody()
						.map(body -> {
							try {
								return decode(KV_ITEM_CODEC, body.slice());
							} catch (ParseException e) {
								throw new UncheckedException(e);
							}
						}));
	}

	@Override
	public Promise<Void> put(PubKey space, String table, SignedData<RawKvItem> item) {
		return client.request(
				HttpRequest.put(
						url + UrlBuilder.relative()
								.appendPathPart(PUT_ITEM)
								.appendPathPart(space.asString())
								.appendPathPart(table)
								.build())
						.withBody(encode(KV_ITEM_CODEC, item)))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.toVoid();
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
		return "HttpGlobalKvNode{url='" + url + "'}";
	}
}
