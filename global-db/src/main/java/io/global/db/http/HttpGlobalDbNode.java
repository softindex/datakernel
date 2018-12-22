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

package io.global.db.http;

import io.datakernel.async.MaterializedPromise;
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
import io.global.db.DbItem;
import io.global.db.api.GlobalDbNode;
import io.global.db.api.TableID;

import java.util.List;

import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.global.db.api.DbCommand.*;
import static io.global.db.http.GlobalDbNodeServlet.*;

public final class HttpGlobalDbNode implements GlobalDbNode {
	public final String url;
	private final IAsyncHttpClient client;

	private HttpGlobalDbNode(String url, IAsyncHttpClient client) {
		this.url = url.endsWith("/") ? url : url + '/';
		this.client = client;
	}

	public static HttpGlobalDbNode create(String url, IAsyncHttpClient client) {
		return new HttpGlobalDbNode(url, client);
	}

	@Override
	public Promise<ChannelConsumer<SignedData<DbItem>>> upload(TableID tableID) {
		ChannelZeroBuffer<SignedData<DbItem>> buffer = new ChannelZeroBuffer<>();
		MaterializedPromise<HttpResponse> request = client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(UPLOAD)
								.appendPath(tableID.asString())
								.build())
						.withBodyStream(buffer.getSupplier()
								.map(signedDbItem -> encodeWithSizePrefix(DB_ITEM_CODEC, signedDbItem))))
				.thenCompose(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.materialize();
		return Promise.of(buffer.getConsumer().withAcknowledgement(ack -> ack.both(request)));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<DbItem>>> download(TableID tableID, long timestamp) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(DOWNLOAD)
								.appendPath(tableID.asString())
								.build()))
				.thenCompose(response1 -> response1.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response1.getCode())) : Promise.of(response1))
				.thenApply(response -> BinaryChannelSupplier.of(response.getBodyStream()).parseStream(DB_ITEM_PARSER));
	}

	@Override
	public Promise<SignedData<DbItem>> get(TableID tableID, byte[] key) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(GET_ITEM)
								.appendPath(tableID.asString())
								.build())
						.withBody(ByteBuf.wrapForReading(key)))
				.thenCompose(response1 -> response1.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response1.getCode())) : Promise.of(response1))
				.thenCompose(response -> response.getBody().thenApply(body -> {
					try {
						return decode(DB_ITEM_CODEC, body.slice());
					} catch (ParseException e) {
						throw new UncheckedException(e);
					} finally {
						body.recycle();
					}
				}));
	}

	@Override
	public Promise<Void> put(TableID tableID, SignedData<DbItem> item) {
		return client.request(
				HttpRequest.put(
						url + UrlBuilder.relative()
								.appendPathPart(PUT_ITEM)
								.appendPath(tableID.asString())
								.build())
						.withBody(encode(DB_ITEM_CODEC, item)))
				.thenCompose(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.toVoid();
	}

	@Override
	public Promise<List<String>> list(PubKey owner) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(LIST)
								.appendPath(owner.asString())
								.build()))
				.thenCompose(response1 -> response1.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response1.getCode())) : Promise.of(response1))
				.thenCompose(response -> response.getBody().thenCompose(body -> {
					try {
						return Promise.of(decode(LIST_STRING_CODEC, body.slice()));
					} catch (ParseException e) {
						return Promise.ofException(e);
					} finally {
						body.recycle();
					}
				}));
	}

	@Override
	public String toString() {
		return "HttpGlobalDbNode{url='" + url + "'}";
	}
}
