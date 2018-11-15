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
import io.datakernel.functional.Try;
import io.datakernel.http.*;
import io.datakernel.json.GsonAdapters;
import io.datakernel.util.ParserFunction;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static io.datakernel.http.IAsyncHttpClient.ensureStatusCode;
import static io.global.fs.http.DiscoveryServlet.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpDiscoveryService implements DiscoveryService {
	private final IAsyncHttpClient client;
	private final InetSocketAddress address;

	private HttpDiscoveryService(InetSocketAddress address, IAsyncHttpClient client) {
		this.client = client;
		this.address = address;
	}

	public static HttpDiscoveryService create(InetSocketAddress address, IAsyncHttpClient client) {
		return new HttpDiscoveryService(address, client);
	}

	@Override
	public Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData) {
		return client.request(
				HttpRequest.of(HttpMethod.PUT,
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(ANNOUNCE_ALL)
								.appendPathPart(space.asString())
								.build())
						.withBody(SIGNED_ANNOUNCE.toJson(announceData).getBytes(UTF_8)))
				.thenCompose(ensureStatusCode(201))
				.toVoid();
	}

	private <T> Try<T> tryParseResponse(HttpResponse response, ParserFunction<ByteBuf, T> from) {
		switch (response.getCode()) {
			case 200:
				try {
					return Try.of(from.parse(response.getBody()));
				} catch (ParseException e) {
					return Try.ofException(e);
				}
			case 404:
				return Try.of(null);
			default:
				return Try.ofException(HttpException.ofCode(response.getCode(), response.getBody().getString(UTF_8)));
		}
	}

	@Override
	public Promise<SignedData<AnnounceData>> find(PubKey space) {
		return client.request(HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(FIND_ALL)
						.appendPathPart(space.asString())
						.build()))
				.thenCompose(ensureResponseBody())
				.thenCompose(response -> Promise.ofTry(
						tryParseResponse(response, body -> GsonAdapters.fromJson(SIGNED_ANNOUNCE, body.asString(UTF_8)))
								.flatMap(v -> v != null ? Try.of(v) : Try.ofException(NO_ANNOUNCE_DATA))));
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(SHARE_KEY)
								.appendPathPart(receiver.asString())
								.build())
						.withBody(SIGNED_SHARED_SIM_KEY.toJson(simKey).getBytes(UTF_8)))
				.thenCompose(ensureStatusCode(201))
				.toVoid();
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		return client.request(HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(GET_SHARED_KEY)
						.appendPathPart(receiver.asString())
						.appendPathPart(hash.asString())
						.build()))
				.thenCompose(ensureResponseBody())
				.thenCompose(response -> Promise.ofTry(
						tryParseResponse(response, body -> GsonAdapters.fromJson(SIGNED_SHARED_SIM_KEY, body.asString(UTF_8)))
								.flatMap(v -> v != null ? Try.of(v) : Try.ofException(NO_KEY))));
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return client.request(HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(GET_SHARED_KEY)
						.appendPathPart(receiver.asString())
						.build()))
				.thenCompose(ensureResponseBody())
				.thenCompose(response -> Promise.ofTry(
						tryParseResponse(response, body ->
								GsonAdapters.fromJson(LIST_OF_SIGNED_SHARED_SIM_KEYS, body.asString(UTF_8)))));
	}
}
