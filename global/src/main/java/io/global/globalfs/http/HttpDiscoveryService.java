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

import io.datakernel.async.Stage;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpMethod;
import io.datakernel.http.HttpRequest;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;

import java.net.InetSocketAddress;

import static io.global.globalfs.http.DiscoveryServlet.ANNOUNCE;
import static io.global.globalfs.http.DiscoveryServlet.FIND;
import static io.global.globalfs.http.UrlBuilder.query;

public final class HttpDiscoveryService implements DiscoveryService {
	private final AsyncHttpClient client;
	private final InetSocketAddress address;

	// region creators
	public HttpDiscoveryService(AsyncHttpClient client, InetSocketAddress address) {
		this.client = client;
		this.address = address;
	}
	// endregion

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get(
				UrlBuilder.http(FIND)
						.withAuthority(address)
						.withQuery(query().with("key", pubKey.asString()))
						.build()))
				.thenCompose(response -> {
					if (response.getCode() == 404) {
						return Stage.of(null);
					}
					try {
						return Stage.of(SignedData.ofBytes(response.getBody().asArray(), AnnounceData::fromBytes));
					} catch (ParseException e) {
						return Stage.ofException(e);
					}
				});
	}

	@Override
	public Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData) {
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.of(HttpMethod.PUT,
				UrlBuilder.http(ANNOUNCE)
						.withAuthority(address)
						.withQuery(query().with("key", pubKey.asString()))
						.build())
				.withBody(announceData.toBytes()))
				.toVoid();
	}
}
