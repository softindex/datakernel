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
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpHeaders;
import io.datakernel.http.HttpMethod;
import io.datakernel.http.HttpRequest;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsName;

import java.io.IOException;

public final class HttpDiscoveryService implements DiscoveryService {
	public final String test = "";
	private final AsyncHttpClient client;
	private final String host;

	// region creators
	public HttpDiscoveryService(AsyncHttpClient client, String host) {
		this.client = client;
		this.host = host;
	}
	// endregion

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		return client.request(HttpRequest.get(host + DiscoveryServlet.FIND + "?key=" + GlobalFsName.serializePubKey(pubKey)))
				.thenCompose(data -> {
					try {
						return Stage.of(SignedData.ofBytes(data.getBody().asArray(), AnnounceData::fromBytes));
					} catch (IOException e) {
						return Stage.ofException(e);
					}
				});
	}

	@Override
	public Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData) {
		return client.request(HttpRequest.of(HttpMethod.PUT, host + DiscoveryServlet.ANNOUNCE + "?key=" + GlobalFsName.serializePubKey(pubKey))
				.withBody(announceData.toBytes())
				.withHeader(HttpHeaders.HOST, host))
				.toVoid();
	}
}
