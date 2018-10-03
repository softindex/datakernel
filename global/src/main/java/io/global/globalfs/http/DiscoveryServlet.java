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
import io.datakernel.http.*;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;

public final class DiscoveryServlet {
	public static final String FIND = "find";
	public static final String ANNOUNCE = "announce";

	// region creators
	private DiscoveryServlet() {
		throw new AssertionError("nope.");
	}

	public static AsyncServlet wrap(DiscoveryService service) {
		return MiddlewareServlet.create()
				.with(HttpMethod.GET, "/" + FIND, request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					return service.findServers(pubKey)
							.thenCompose(data -> {
								if (data != null) {
									return Stage.of(HttpResponse.ok200().withBody(data.toBytes()));
								}
								return Stage.ofException(HttpException.notFound404());
							});
				})
				.with(HttpMethod.PUT, "/" + ANNOUNCE, request -> {
					PubKey pubKey = PubKey.fromString(request.getQueryParameter("key"));
					return request.getBodyStage(Integer.MAX_VALUE)
							.thenCompose(body -> {
								try {
									return service.announce(pubKey, SignedData.ofBytes(body.getArray(), AnnounceData::fromBytes));
								} catch (ParseException e) {
									return Stage.ofException(e);
								}
							})
							.thenApply($ -> HttpResponse.ok200());

				});
	}
	// endregion
}
