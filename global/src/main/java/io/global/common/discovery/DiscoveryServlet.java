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

package io.global.common.discovery;

import com.google.gson.TypeAdapter;
import com.google.inject.Inject;
import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.*;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;

import java.io.IOException;
import java.util.List;

import static io.datakernel.json.GsonAdapters.ofList;
import static io.global.common.GlobalJsonAdapters.withSignature;
import static io.global.ot.util.BinaryDataFormats2.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DiscoveryServlet implements AsyncServlet {
	public static final String ANNOUNCE_ALL = "announceAll";
	public static final String ANNOUNCE = "announce";
	public static final String FIND = "find";
	public static final String FIND_ALL = "findAll";
	public static final String SHARE_KEY = "shareKey";
	public static final String GET_SHARED_KEY = "getSharedKey";
	public static final String GET_SHARED_KEYS = "getSharedKeys";

	private final AsyncServlet servlet;

	static final TypeAdapter<SignedData<AnnounceData>> SIGNED_ANNOUNCE = withSignature(REGISTRY.get(SignedData.class, AnnounceData.class));
	static final TypeAdapter<SignedData<SharedSimKey>> SIGNED_SHARED_SIM_KEY = withSignature(REGISTRY.get(SignedData.class, SharedSimKey.class));
	static final TypeAdapter<List<SignedData<SharedSimKey>>> LIST_OF_SIGNED_SHARED_SIM_KEYS = ofList(SIGNED_SHARED_SIM_KEY);

	@Inject
	public DiscoveryServlet(DiscoveryService discoveryService) {
		servlet = MiddlewareServlet.create()
				.with(HttpMethod.PUT, "/" + ANNOUNCE_ALL + "/:owner", request -> {
					PubKey owner = PubKey.fromString(request.getPathParameter("owner"));
					return request.getBodyPromise(Integer.MAX_VALUE)
							.thenCompose(body -> {
								try {
									return discoveryService.announce(owner, SIGNED_ANNOUNCE.fromJson(body.asString(UTF_8)));
								} catch (IOException e) {
									return Promise.ofException(e);
								}
							})
							.thenApply($ -> HttpResponse.ok201());

				})
				.with(HttpMethod.GET, "/" + FIND_ALL + "/:owner", request ->
						discoveryService.find(PubKey.fromString(request.getPathParameter("owner")))
								.thenComposeEx((data, e) ->
										e == null ?
												Promise.of(HttpResponse.ok200()
														.withBody(SIGNED_ANNOUNCE.toJson(data).getBytes(UTF_8))) :
												Promise.ofException(HttpException.notFound404())))
				.with(HttpMethod.POST, "/" + SHARE_KEY + "/:receiver", request -> {
					PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
					return request.getBodyPromise(Integer.MAX_VALUE)
							.thenCompose(body -> {
								try {
									return discoveryService.shareKey(receiver, SIGNED_SHARED_SIM_KEY.fromJson(body.asString(UTF_8)));
								} catch (IOException e) {
									return Promise.ofException(e);
								}
							})
							.thenApply($ -> HttpResponse.ok201());
				})
				.with(HttpMethod.GET, "/" + GET_SHARED_KEY + "/:receiver/:hash", request -> {
					PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
					Hash simKeyHash = Hash.parseString(request.getPathParameter("hash"));
					return discoveryService.getSharedKey(receiver, simKeyHash)
							.thenComposeEx((signedSharedKey, e) ->
									e == null ?
											Promise.of(HttpResponse.ok200()
													.withBody(SIGNED_SHARED_SIM_KEY.toJson(signedSharedKey).getBytes(UTF_8))) :
											Promise.ofException(HttpException.notFound404()));
				})
				.with(HttpMethod.GET, "/" + GET_SHARED_KEYS + "/:receiver", request ->
						discoveryService.getSharedKeys(PubKey.fromString(request.getPathParameter("receiver")))
								.thenApply(signedSharedKeys ->
										HttpResponse.ok200()
												.withBody(LIST_OF_SIGNED_SHARED_SIM_KEYS.toJson(signedSharedKeys).getBytes(UTF_8))));
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws ParseException, UncheckedException {
		return servlet.serve(request);
	}
}
