/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.ApplicationSettings;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.http.HttpResponse.ok200;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.api.DiscoveryCommand.*;
import static io.global.util.Utils.PUB_KEYS_MAP;

public final class DiscoveryServlet implements AsyncServlet {
	static final StructuredCodec<SignedData<AnnounceData>> SIGNED_ANNOUNCE = REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});
	static final StructuredCodec<SignedData<SharedSimKey>> SIGNED_SHARED_SIM_KEY = REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});
	static final StructuredCodec<@Nullable SignedData<SharedSimKey>> NULLABLE_SIGNED_SHARED_SIM_KEY = SIGNED_SHARED_SIM_KEY.nullable();
	static final StructuredCodec<List<SignedData<SharedSimKey>>> LIST_OF_SIGNED_SHARED_SIM_KEYS = REGISTRY.get(new TypeT<List<SignedData<SharedSimKey>>>() {});
	private static final String BASIC_AUTH_LOGIN = ApplicationSettings.getString(DiscoveryServlet.class, "basic.login", "admin");
	private static final String BASIC_AUTH_PASSWORD = ApplicationSettings.getString(DiscoveryServlet.class, "basic.passoword", "admin");
	private final RoutingServlet servlet;

	private DiscoveryServlet(DiscoveryService discoveryService) {
		servlet = servlet(discoveryService);
	}

	public static DiscoveryServlet create(DiscoveryService discoveryService) {
		return new DiscoveryServlet(discoveryService);
	}

	private RoutingServlet servlet(DiscoveryService discoveryService) {
		return RoutingServlet.create()
				.map(PUT, "/" + ANNOUNCE + "/:owner", loadBody().serve(
						request -> {
							ByteBuf body = request.getBody();

							String parameterOwner = request.getPathParameter("owner");
							try {
								PubKey owner = PubKey.fromString(parameterOwner);
								SignedData<AnnounceData> announceData = decode(SIGNED_ANNOUNCE, body.slice());
								return discoveryService.announce(owner, announceData)
										.map($ -> HttpResponse.ok201());
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}
				))
				.map(GET, "/" + FIND + "/:owner", request -> {
					String owner = request.getPathParameter("owner");
					try {
						return discoveryService.find(PubKey.fromString(owner))
								.thenEx((data, e) -> {
									if (e != null || data == null) {
										return Promise.<HttpResponse>ofException(HttpException.notFound404());
									}
									return Promise.of(HttpResponse.ok200()
											.withBody(encode(SIGNED_ANNOUNCE, data)));
								});
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(POST, "/" + SHARE_KEY + "/:receiver", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							String parameterReceiver = request.getPathParameter("receiver");
							try {
								PubKey receiver = PubKey.fromString(parameterReceiver);
								SignedData<SharedSimKey> simKey = decode(SIGNED_SHARED_SIM_KEY, body.slice());
								return discoveryService.shareKey(receiver, simKey)
										.map($ -> HttpResponse.ok201());
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}))
				.map(GET, "/" + GET_SHARED_KEY + "/:receiver/:hash", request -> {
					try {
						String parameterReceiver = request.getPathParameter("receiver");
						String parameterHash = request.getPathParameter("hash");
						PubKey receiver = PubKey.fromString(parameterReceiver);
						Hash simKeyHash = Hash.fromString(parameterHash);
						return discoveryService.getSharedKey(receiver, simKeyHash)
								.map(signedSharedKey ->
										HttpResponse.ok200()
												.withBody(encode(NULLABLE_SIGNED_SHARED_SIM_KEY, signedSharedKey)));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(GET, "/" + GET_SHARED_KEYS + "/:receiver", request -> {
					String receiver = request.getPathParameter("receiver");
					try {
						return discoveryService.getSharedKeys(PubKey.fromString(receiver))
								.map(signedSharedKeys ->
										HttpResponse.ok200()
												.withBody(encode(LIST_OF_SIGNED_SHARED_SIM_KEYS, signedSharedKeys)));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(GET, "/" + FIND_ALL + "/", BasicAuth.decorator("list pub keys",
						(l, p) -> Promise.of(BASIC_AUTH_LOGIN.equals(l) && BASIC_AUTH_PASSWORD.equals(p)))
						.serve(request -> discoveryService.findAll()
								.map(pubKeys -> ok200()
										.withBody(encode(PUB_KEYS_MAP, pubKeys))))
				);
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return servlet.serve(request);
	}
}
