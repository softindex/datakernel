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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.util.TypeT;
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
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.api.DiscoveryCommand.*;

public final class DiscoveryServlet implements AsyncServlet {
	private final RoutingServlet servlet;

	static final StructuredCodec<SignedData<AnnounceData>> SIGNED_ANNOUNCE = REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});
	static final StructuredCodec<SignedData<SharedSimKey>> SIGNED_SHARED_SIM_KEY = REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});
	static final StructuredCodec<@Nullable SignedData<SharedSimKey>> NULLABLE_SIGNED_SHARED_SIM_KEY = SIGNED_SHARED_SIM_KEY.nullable();
	static final StructuredCodec<List<SignedData<SharedSimKey>>> LIST_OF_SIGNED_SHARED_SIM_KEYS = REGISTRY.get(new TypeT<List<SignedData<SharedSimKey>>>() {});

	private DiscoveryServlet(DiscoveryService discoveryService) {
		servlet = servlet(discoveryService);
	}

	public static DiscoveryServlet create(DiscoveryService discoveryService) {
		return new DiscoveryServlet(discoveryService);
	}

	private RoutingServlet servlet(DiscoveryService discoveryService) {
		return RoutingServlet.create()
				.with(HttpMethod.PUT, "/" + ANNOUNCE + "/:owner", loadBody()
						.serve(request -> {
									ByteBuf body = request.getBody();

									String parameterOwner = request.getPathParameter("owner");
									if (parameterOwner == null) {
										return Promise.ofException(new ParseException());
									}
									try {
										PubKey owner = PubKey.fromString(parameterOwner);
										SignedData<AnnounceData> announceData = decode(SIGNED_ANNOUNCE, body.slice());
										return discoveryService.announce(owner, announceData)
												.map($ -> HttpResponse.ok201());
									} catch (ParseException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								}
						))
				.with(HttpMethod.GET, "/" + FIND + "/:owner", request -> {
					String owner = request.getPathParameter("owner");
					if (owner == null) {
						return Promise.ofException(new ParseException());
					}
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
						return Promise.ofException(e);
					}
				})
				.with(HttpMethod.POST, "/" + SHARE_KEY + "/:receiver", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							String parameterReceiver = request.getPathParameter("receiver");
							if (parameterReceiver == null) {
								return Promise.<HttpResponse>ofException(new ParseException());
							}
							try {
								PubKey receiver = PubKey.fromString(parameterReceiver);
								SignedData<SharedSimKey> simKey = decode(SIGNED_SHARED_SIM_KEY, body.slice());
								return discoveryService.shareKey(receiver, simKey)
										.map($ -> HttpResponse.ok201());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(HttpMethod.GET, "/" + GET_SHARED_KEY + "/:receiver/:hash", request -> {
					try {
						String parameterReceiver = request.getPathParameter("receiver");
						String parameterHash = request.getPathParameter("hash");
						if (parameterReceiver == null || parameterHash == null) {
							return Promise.ofException(new ParseException());
						}
						PubKey receiver = PubKey.fromString(parameterReceiver);
						Hash simKeyHash = Hash.fromString(parameterHash);
						return discoveryService.getSharedKey(receiver, simKeyHash)
								.map(signedSharedKey ->
										HttpResponse.ok200()
												.withBody(encode(NULLABLE_SIGNED_SHARED_SIM_KEY, signedSharedKey)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(HttpMethod.GET, "/" + GET_SHARED_KEYS + "/:receiver", request -> {
					String receiver = request.getPathParameter("receiver");
					if (receiver == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						return discoveryService.getSharedKeys(PubKey.fromString(receiver))
								.map(signedSharedKeys ->
										HttpResponse.ok200()
												.withBody(encode(LIST_OF_SIGNED_SHARED_SIM_KEYS, signedSharedKeys)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return servlet.serve(request);
	}
}
