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
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.api.DiscoveryCommand.*;

public final class DiscoveryServlet implements WithMiddleware {
	private final MiddlewareServlet servlet;

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

	private MiddlewareServlet servlet(DiscoveryService discoveryService) {
		return MiddlewareServlet.create()
				.with(HttpMethod.PUT, "/" + ANNOUNCE + "/:owner", request -> request.getBody().then(body -> {
					try {
						PubKey owner = PubKey.fromString(request.getPathParameter("owner"));
						SignedData<AnnounceData> announceData = decode(SIGNED_ANNOUNCE, body.slice());
						return discoveryService.announce(owner, announceData)
								.map($ -> HttpResponse.ok201());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(HttpMethod.GET, "/" + FIND + "/:owner", request -> {
					try {
						return discoveryService.find(PubKey.fromString(request.getPathParameter("owner")))
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
				.with(HttpMethod.POST, "/" + SHARE_KEY + "/:receiver", request -> request.getBody().then(body -> {
					try {
						PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
						SignedData<SharedSimKey> simKey = decode(SIGNED_SHARED_SIM_KEY, body.slice());
						return discoveryService.shareKey(receiver, simKey)
								.map($ -> HttpResponse.ok201());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(HttpMethod.GET, "/" + GET_SHARED_KEY + "/:receiver/:hash", request -> {
					try {
						PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
						Hash simKeyHash = Hash.fromString(request.getPathParameter("hash"));
						return discoveryService.getSharedKey(receiver, simKeyHash)
								.map(signedSharedKey ->
										HttpResponse.ok200()
												.withBody(encode(NULLABLE_SIGNED_SHARED_SIM_KEY, signedSharedKey)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(HttpMethod.GET, "/" + GET_SHARED_KEYS + "/:receiver", request -> {
					try {
						return discoveryService.getSharedKeys(PubKey.fromString(request.getPathParameter("receiver")))
								.map(signedSharedKeys ->
										HttpResponse.ok200()
												.withBody(encode(LIST_OF_SIGNED_SHARED_SIM_KEYS, signedSharedKeys)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
