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

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.kv.GlobalKvDriver;
import io.datakernel.kv.KvItem;

import java.util.List;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.kv.api.KvCommand.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class GlobalKvDriverServlet {
	static final StructuredCodec<Set<String>> SET_STRING_CODEC = ofSet(STRING_CODEC);

	public static <K, V> RoutingServlet create(GlobalKvDriver<K, V> driver) {
		StructuredCodec<K> keyCodec = driver.getKeyCodec();
		//noinspection RedundantTypeArguments
		StructuredCodec<KvItem<K, V>> codec = StructuredCodecs.<KvItem<K, V>, Long, K, V>object(KvItem::new,
				"timestamp", KvItem::getTimestamp, LONG_CODEC,
				"key", KvItem::getKey, keyCodec,
				"value", KvItem::getValue, driver.getValueCodec())
				.nullable();
		StructuredCodec<List<KvItem<K, V>>> listCodec = ofList(codec);
		return RoutingServlet.create()
				.map(POST, "/" + UPLOAD + "/:table", request -> {
					String table = request.getPathParameter("table");
					String key = request.getCookie("Key");
					if (key == null) {
						return Promise.ofException(new ParseException("No 'Key' cookie"));
					}
					String simKeyString = request.getCookie("Sim-Key");
					try {
						SimKey simKey = simKeyString != null ? SimKey.fromString(simKeyString) : null;
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						List<KvItem<K, V>> items = fromJson(listCodec, request.getBody().getString(UTF_8));
						return driver.upload(keys, table, simKey)
								.map(consumer -> consumer.acceptAll(items))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + DOWNLOAD + "/:space/:table", request -> {
					String parameterSpace = request.getPathParameter("space");
					String table = request.getPathParameter("table");
					String simKeyString = request.getCookie("Sim-Key");
					try {
						SimKey simKey = simKeyString != null ? SimKey.fromString(simKeyString) : null;
						PubKey space = PubKey.fromString(parameterSpace);
						long offset;
						try {
							String offsetParam = request.getQueryParameter("offset");
							offset = Long.parseUnsignedLong(offsetParam != null ? offsetParam : "0");
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return driver.download(space, table, offset, simKey)
								.then(ChannelSupplier::toList)
								.map(items -> HttpResponse.ok200().withJson(JsonUtils.toJson(listCodec, items)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + GET_ITEM + "/:space/:table", loadBody()
						.serve(request -> {
							String parameterSpace = request.getPathParameter("space");
							String table = request.getPathParameter("table");
							String key = request.getQueryParameter("key");
							if (key == null) {
								return Promise.ofException(new ParseException("No 'key' query parameter"));
							}
							String simKeyString = request.getCookie("Sim-Key");
							try {
								SimKey simKey = simKeyString != null ? SimKey.fromString(simKeyString) : null;
								PubKey space = PubKey.fromString(parameterSpace);
								return driver.get(space, table, fromJson(keyCodec, key), simKey)
										.map(item -> HttpResponse.ok200().withJson(JsonUtils.toJson(codec, item)));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(PUT, "/" + PUT_ITEM + "/:table", loadBody()
						.serve(request -> {
							String table = request.getPathParameter("table");
							String key = request.getCookie("Key");
							if (key == null) {
								return Promise.ofException(new ParseException("No 'Key' cookie"));
							}
							String simKeyString = request.getCookie("Sim-Key");
							try {
								SimKey simKey = simKeyString != null ? SimKey.fromString(simKeyString) : null;
								KeyPair keys = PrivKey.fromString(key).computeKeys();
								return driver.put(keys, table, fromJson(codec, request.getBody().getString(UTF_8)), simKey)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/" + LIST + "/:space", request -> {
					String parameterSpace = request.getPathParameter("space");
					try {
						return driver.list(PubKey.fromString(parameterSpace))
								.map(list -> HttpResponse.ok200().withJson(JsonUtils.toJson(SET_STRING_CODEC, list)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}
}
