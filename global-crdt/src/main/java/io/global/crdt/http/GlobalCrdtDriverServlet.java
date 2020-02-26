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

package io.global.crdt.http;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.crdt.CrdtData;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.crdt.GlobalCrdtDriver;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.crdt.CrdtCommand.*;
import static io.global.crdt.http.GlobalCrdtNodeServlet.SET_STRING_CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public final class GlobalCrdtDriverServlet {
	public static <K extends Comparable<K>, V> RoutingServlet create(GlobalCrdtDriver<K, V> driver) {
		StructuredCodec<K> keyCodec = driver.getKeyCodec();

		@SuppressWarnings("RedundantTypeArguments") // does not compile without type args here for some reason
		StructuredCodec<CrdtData<K, V>> codec = StructuredCodecs.<CrdtData<K, V>, K, V>object(CrdtData::new,
				"key", CrdtData::getKey, keyCodec,
				"state", CrdtData::getState, driver.getStateCodec())
				.nullable();

		StructuredCodec<List<CrdtData<K, V>>> listCodec = ofList(codec);

		return RoutingServlet.create()
				.map(POST, "/" + UPLOAD + "/:repo", request -> {
					String repo = request.getPathParameter("repo");
					String key = request.getCookie("Key");
					if (key == null) {
						return Promise.ofException(new ParseException("No 'Key' cookie"));
					}
					String simKeyString = request.getCookie("Sim-Key");
					try {
						SimKey simKey = simKeyString != null ? SimKey.fromString(simKeyString) : null;
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						List<CrdtData<K, V>> items = fromJson(listCodec, request.getBody().getString(UTF_8));
						return driver.upload(keys, repo, simKey)
								.then(consumer -> StreamSupplier.ofIterable(items).streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + DOWNLOAD + "/:space/:repo", request -> {
					String parameterSpace = request.getPathParameter("space");
					String repo = request.getPathParameter("repo");
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
						return driver.download(space, repo, offset, simKey)
								.then(supplier -> supplier.toCollector(toList()))
								.map(items -> HttpResponse.ok200().withJson(JsonUtils.toJson(listCodec, items)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
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
