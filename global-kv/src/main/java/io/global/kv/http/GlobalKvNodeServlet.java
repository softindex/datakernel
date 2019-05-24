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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.RawKvItem;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.datakernel.csp.binary.ByteBufsParser.ofDecoder;
import static io.datakernel.http.AsyncServletWrapper.loadBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.kv.api.KvCommand.*;
import static io.global.kv.util.BinaryDataFormats.REGISTRY;

public final class GlobalKvNodeServlet {
	static final StructuredCodec<SignedData<RawKvItem>> KV_ITEM_CODEC = REGISTRY.get(new TypeT<SignedData<RawKvItem>>() {});
	static final ByteBufsParser<SignedData<RawKvItem>> KV_ITEM_PARSER = ofDecoder(KV_ITEM_CODEC);
	static final StructuredCodec<Set<String>> SET_STRING_CODEC = ofSet(STRING_CODEC);

	public static RoutingServlet create(GlobalKvNode node) {
		return RoutingServlet.create()
				.with(POST, "/" + UPLOAD + "/:space/:table", request -> {
					String parameterSpace = request.getPathParameter("space");
					String table = request.getPathParameter("table");
					if (parameterSpace == null || table == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return node.upload(space, table)
								.map(consumer ->
										BinaryChannelSupplier.of(bodyStream)
												.parseStream(KV_ITEM_PARSER)
												.streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + DOWNLOAD + "/:space/:table", request -> {
					String parameterSpace = request.getPathParameter("space");
					String table = request.getPathParameter("table");
					if (parameterSpace == null || table == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						long offset;
						try {
							String offsetParam = request.getQueryParameter("offset");
							offset = Long.parseUnsignedLong(offsetParam != null ? offsetParam : "0");
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return node.download(space, table, offset)
								.map(supplier ->
										HttpResponse.ok200()
												.withBodyStream(supplier.map(signedDbItem -> encodeWithSizePrefix(KV_ITEM_CODEC, signedDbItem))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + GET_ITEM + "/:space/:table", loadBody()
						.then(request -> {
							ByteBuf body = request.getBody();
							String parameterSpace = request.getPathParameter("space");
							String table = request.getPathParameter("table");
							if (parameterSpace == null || table == null) {
								return Promise.ofException(new ParseException());
							}
							try {
								PubKey space = PubKey.fromString(parameterSpace);
								return node.get(space, table, body.asArray())
										.map(item ->
												HttpResponse.ok200().withBody(encode(KV_ITEM_CODEC, item)));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(PUT, "/" + PUT_ITEM + "/:space/:table", loadBody()
						.then(request -> {
							ByteBuf body = request.getBody();
							String parameterSpace = request.getPathParameter("space");
							String table = request.getPathParameter("table");
							if (parameterSpace == null || table == null) {
								return Promise.ofException(new ParseException());
							}
							try {
								PubKey space = PubKey.fromString(parameterSpace);
								return node.put(space, table, decode(KV_ITEM_CODEC, body.slice()))
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(GET, "/" + LIST + "/:space", request -> {
					String parameterSpace = request.getPathParameter("space");
					if (parameterSpace == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						return node.list(space)
								.map(list ->
										HttpResponse.ok200()
												.withBody(encode(SET_STRING_CODEC, list)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}
}
