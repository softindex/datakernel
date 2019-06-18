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

package io.global.fs.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.fs.api.FsCommand.*;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static io.global.fs.util.HttpDataFormats.*;

public final class GlobalFsNodeServlet {
	static final StructuredCodec<SignedData<GlobalFsCheckpoint>> SIGNED_CHECKPOINT_CODEC = REGISTRY.get(new TypeT<SignedData<GlobalFsCheckpoint>>() {});
	static final StructuredCodec<@Nullable SignedData<GlobalFsCheckpoint>> NULLABLE_SIGNED_CHECKPOINT_CODEC = SIGNED_CHECKPOINT_CODEC.nullable();

	public static RoutingServlet create(GlobalFsNode node) {
		return RoutingServlet.create()
				.with(POST, "/" + UPLOAD + "/:space/*", request -> {
					String parameterSpace = request.getPathParameter("space");
					if (parameterSpace == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						String path = request.getRelativePath();
						long offset = parseOffset(request);
						long revision = parseRevision(request);
						ChannelSupplier<ByteBuf> body = request.getBodyStream();
						return node.upload(space, path, offset, revision)
								.then(consumer -> body.streamTo(consumer.transformWith(new FrameDecoder())))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + DOWNLOAD + "/:space/*", request -> {
					String spaceParam = request.getPathParameter("space");
					if (spaceParam == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey space = PubKey.fromString(spaceParam);
						long[] range = parseRange(request);
						String path = request.getRelativePath();
						return node.download(space, path, range[0], range[1])
								.map(supplier ->
										HttpResponse.ok200()
												.withBodyStream(supplier.transformWith(new FrameEncoder())));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + LIST + "/:space/:name", request -> {
					String parameterSpace = request.getPathParameter("space");
					String glob = request.getPathParameter("glob");
					if (parameterSpace == null || glob == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						return node.listEntities(space, glob)
								.map(list ->
										HttpResponse.ok200()
												.withBodyStream(ChannelSupplier.ofStream(list.stream()
														.map(meta -> encodeWithSizePrefix(SIGNED_CHECKPOINT_CODEC, meta)))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + GET_METADATA + "/:space/*", request -> {
					String parameterSpace = request.getPathParameter("space");
					String path = request.getPathParameter("path");
					if (parameterSpace == null || path == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						return node.getMetadata(space, request.getRelativePath())
								.then(meta ->
										Promise.of(HttpResponse.ok200()
												.withBody(encode(NULLABLE_SIGNED_CHECKPOINT_CODEC, meta))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + DELETE + "/:space", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							String parameterSpace = request.getPathParameter("space");
							if (parameterSpace == null) {
								return Promise.<HttpResponse>ofException(new ParseException());
							}
							try {
								PubKey space = PubKey.fromString(parameterSpace);
								SignedData<GlobalFsCheckpoint> checkpoint = decode(SIGNED_CHECKPOINT_CODEC, body.getArray());
								return node.delete(space, checkpoint)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.<HttpResponse>ofException(e);
							}
						}));
	}
}
