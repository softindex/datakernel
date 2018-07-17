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

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.StacklessException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.http.UrlBuilder;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.csp.binary.ByteBufsParser.ofVarIntSizePrefixedBytes;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static io.global.fs.http.GlobalFsNodeServlet.*;
import static io.global.ot.util.BinaryDataFormats2.REGISTRY;
import static java.util.stream.Collectors.toList;

public final class HttpGlobalFsNode implements GlobalFsNode {
	private static final StructuredCodec<SignedData<GlobalFsMetadata>> SIGNED_METADATA_CODEC = REGISTRY.get(new TypeT<SignedData<GlobalFsMetadata>>() {});

	private final InetSocketAddress address;
	private final IAsyncHttpClient client;

	// region creators
	public HttpGlobalFsNode(IAsyncHttpClient client, InetSocketAddress address) {
		this.client = client;
		this.address = address;
	}
	// endregion

	@Override
	public Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long limit) {
		return client.request(
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DOWNLOAD)
								.appendPathPart(space.asString())
								.appendPath(filename)
								.appendQuery("range", offset + (limit != -1 ? "-" + (offset + limit) : ""))
								.build()))
				.thenApply(response -> {
					if (response.getCode() != 200) {
						throw new UncheckedException(new StacklessException(HttpGlobalFsNode.class, "Response code is not 200"));
					}
					return response.getBodyStream().transformWith(new FrameDecoder());
				});
	}

	@Override
	public ChannelConsumer<DataFrame> uploader(PubKey space, String filename, long offset) {
		ChannelZeroBuffer<DataFrame> buffer = new ChannelZeroBuffer<>();
		MaterializedPromise<HttpResponse> request = client.request(HttpRequest.post(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(UPLOAD)
						.appendPathPart(space.asString())
						.appendPath(filename)
						.appendQuery("offset", "" + offset)
						.build())
				.withBodyStream(buffer.getSupplier().transformWith(new FrameEncoder())))
				.thenCompose(ensureResponseBody())
				.materialize();
		return buffer.getConsumer().withAcknowledgement(ack -> ack.both(request));
	}

	@Override
	public Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset) {
		return Promise.of(uploader(space, filename, offset));
	}

	public static final ByteBufsParser<SignedData<GlobalFsMetadata>> SIGNED_METADATA_PARSER =
			ofVarIntSizePrefixedBytes()
					.andThen(buf -> decode(SIGNED_METADATA_CODEC, buf));

	private static final Function<HttpResponse, Promise<List<SignedData<GlobalFsMetadata>>>> LIST_RESPONSE_PARSER =
			response ->
					BinaryChannelSupplier.of(response.getBodyStream())
							.parseStream(SIGNED_METADATA_PARSER)
							.toCollector(toList());

	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> list(PubKey space, String glob) {
		return client.request(HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(LIST)
						.appendPathPart(space.asString())
						.appendQuery("glob", glob)
						.build()))
				.thenCompose(ensureResponseBody())
				.thenCompose(LIST_RESPONSE_PARSER);
	}

	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> listLocal(PubKey space, String glob) {
		return client.request(HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(LIST)
						.appendPathPart(space.asString())
						.appendQuery("glob", glob)
						.appendQuery("local", "1")
						.build()))
				.thenCompose(ensureResponseBody())
				.thenCompose(LIST_RESPONSE_PARSER);
	}

	@Override
	public Promise<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata) {
		return client.request(HttpRequest.post(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(PUSH)
						.appendPathPart(pubKey.asString())
						.build())
				.withBody(encode(SIGNED_METADATA_CODEC, signedMetadata)))
				.toVoid();
	}

	// @Override
	// public Promise<Set<String>> copy(RepoID name, Map<String, String> changes) {
	// 	return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
	// 			UrlBuilder.http()
	// 					.withAuthority(address)
	// 					.appendPathPart(COPY)
	// 					.appendPathPart(name.getOwner().asString())
	// 					.appendPathPart(name.getName())
	// 					.build())
	// 			.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
	// 			.thenCompose(response -> {
	// 				try {
	// 					return Promise.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
	// 				} catch (ParseException e) {
	// 					return Promise.ofException(e);
	// 				}
	// 			});
	// }
	//
	// @Override
	// public Promise<Set<String>> move(RepoID name, Map<String, String> changes) {
	// 	return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
	// 			UrlBuilder.http()
	// 					.withAuthority(address)
	// 					.appendPathPart(MOVE)
	// 					.appendPathPart(name.getOwner().asString())
	// 					.appendPathPart(name.getName())
	// 					.build())
	// 			.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
	// 			.thenCompose(response -> {
	// 				try {
	// 					return Promise.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
	// 				} catch (ParseException e) {
	// 					return Promise.ofException(e);
	// 				}
	// 			});
	// }
}
