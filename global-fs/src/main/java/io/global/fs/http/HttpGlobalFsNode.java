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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.csp.binary.ByteBufsParser.ofDecoder;
import static io.global.fs.api.FsCommand.*;
import static io.global.fs.http.GlobalFsNodeServlet.NULLABLE_SIGNED_CHECKPOINT_CODEC;
import static io.global.fs.http.GlobalFsNodeServlet.SIGNED_CHECKPOINT_CODEC;
import static java.util.stream.Collectors.toList;

public final class HttpGlobalFsNode implements GlobalFsNode {
	private static final String FS_NODE_SUFFIX = "/fs/";

	private final String url;
	private final IAsyncHttpClient client;

	private HttpGlobalFsNode(String url, IAsyncHttpClient client) {
		this.url = url + FS_NODE_SUFFIX;
		this.client = client;
	}

	public static HttpGlobalFsNode create(String url, IAsyncHttpClient client) {
		return new HttpGlobalFsNode(url, client);
	}

	@Override
	public Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset, long revision) {
		SettablePromise<ChannelConsumer<DataFrame>> channelPromise = new SettablePromise<>();
		SettablePromise<HttpResponse> responsePromise = new SettablePromise<>();

		client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(UPLOAD)
								.appendPathPart(space.asString())
								.appendPath(filename)
								.appendQuery("offset", String.valueOf(offset))
								.appendQuery("revision", String.valueOf(revision))
								.build())
						.withBodyStream(ChannelSupplier.ofLazyProvider(() -> {
							ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
							channelPromise.trySet(buffer.getConsumer()
									.transformWith(new FrameEncoder())
									.withAcknowledgement(ack -> ack.both(responsePromise)));
							return buffer.getSupplier();
						})))
				.then(response -> response.getCode() != 200 && response.getCode() != 201 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.whenException(channelPromise::trySetException)
				.whenComplete(responsePromise::trySet);

		return channelPromise;
	}

	@Override
	public Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long limit) {
		return client.request(HttpRequest.get(
				url + UrlBuilder.relative()
						.appendPathPart(DOWNLOAD)
						.appendPathPart(space.asString())
						.appendPath(filename)
						.appendQuery("range", offset + (limit != -1 ? "-" + (offset + limit) : ""))
						.build()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.map(response -> response.getBodyStream().transformWith(new FrameDecoder()));
	}

	public static final ByteBufsParser<SignedData<GlobalFsCheckpoint>> SIGNED_CHECKPOINT_PARSER = ofDecoder(SIGNED_CHECKPOINT_CODEC);

	@Override
	public Promise<List<SignedData<GlobalFsCheckpoint>>> listEntities(PubKey space, String glob) {
		return client.request(HttpRequest.get(
				url + UrlBuilder.relative()
						.appendPathPart(LIST)
						.appendPathPart(space.asString())
						.appendQuery("glob", glob)
						.build()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.then(response ->
						BinaryChannelSupplier.of(response.getBodyStream())
								.parseStream(SIGNED_CHECKPOINT_PARSER)
								.toCollector(toList()));
	}

	@Override
	public Promise<@Nullable SignedData<GlobalFsCheckpoint>> getMetadata(PubKey space, String filename) {
		return client.request(HttpRequest.get(
				url + UrlBuilder.relative()
						.appendPathPart(GET_METADATA)
						.appendPathPart(space.asString())
						.appendPath(filename)
						.build()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.then(response -> response.loadBody()
						.then(body -> {
							try {
								return Promise.of(decode(NULLABLE_SIGNED_CHECKPOINT_CODEC, body.slice()));
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}));
	}

	@Override
	public Promise<Void> delete(PubKey space, SignedData<GlobalFsCheckpoint> tombstone) {
		return client.request(HttpRequest.post(
				url + UrlBuilder.relative()
						.appendPathPart(DELETE)
						.appendPathPart(space.asString())
						.build())
				.withBody(encode(SIGNED_CHECKPOINT_CODEC, tombstone).asArray()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.toVoid();
	}

	@Override
	public String toString() {
		return "HttpGlobalFsNode{" + url + '}';
	}
}
