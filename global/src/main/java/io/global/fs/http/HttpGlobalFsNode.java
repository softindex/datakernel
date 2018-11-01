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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.http.UrlBuilder;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.RepoID;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.api.GlobalPath;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.serial.ByteBufsParser.ofVarIntSizePrefixedBytes;
import static io.global.fs.http.GlobalFsNodeServlet.*;
import static java.util.stream.Collectors.toList;

public final class HttpGlobalFsNode implements GlobalFsNode {
	private final RawServerId id;
	private final InetSocketAddress address;
	private final IAsyncHttpClient client;

	// region creators
	public HttpGlobalFsNode(RawServerId id, IAsyncHttpClient client) {
		this.id = id;
		this.client = client;
		this.address = id.getInetSocketAddress();
	}
	// endregion

	@Override
	public RawServerId getId() {
		return id;
	}

	@Override
	public Promise<SerialSupplier<DataFrame>> download(GlobalPath path, long offset, long limit) {
		return client.request(
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DOWNLOAD)
								.appendPathPart(path.getOwner().asString())
								.appendPathPart(path.getFs())
								.appendPath(path.getPath())
								.appendQuery("range", offset + (limit != -1 ? "-" + (offset + limit) : ""))
								.build()))
				.thenApply(response -> {
					if (response.getCode() != 200) {
						throw new UncheckedException(new StacklessException(HttpGlobalFsNode.class, "Response code is not 200"));
					}
					return response.getBodyStream().apply(new FrameDecoder());
				});
	}

	@Override
	public SerialConsumer<DataFrame> uploader(GlobalPath path, long offset) {
		SerialZeroBuffer<DataFrame> buffer = new SerialZeroBuffer<>();
		MaterializedPromise<HttpResponse> request = client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(UPLOAD)
						.appendPathPart(path.getOwner().asString())
						.appendPathPart(path.getFs())
						.appendPath(path.getPath())
						.appendQuery("offset", "" + offset)
						.build())
				.withBodyStream(buffer.getSupplier().apply(new FrameEncoder())))
				.materialize();
		return buffer.getConsumer().withAcknowledgement(ack -> ack.both(request));
	}

	@Override
	public Promise<SerialConsumer<DataFrame>> upload(GlobalPath path, long offset) {
		return Promise.of(uploader(path, offset));
	}

	@Override
	public Promise<List<SignedData<GlobalFsMetadata>>> list(RepoID repo, String glob) {
		PubKey pubKey = repo.getOwner();
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(LIST)
						.appendPathPart(pubKey.asString())
						.appendPathPart(repo.getName())
						.appendQuery("glob", glob)
						.build()))
				.thenCompose(response ->
						ByteBufsSupplier.of(response.getBodyStream())
								.parseStream(ofVarIntSizePrefixedBytes())
								.transform(buf -> {
									try {
										return SignedData.ofBytes(buf.asArray(), GlobalFsMetadata::fromBytes);
									} catch (ParseException e) {
										throw new UncheckedException(e);
									}
								}).toCollector(toList()));
	}

	@Override
	public Promise<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata) {
		return client.request(HttpRequest.post(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(PUSH)
						.appendPathPart(pubKey.asString())
						.build())
				.withBody(ByteBuf.wrapForReading(signedMetadata.toBytes())))
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
