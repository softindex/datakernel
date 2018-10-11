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

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.fs.api.*;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import java.net.InetSocketAddress;
import java.util.List;

import static io.global.fs.http.GlobalFsNodeServlet.*;

public final class HttpGlobalFsNode implements GlobalFsNode {
	private final RawServerId id;
	private final InetSocketAddress address;
	private final AsyncHttpClient client;

	// region creators
	public HttpGlobalFsNode(RawServerId id, AsyncHttpClient client) {
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
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsPath path, long offset, long limit) {
		return client.request(
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(DOWNLOAD)
								.appendPathPart(path.getPubKey().asString())
								.appendPathPart(path.getFs())
								.appendPath(path.getPath())
								.appendQuery("offset", offset)
								.appendQuery("limit", limit)
								.build()))
				.thenApply(response -> response.getBodyStream().apply(new FrameDecoder()));
	}

	@Override
	public SerialConsumer<DataFrame> uploader(GlobalFsPath path, long offset) {
		SerialZeroBuffer<DataFrame> buffer = new SerialZeroBuffer<>();
		MaterializedStage<HttpResponse> request = client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(UPLOAD)
						.appendPathPart(path.getPubKey().asString())
						.appendPathPart(path.getFs())
						.appendPath(path.getPath())
						.appendQuery("offset", offset)
						.build())
				.withBodyStream(buffer.getSupplier().apply(new FrameEncoder())))
				.materialize();
		return buffer.getConsumer().withAcknowledgement(ack -> ack.both(request));
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath path, long offset) {
		return Stage.of(uploader(path, offset));
	}

	@Override
	public Stage<List<SignedData<GlobalFsMetadata>>> list(GlobalFsSpace space, String glob) {
		PubKey pubKey = space.getPubKey();
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(LIST)
						.appendPathPart(pubKey.asString())
						.appendPathPart(space.getFs())
						.appendQuery("glob", glob)
						.build()))
				.thenCompose(response -> {
					throw new UnsupportedOperationException("not implemented."); // TODO anton: implement
					// try {
					// 	return Stage.of(GsonAdapters.fromJson(KEYLESS_META_LIST, response.getBody().asString(UTF_8))
					// 			.stream()
					// 			.map(kls -> kls.into(pubKey))
					// 			.collect(toList()));
					// } catch (ParseException e) {
					// 	return Stage.ofException(e);
					// }
				});
	}

	@Override
	public Stage<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata) {
		throw new UnsupportedOperationException("HttpGlobalFsNode#pushMetadata is not implemented yet");
	}

	// @Override
	// public Stage<Set<String>> copy(GlobalFsSpace name, Map<String, String> changes) {
	// 	return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
	// 			UrlBuilder.http()
	// 					.withAuthority(address)
	// 					.appendPathPart(COPY)
	// 					.appendPathPart(name.getPubKey().asString())
	// 					.appendPathPart(name.getFs())
	// 					.build())
	// 			.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
	// 			.thenCompose(response -> {
	// 				try {
	// 					return Stage.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
	// 				} catch (ParseException e) {
	// 					return Stage.ofException(e);
	// 				}
	// 			});
	// }
	//
	// @Override
	// public Stage<Set<String>> move(GlobalFsSpace name, Map<String, String> changes) {
	// 	return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
	// 			UrlBuilder.http()
	// 					.withAuthority(address)
	// 					.appendPathPart(MOVE)
	// 					.appendPathPart(name.getPubKey().asString())
	// 					.appendPathPart(name.getFs())
	// 					.build())
	// 			.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
	// 			.thenCompose(response -> {
	// 				try {
	// 					return Stage.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
	// 				} catch (ParseException e) {
	// 					return Stage.ofException(e);
	// 				}
	// 			});
	// }
}
