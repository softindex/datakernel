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
import io.datakernel.exception.UncheckedException;
import io.datakernel.file.FileUtils;
import io.datakernel.http.HttpMessage;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.RepoID;
import io.global.fs.api.GlobalFsGateway;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalPath;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.serial.ByteBufsParser.ofVarIntSizePrefixedBytes;
import static io.global.fs.http.GlobalFsNodeServlet.*;
import static java.util.stream.Collectors.toList;

public final class HttpGlobalFsGateway implements GlobalFsGateway {
	private final InetSocketAddress address;
	private final IAsyncHttpClient client;

	// region creators
	public HttpGlobalFsGateway(RawServerId id, IAsyncHttpClient client) {
		this.client = client;
		this.address = id.getInetSocketAddress();
	}
	// endregion

	@Override
	public Promise<SerialSupplier<ByteBuf>> download(GlobalPath path, long offset, long limit) {
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
				.thenCompose(response -> response.ensureStatusCode(200))
				.thenApply(HttpMessage::getBodyStream);
	}

	@Override
	public SerialConsumer<ByteBuf> uploader(GlobalPath path, long offset) {
		SerialZeroBuffer<ByteBuf> buffer = new SerialZeroBuffer<>();
		MaterializedPromise<HttpResponse> request = client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(UPLOAD)
						.appendPathPart(path.getOwner().asString())
						.appendPathPart(path.getFs())
						.appendPath(path.getPath())
						.appendQuery("offset", "" + offset)
						.build())
				.withBodyStream(buffer.getSupplier()))
				.thenCompose(response -> response.ensureStatusCode(200))
				.materialize();
		return buffer.getConsumer().withAcknowledgement(ack -> ack.both(request));
	}

	@Override
	public Promise<SerialConsumer<ByteBuf>> upload(GlobalPath path, long offset) {
		return Promise.of(uploader(path, offset));
	}

	@Override
	public Promise<List<GlobalFsMetadata>> list(RepoID space, String glob) {
		PubKey pubKey = space.getOwner();
		return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(LIST)
						.appendPathPart(pubKey.asString())
						.appendPathPart(space.getName())
						.appendQuery("glob", glob)
						.build()))
				.thenCompose(response -> response.ensureStatusCode(200))
				.thenCompose(response ->
						ByteBufsSupplier.of(response.getBodyStream())
								.parseStream(ofVarIntSizePrefixedBytes())
								.transform(buf -> {
									try {
										return GlobalFsMetadata.fromBytes(buf.asArray());
									} catch (ParseException e) {
										throw new UncheckedException(e);
									}
								}).toCollector(toList()));
	}

	@Override
	public Promise<Void> delete(GlobalPath path) {
		return delete(path.toRepoID(), FileUtils.escapeGlob(path.getPath()));
	}

	@Override
	public Promise<Void> delete(RepoID space, String glob) {
		PubKey pubKey = space.getOwner();
		return client.request(HttpRequest.get(
				UrlBuilder.http()
						.withAuthority(address)
						.appendPathPart(DEL)
						.appendPathPart(pubKey.asString())
						.appendPathPart(space.getName())
						.appendQuery("glob", glob)
						.build()))
				.thenCompose(response -> response.ensureStatusCode(200))
				.toVoid();
	}
}
