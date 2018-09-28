/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.http;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialZeroBuffer;
import io.global.globalfs.api.*;
import io.global.globalfs.transformers.FrameDecoder;
import io.global.globalfs.transformers.FrameEncoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.global.globalfs.api.GlobalFsName.serializePubKey;
import static io.global.globalfs.http.GlobalFsNodeServlet.DOWNLOAD;
import static io.global.globalfs.http.GlobalFsNodeServlet.UPLOAD;

public final class HttpGlobalFsNode implements GlobalFsNode {
	private final AsyncHttpClient client;
	private final String host;

	// region creators
	public HttpGlobalFsNode(AsyncHttpClient client, String host) {
		this.client = client;
		this.host = host;
	}
	// endregion

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsPath file, long offset, long limit) {
		return client.requestBodyStream(
				HttpRequest.get(host + DOWNLOAD +
						"?key=" + serializePubKey(file.getPubKey()) +
						"&fs=" + file.getFsName() +
						"&path=" + file.getPath() +
						"&offset=" + offset +
						"&limit=" + limit))
				.thenCompose(response -> {
					if (response.getCode() != 200) {
						return Stage.ofException(new GlobalFsException("response: " + response));
					}
					return Stage.of(response);
				})
				.thenApply(response -> response.getBodyStream().apply(new FrameDecoder()));
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath file, long offset) {
		SerialZeroBuffer<DataFrame> buffer = new SerialZeroBuffer<>();
		MaterializedStage<Void> request = client.request(
				HttpRequest.post(host + UPLOAD +
						"?key=" + serializePubKey(file.getPubKey()) +
						"&fs=" + file.getFsName() +
						"&path=" + file.getPath() +
						"&offset=" + offset)
						.withBodyStream(buffer.getSupplier().apply(new FrameEncoder())))
				.thenCompose(response -> {
					if (response.getCode() != 200) {
						return Stage.ofException(new GlobalFsException("response: " + response));
					}
					return Stage.complete();
				})
				.materialize();
		return Stage.of(buffer.getConsumer().withAcknowledgement(ack -> ack.both(request)));
	}

	@Override
	public Stage<List<GlobalFsMetadata>> list(GlobalFsName name, String glob) {
		throw new UnsupportedOperationException("HttpGlobalFsNode#list is not implemented yet");
	}

	@Override
	public Stage<Void> delete(GlobalFsName name, String glob) {
		throw new UnsupportedOperationException("HttpGlobalFsNode#delete is not implemented yet");
	}

	@Override
	public Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes) {
		throw new UnsupportedOperationException("HttpGlobalFsNode#copy is not implemented yet");
	}

	@Override
	public Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes) {
		throw new UnsupportedOperationException("HttpGlobalFsNode#move is not implemented yet");
	}
}
