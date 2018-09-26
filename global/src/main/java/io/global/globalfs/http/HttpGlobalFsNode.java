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

import io.datakernel.async.Stage;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.globalfs.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
		throw new UnsupportedOperationException("HttpGlobalFsNode#download is not implemented yet");
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath file, long offset) {
		throw new UnsupportedOperationException("HttpGlobalFsNode#upload is not implemented yet");
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
