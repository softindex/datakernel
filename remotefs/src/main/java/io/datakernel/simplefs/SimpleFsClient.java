/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.simplefs;

import io.datakernel.FsClient;
import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;

import java.net.InetSocketAddress;
import java.util.List;

public final class SimpleFsClient extends FsClient {
	private final InetSocketAddress address;

	public SimpleFsClient(Eventloop eventloop, InetSocketAddress address) {
		super(eventloop);
		this.address = address;
	}

	@Override
	public void upload(String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		doUpload(address, fileName, producer, callback);
	}

	@Override
	public void download(String fileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback) {
		doDownload(address, fileName, startPosition, callback);
	}

	@Override
	public void delete(String fileName, CompletionCallback callback) {
		doDelete(address, fileName, callback);
	}

	@Override
	public void list(ResultCallback<List<String>> callback) {
		doList(address, callback);
	}
}