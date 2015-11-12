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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.RfsConfig;
import io.datakernel.remotefs.ServerInfo;
import io.datakernel.remotefs.protocol.ClientProtocol;
import io.datakernel.remotefs.protocol.gson.GsonClientProtocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public final class SimpleFsClient implements FsClient {
	private final ServerInfo serverInfo;
	private final ClientProtocol protocol;

	private SimpleFsClient(ServerInfo serverInfo, ClientProtocol protocol) {
		this.serverInfo = serverInfo;
		this.protocol = protocol;
	}

	public static SimpleFsClient createInstance(ServerInfo serverInfo, ClientProtocol protocol) {
		return new SimpleFsClient(serverInfo, protocol);
	}

	public static SimpleFsClient createInstance(NioEventloop eventloop, InetSocketAddress address) {
		ClientProtocol protocol = GsonClientProtocol.createInstance(eventloop, RfsConfig.getDefaultConfig());
		ServerInfo info = new ServerInfo(-1, address, 1.0);
		return new SimpleFsClient(info, protocol);
	}

	@Override
	public void upload(final String fileName, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		protocol.upload(serverInfo, fileName, producer, callback);
	}

	@Override
	public void download(final String fileName, final StreamConsumer<ByteBuf> consumer) {
		protocol.download(serverInfo, fileName, consumer, ignoreCompletionCallback());
	}

	@Override
	public void list(final ResultCallback<List<String>> callback) {
		protocol.list(serverInfo, new ForwardingResultCallback<Set<String>>(callback) {
			@Override
			public void onResult(Set<String> result) {
				callback.onResult(new ArrayList<>(result));
			}
		});
	}

	@Override
	public void delete(final String fileName, final CompletionCallback callback) {
		protocol.delete(serverInfo, fileName, callback);
	}
}