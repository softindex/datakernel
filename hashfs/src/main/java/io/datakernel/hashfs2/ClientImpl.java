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

package io.datakernel.hashfs2;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs2.net.Protocol;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.List;
import java.util.Set;

// TODO

/**
 * HashFSClient logic impl:
 * 1) multiple tries
 * 2) server choosing
 * 3)
 */
public class ClientImpl implements Client {
	private Protocol protocol;
	private NioEventloop eventloop;
	private Hashing hashing;

	@Override
	public void upload(String filePath, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		//protocol.upload(server, filePath, producer, callback);
	}

	@Override
	public void download(String filePath, StreamConsumer<ByteBuf> consumer) {

	}

	@Override
	public void list(ResultCallback<Set<String>> files) {

	}

	@Override
	public void delete(CompletionCallback callback) {

	}

	@Override
	public void alive(ResultCallback<Set<ServerInfo>> servers) {

	}

	@Override
	public void offer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {

	}
}
