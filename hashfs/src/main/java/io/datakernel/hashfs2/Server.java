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
import io.datakernel.eventloop.NioService;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;

import java.util.Set;

public interface Server extends NioService {
	void upload(final String filePath, StreamProducer<ByteBuf> producer, final CompletionCallback callback);

	void commit(final String filePath, final boolean success, final CompletionCallback callback);

	void download(final String filePath, StreamForwarder<ByteBuf> consumer, ResultCallback<CompletionCallback> callback);

	void delete(final String filePath, final CompletionCallback callback);

	void listFiles(ResultCallback<Set<String>> files);

	void showAlive(ResultCallback<Set<ServerInfo>> callback);

	void checkOffer(Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback);
}
