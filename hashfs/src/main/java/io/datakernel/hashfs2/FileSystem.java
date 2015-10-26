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
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.Set;

public interface FileSystem {
	void saveToTemporary(String filePath, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	void commitTemporary(String filePath, boolean successful, CompletionCallback file);

	void get(String filePath, StreamConsumer<ByteBuf> consumer);

	void deleteFile(String filePath, CompletionCallback callback);

	void listFiles(ResultCallback<Set<String>> files);
}
