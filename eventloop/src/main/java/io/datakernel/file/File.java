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

package io.datakernel.file;

import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;

public interface File {
	void write(final ByteBuf buf, long position, final ResultCallback<Integer> callback);

	AsyncCancellable writeFully(ByteBuf byteBuf, long position, CompletionCallback callback);

	void read(final ByteBuf buf, long position, final ResultCallback<Integer> callback);

	AsyncCancellable readFully(ByteBuf buf, long position, CompletionCallback callback);

	void readFully(final ResultCallback<ByteBuf> callback);

	void close(CompletionCallback callback);

	void truncate(final long size, CompletionCallback callback);

	void force(final boolean metaData, CompletionCallback callback);
}
