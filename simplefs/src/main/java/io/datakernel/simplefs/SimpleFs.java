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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.net.InetSocketAddress;
import java.util.List;

public interface SimpleFs {

	void write(InetSocketAddress address, final String destinationFileName, final ResultCallback<StreamConsumer<ByteBuf>> callback);

	void read(InetSocketAddress address, final String path, final ResultCallback<StreamProducer<ByteBuf>> callback);

	void fileList(InetSocketAddress address, final ResultCallback<List<String>> callback);

	void deleteFile(InetSocketAddress address, final String fileName, final ResultCallback<Boolean> callback);
}
