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

package io.datakernel.hashfs.protocol;

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.hashfs.FileMetadata;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface HashFsClientProtocol {

	void download(final ServerInfo server, final String filename, final ResultCallback<StreamProducer<ByteBuf>> callback);

	void upload(final ServerInfo server, String destinationName, ResultCallback<StreamConsumer<ByteBuf>> callback);

	void delete(final ServerInfo server, final String filename, final ResultCallback<Boolean> callback);

	void list(final ServerInfo server, ResultCallback<Set<String>> callback);

	void listAbsent(final ServerInfo serverInfo, Map<String, FileMetadata.FileBaseInfo> interestFiles,
	                ResultCallback<Set<String>> callback);

	void replicas(final ServerInfo server, ResultCallback<Map<String, Set<Integer>>> callback);

	void getAliveServers(final ServerInfo server, ResultCallback<List<ServerInfo>> callback);

}
