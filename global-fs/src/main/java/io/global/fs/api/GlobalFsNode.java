/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.global.fs.api;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.global.common.PubKey;
import io.global.common.SignedData;

import java.util.List;

import static io.datakernel.file.FileUtils.escapeGlob;
import static io.global.fs.api.CheckpointStorage.NO_CHECKPOINT;

/**
 * This component handles one of the GlobalFS nodes.
 */
public interface GlobalFsNode {
	StacklessException UNEXPECTED_TOMBSTONE = new StacklessException(GlobalFsNode.class, "Tombstones are not allowed to be streamed");
	StacklessException UPLOADING_TO_TOMBSTONE = new StacklessException(GlobalFsNode.class, "Trying to upload to a file which was deleted");
	StacklessException FILE_ALREADY_EXISTS = new StacklessException(GlobalFsNode.class, "File already exists");

	Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset);

	Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long limit);

	Promise<List<SignedData<GlobalFsCheckpoint>>> list(PubKey space, String glob);

	default Promise<SignedData<GlobalFsCheckpoint>> getMetadata(PubKey space, String filename) {
		return list(space, escapeGlob(filename))
				.thenCompose(res ->
						res.size() == 1 ?
								Promise.of(res.get(0)) :
								Promise.ofException(NO_CHECKPOINT));
	}

	Promise<Void> delete(PubKey space, SignedData<GlobalFsCheckpoint> tombstone);
}
