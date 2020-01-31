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

package io.global.fs.transformers;

import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.GlobalFsCheckpoint;

/**
 * Something like a splitter, which outputs the bytebuf data, but
 * also stores the checkpoints in given {@link CheckpointStorage}.
 * <p>
 * It's counterpart is the {@link FramesFromStorage}.
 */
public final class FramesIntoStorage extends FramesToByteBufs {
	private final String filename;
	private final CheckpointStorage checkpointStorage;

	private FramesIntoStorage(String filename, PubKey pubKey, CheckpointStorage checkpointStorage) {
		super(pubKey, filename);
		this.filename = filename;
		this.checkpointStorage = checkpointStorage;
	}

	public static FramesIntoStorage create(String filename, PubKey pubKey, CheckpointStorage checkpointStorage) {
		return new FramesIntoStorage(filename, pubKey, checkpointStorage);
	}

	@Override
	protected Promise<Void> receiveCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
		return checkpointStorage.store(filename, checkpoint);
	}
}
