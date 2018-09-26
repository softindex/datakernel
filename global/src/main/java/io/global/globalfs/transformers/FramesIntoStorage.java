/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.transformers;

import io.datakernel.async.Stage;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.globalfs.api.CheckpointStorage;
import io.global.globalfs.api.GlobalFsCheckpoint;

/**
 * Something like a splitter, which outputs the byte buf data, but
 * also stores the checkpoints in given {@link CheckpointStorage}.
 * Unlike {@link FrameVerifier}, it does not cut the output stream,
 * although it does verify received checkpoints and data.
 * <p>
 * It's counterpart is the {@link FramesFromStorage}.
 */
public final class FramesIntoStorage extends FramesToByteBufs {
	private final String fileName;
	private final CheckpointStorage checkpointStorage;

	// region creators
	public FramesIntoStorage(PubKey pubKey, String fileName, CheckpointStorage checkpointStorage) {
		super(pubKey);
		this.fileName = fileName;
		this.checkpointStorage = checkpointStorage;
	}
	// endregion

	@Override
	protected Stage<Void> receiveCheckpoint(SignedData<GlobalFsCheckpoint> checkpoint) {
		return checkpointStorage.saveCheckpoint(fileName, checkpoint);
	}
}
