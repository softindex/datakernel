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

package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.global.common.SignedData;

import java.util.Arrays;

public interface CheckpointStorage {
	Stage<long[]> getCheckpoints(String filename);

	default Stage<Long> getLastCheckpoint(String filename) {
		return getCheckpoints(filename)
				.thenApply(positions -> Arrays.stream(positions).max().orElse(0L));
	}

	Stage<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position);

	Stage<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint);
}
