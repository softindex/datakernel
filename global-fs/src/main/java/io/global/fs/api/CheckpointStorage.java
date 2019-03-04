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
import io.datakernel.exception.StacklessException;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface CheckpointStorage {
	StacklessException OVERRIDING = new StacklessException(CheckpointStorage.class, "Trying to override existing checkpoint with different one");

	Promise<Void> store(String filename, SignedData<GlobalFsCheckpoint> checkpoint);

	Promise<@Nullable SignedData<GlobalFsCheckpoint>> load(String filename, long position);

	Promise<List<String>> listMetaCheckpoints(String glob);

	Promise<@Nullable SignedData<GlobalFsCheckpoint>> loadMetaCheckpoint(String filename);

	Promise<long[]> loadIndex(String filename);

	Promise<Void> drop(String filename, long revision);
}
