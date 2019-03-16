/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.launchers.crdt;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.crdt.local.CrdtStorageFileSystem;
import io.datakernel.crdt.local.CrdtStorageTreeMap;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

@Singleton
public final class BackupService<K extends Comparable<K>, S> implements EventloopService {
	private final Eventloop eventloop;
	private final CrdtStorageTreeMap<K, S> inMemory;
	private final CrdtStorageFileSystem<K, S> localFiles;

	private long lastTimestamp = 0;

	@Nullable
	private Promise<Void> backupPromise = null;

	@Inject
	public BackupService(CrdtStorageTreeMap<K, S> inMemory, CrdtStorageFileSystem<K, S> localFiles) {
		this.inMemory = inMemory;
		this.localFiles = localFiles;
		this.eventloop = localFiles.getEventloop();
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public Promise<Void> restore() {
		return localFiles.download()
				.then(supplierWithResult ->
						supplierWithResult.streamTo(StreamConsumer.ofPromise(inMemory.upload())));
	}

	public Promise<Void> backup() {
		if (backupPromise != null) {
			return backupPromise;
		}
		Set<K> removedKeys = inMemory.getRemovedKeys();
		long lastTimestamp = this.lastTimestamp;
		this.lastTimestamp = eventloop.currentTimeMillis();
		return backupPromise = inMemory.download(lastTimestamp)
				.then(supplierWithResult -> supplierWithResult
						.streamTo(StreamConsumer.ofPromise(localFiles.upload()))
						.then($ -> StreamSupplier.ofIterable(removedKeys)
								.streamTo(StreamConsumer.ofPromise(localFiles.remove())))
						.acceptEx(($, e) -> {
							inMemory.clearRemovedKeys();
							backupPromise = null;
						}));
	}

	public boolean backupInProgress() {
		return backupPromise != null;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return restore().then($ -> localFiles.consolidate()).materialize();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return backup().materialize();
	}
}
