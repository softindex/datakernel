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

package io.global.globalfs.local;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.SerialSplitter;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.*;

import java.time.Duration;
import java.util.*;

public final class LocalGlobalFsNode implements GlobalFsNode {
	private final Map<PubKey, LocalGlobalFsNamespace> publicKeyGroups = new HashMap<>();
	private final Set<GlobalFsPath> searchingForDownload = new HashSet<>();
	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final RawNodeFactory clientFactory;
	private final FileSystemFactory fileSystemFactory;
	private final Settings settings;

	// region creators
	public LocalGlobalFsNode(RawServerId id, DiscoveryService discoveryService, RawNodeFactory clientFactory, FileSystemFactory fileSystemFactory, Settings settings) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.clientFactory = clientFactory;
		this.fileSystemFactory = fileSystemFactory;
		this.settings = settings;
	}
	// endregion

	private LocalGlobalFsNamespace getNamespace(PubKey key) {
		return publicKeyGroups.computeIfAbsent(key, $ -> new LocalGlobalFsNamespace(this, key));
	}

	public RemoteFsFileSystem getFs(GlobalFsName name) {
		return getNamespace(name.getPubKey()).getFs(name.getFsName());
	}

	public RawServerId getId() {
		return id;
	}

	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	public RawNodeFactory getClientFactory() {
		return clientFactory;
	}

	public FileSystemFactory getFileSystemFactory() {
		return fileSystemFactory;
	}

	public Settings getSettings() {
		return settings;
	}

	private Stage<SerialSupplier<DataFrame>> downloadFrom(GlobalFsNode node, RemoteFsFileSystem local, GlobalFsPath address, long offset, long limit) {
		return node.getMetadata(address)
				.thenCompose(res -> {
					if (res == null) {
						return Stage.ofException(new StacklessException("no such file"));
					}
					return node.download(address, offset, limit);
				})
				.thenApply(supplier -> {
					SerialSplitter<DataFrame> splitter = SerialSplitter.<DataFrame>create()
							.withInput(supplier.transform(DataFrame::slice));

					SerialSupplier<DataFrame> output = splitter
							.addOutput()
							.getOutputSupplier()
							.transform(df -> {
								DataFrame slice = df.slice();
								df.recycle();
								return slice;
							});
					MaterializedStage<Void> cacheProcess = splitter.addOutput()
							.getOutputSupplier()
							.transform(df -> {
								DataFrame slice = df.slice();
								df.recycle();
								return slice;
							})
							.streamTo(SerialConsumer.ofStage(local.upload(address.getPath(), offset)))
							.materialize();
					splitter.start();
					return output.withEndOfStream(eos -> eos.both(cacheProcess));
				});
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsPath address, long offset, long limit) {
		if (!searchingForDownload.add(address)) {
			return Stage.ofException(RECURSIVE_ERROR);
		}
		LocalGlobalFsNamespace ns = getNamespace(address.getPubKey());
		RemoteFsFileSystem fs = ns.getFs(address.getGlobalFsName().getFsName());
		return fs
				.download(address.getPath(), offset, limit)
				.thenComposeEx((result, e) -> {
					if (e == null) {
						searchingForDownload.remove(address);
						return Stage.of(result);
					}
					return ns.findNodes()
							.thenCompose(nodes ->
									Stages.firstSuccessful(nodes
											.stream()
											.map(node -> downloadFrom(node, fs, address, offset, limit))))
							.thenRunEx(() -> searchingForDownload.remove(address));
				});
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath file, long offset) {
		return getFs(file.getGlobalFsName()).upload(file.getPath(), offset);
	}

	@Override
	public Stage<List<GlobalFsMetadata>> list(GlobalFsName name, String glob) {
		return getFs(name).list(glob);
	}

	@Override
	public Stage<Void> delete(GlobalFsName name, String glob) {
		return getFs(name).delete(glob);
	}

	@Override
	public Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes) {
		return getFs(name).copy(changes);
	}

	@Override
	public Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes) {
		return getFs(name).move(changes);
	}

	public interface Settings {
		Duration getLatencyMargin();
	}

	@FunctionalInterface
	public interface FileSystemFactory {
		RemoteFsFileSystem create(LocalGlobalFsNamespace group, String fsName);
	}
}
