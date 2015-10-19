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

package io.datakernel.hashfs2;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogicBaseImpl implements Logic {
	private Commands commands;

	public LogicBaseImpl(Commands commands) {
		this.commands = commands;
	}

	@Override
	public void update() {
		Map<FileInfo, Set<ServerInfo>> filesMap = commands.showFiles();
		Set<ServerInfo> aliveServers = commands.showServers();
		Map<ServerInfo, Set<Operation>> pendingOperations = commands.showPendingOperations();

		Config config = commands.showConfigs();
		Set<FileInfo> files = filesMap.keySet();

		for (FileInfo file : files) {
			List<ServerInfo> rangedServers = RendezvousHashing.sortServers(aliveServers, file.getFileName());
			List<ServerInfo> candidates = rangedServers.subList(0, Math.min(rangedServers.size(), config.getReplicasQuantity()));
			for (ServerInfo server : candidates) {
				if (!filesMap.get(file).contains(server) && !pendingOperations.containsKey(server)) {
					commands.replicate(file, server);
				}
			}
			if (!candidates.contains(config.getMyInfo()) && filesMap.get(file).size() > 1) {
				commands.delete(file);
			}
		}
		
	}

}
