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

import java.util.Map;
import java.util.Set;

public interface Commands {

	/**
	 * Creates replica of a file on chosen server.
	 *
	 * @param file   - name of a file in a local file system.
	 * @param server - server the replica to be created at.
	 */
	void replicate(FileInfo file, ServerInfo server);

	/**
	 * Delete file that is not supposed to be at this server storage;
	 *
	 * @param fileName - name of a file to be deleted.
	 */
	void delete(FileInfo fileName);

	/**
	 * Shows files that are being kept on this server.
	 *
	 * @return files mapped to their replicas.
	 */
	Map<FileInfo, Set<ServerInfo>> showFiles();

	/**
	 * Updates info about servers that are defined to be alive.
	 *
	 * @return alive servers.
	 */
	Set<ServerInfo> showServers();

	/**
	 * Shows current pending operations.
	 *
	 * @return info about operations being executed at the moment.
	 */
	Map<ServerInfo, Set<Operation>> showPendingOperations();

	Config showConfigs();
}
