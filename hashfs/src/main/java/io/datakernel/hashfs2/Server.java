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

import io.datakernel.eventloop.NioEventloop;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class Server {
	private final NioEventloop eventloop;

	private final Logic logic;
	private final FileSystem fileSystem;
	private final Client client;

	private Map<ServerInfo, Set<Operation>> pendingOperations = new HashMap<>();
	private Map<FileInfo, Set<ServerInfo>> files = new HashMap<>();
	private Set<ServerInfo> alive = new HashSet<>();

	public Server(NioEventloop eventloop, Logic logic, FileSystem fileSystem, Client client) {
		this.eventloop = eventloop;
		this.logic = logic;
		this.fileSystem = fileSystem;
		this.client = client;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}
