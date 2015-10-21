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

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLogic {

	FileInfo file1 = new FileInfo("good_file.txt", 123, 123);
	FileInfo file2 = new FileInfo("will_replicate.txt", 456, 456);
	FileInfo file3 = new FileInfo("cant_delete.txt", 789, 789);
	FileInfo file4 = new FileInfo("will_delete.txt", 983, 983);

	InetSocketAddress ignored = new InetSocketAddress(1234);
	ServerInfo local = new ServerInfo(0, ignored, 0.1);
	ServerInfo fs1 = new ServerInfo(1, ignored, 0.1);
	ServerInfo fs2 = new ServerInfo(2, ignored, 0.1);
	ServerInfo fs3 = new ServerInfo(3, ignored, 0.1);
	ServerInfo fs4 = new ServerInfo(4, ignored, 0.1);

	class CommandsMock implements Commands {
		private Map<FileInfo, Set<ServerInfo>> fileMap;
		private Set<ServerInfo> serversMap;
		private Map<ServerInfo, Set<Operation>> operationsMap;
		private Config configs;

		Map<FileInfo, Set<ServerInfo>> replicas = new HashMap<>();
		Set<FileInfo> deletedFiles = new HashSet<>();

		public CommandsMock(Map<FileInfo, Set<ServerInfo>> fileMap, Set<ServerInfo> serversMap,
		                    Map<ServerInfo, Set<Operation>> operationsMap, Config configs) {
			this.fileMap = fileMap;
			this.serversMap = serversMap;
			this.operationsMap = operationsMap;
			this.configs = configs;
		}

		@Override
		public void replicate(FileInfo file, ServerInfo server) {
			Set<ServerInfo> replicaHandlers = replicas.get(file);
			if (replicaHandlers == null) {
				replicaHandlers = new HashSet<>();
				replicas.put(file, replicaHandlers);
			}
			replicaHandlers.add(server);
		}

		@Override
		public void delete(FileInfo file) {
			deletedFiles.add(file);
		}

		@Override
		public Map<FileInfo, Set<ServerInfo>> showFiles() {
			return fileMap;
		}

		@Override
		public Set<ServerInfo> showServers() {
			return serversMap;
		}

		@Override
		public Map<ServerInfo, Set<Operation>> showPendingOperations() {
			return operationsMap;
		}

		@Override
		public Config showConfigs() {
			return configs;
		}
	}

	class HashingMock implements Hashing {
		@Override
		public List<ServerInfo> sortServers(Collection<ServerInfo> servers, String fileName) {
			List<ServerInfo> list = new ArrayList<>();
			if (fileName.equals("good_file.txt")) {
				list.add(local);
				list.add(fs1);
				list.add(fs2);
				list.add(fs3);
				list.add(fs4);
				return list;
			}
			if (fileName.equals("will_replicate.txt")) {
				list.add(local);
				list.add(fs2);
				list.add(fs3);
				list.add(fs4);
				list.add(fs1);
				return list;
			}
			if (fileName.equals("cant_delete.txt")) {
				list.add(fs1);
				list.add(fs2);
				list.add(fs3);
				list.add(fs4);
				return list;
			}
			if (fileName.equals("will_delete.txt")) {
				list.add(fs1);
				list.add(fs2);
				list.add(fs3);
				list.add(fs4);
				return list;
			}
			return null;
		}
	}

	@Test
	public void testLogicBaseImpl() {
		Set<ServerInfo> replica1 = new HashSet<>();
		replica1.add(local);
		replica1.add(fs1);
		replica1.add(fs2);

		Set<ServerInfo> replica2 = new HashSet<>();
		replica2.add(local);

		Set<ServerInfo> replica3 = new HashSet<>();
		replica3.add(local);

		Set<ServerInfo> replica4 = new HashSet<>();
		replica4.add(local);
		replica4.add(fs1);
		replica4.add(fs2);
		replica4.add(fs3);

		Map<FileInfo, Set<ServerInfo>> filesMap = new HashMap<>();
		filesMap.put(file1, replica1);
		filesMap.put(file2, replica2);
		filesMap.put(file3, replica3);
		filesMap.put(file4, replica4);

		Set<ServerInfo> alive = new HashSet<>();
		alive.addAll(Arrays.asList(local, fs1, fs2, fs3));

		Map<ServerInfo, Set<Operation>> operations = new HashMap<>();
		operations.put(fs4, new HashSet<Operation>());

		Config config = new Config(1, 3, 1000l, local);

		CommandsMock commands = new CommandsMock(filesMap, alive, operations, config);
		Hashing hashing = new HashingMock();

		Logic logic = new LogicBaseImpl(commands, hashing);
		logic.update();

		assertEquals(1, commands.deletedFiles.size());
		assertTrue(commands.deletedFiles.contains(file4));

		assertEquals(2, commands.replicas.size());
		Set<ServerInfo> updatedReplicasFor2 = commands.replicas.get(file2);
		assertEquals(2, updatedReplicasFor2.size());
		assertTrue(updatedReplicasFor2.contains(fs2) && updatedReplicasFor2.contains(fs3));

		Set<ServerInfo> updatedReplicasFor3 = commands.replicas.get(file3);
		assertEquals(3, updatedReplicasFor3.size());
		assertTrue(updatedReplicasFor3.contains(fs1) && updatedReplicasFor3.contains(fs2) && updatedReplicasFor3.contains(fs3));
	}
}
