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

import io.datakernel.async.ResultCallback;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.*;

import static org.junit.Assert.*;

public class TestLogic {

	private final ServerInfo local = new ServerInfo(0, new InetSocketAddress("http://127.0.0.1", 1234), 0.1);
	private final ServerInfo server1 = new ServerInfo(1, new InetSocketAddress("http://127.0.0.1", 2345), 0.1);
	private final ServerInfo server2 = new ServerInfo(2, new InetSocketAddress("http://127.0.0.1", 3456), 0.1);
	private final ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("http://127.0.0.1", 4567), 0.1);

	private final String a = "a.txt";
	private final String b = "b.txt";
	private final String c = "c.txt";
	private final String d = "d.txt";
	private final String e = "e.txt";
	private final String f = "f.txt";
	private final String g = "g.txt";
	private final String newFile = "new file";

	@Test
	public void testUpdate() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = new LogicImpl(hMock, local, cMock);
		logic.init(new HashSet<>(Arrays.asList(local, server1, server2, server3)));
		logic.update();

		Set<String> real1 = cMock.servers.get(server1);
		Set<String> real2 = cMock.servers.get(server2);
		Set<String> real3 = cMock.servers.get(server3);

		Set<String> expected1 = new HashSet<>(Arrays.asList(a, c, d, f, g));
		Set<String> expected2 = new HashSet<>(Arrays.asList(a, b, d, e, f));
		Set<String> expected3 = new HashSet<>(Arrays.asList(a, b, c, e, g));

		assertEquals(expected1, real1);
		assertEquals(expected2, real2);
		assertEquals(expected3, real3);
	}

	@Test
	public void testUpload() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = new LogicImpl(hMock, local, cMock);
		logic.init(new HashSet<>(Arrays.asList(local, server1, server2, server3)));
		logic.update();

		assertFalse(logic.canUpload(b));

		assertTrue(logic.canUpload(newFile));
		logic.onUploadComplete(newFile);
		assertTrue(cMock.scheduledDeletions.contains(newFile) && cMock.scheduledDeletions.size() == 1);
		assertTrue(logic.canApprove(newFile));
		logic.onApprove(newFile, true);
		logic.update();

		Set<String> real2 = cMock.servers.get(server2);
		Set<String> real3 = cMock.servers.get(server3);

		assertTrue(real2.contains(newFile));
		assertTrue(real3.contains(newFile));
	}

	@Test
	public void testDownload() {
		fail("Not yet implemented");
	}

	@Test
	public void testOffer() {
		fail("Not yet implemented");
	}

	@Test
	public void testAlive() {
		fail("Not yet implemented");
	}

	class HashingMock implements Hashing {
		@Override
		public List<ServerInfo> sortServers(Collection<ServerInfo> servers, String fileName) {
			switch (fileName) {
				case a:
					return sort(1, 2, 3, 0);
				case b:
					return sort(2, 3, 0, 1);
				case c:
					return sort(3, 0, 1, 2);
				case d:
					return sort(0, 1, 2, 3);
				case e:
					return sort(0, 2, 3, 1);
				case f:
					return sort(2, 0, 1, 3);
				case g:
					return sort(1, 3, 0, 2);
				case newFile:
					return sort(0, 2, 3, 1);
				default:
					return new ArrayList<>();
			}
		}

		private List<ServerInfo> sort(int... order) {
			List<ServerInfo> result = new ArrayList<>();
			for (int num : order) {
				switch (num) {
					case 0:
						result.add(local);
						break;
					case 1:
						result.add(server1);
						break;
					case 2:
						result.add(server2);
						break;
					case 3:
						result.add(server3);
						break;
				}
			}
			return result;
		}
	}

	class CommandsMock implements Commands {
		Map<ServerInfo, Set<String>> servers = new HashMap<>();
		Set<String> filesDeletedFromThisServer = new HashSet<>();
		Set<String> scheduledDeletions = new HashSet<>();

		{
			servers.put(local, new HashSet<String>());
			servers.put(server1, new HashSet<String>());
			servers.put(server2, new HashSet<String>());
			servers.put(server3, new HashSet<String>());
		}

		@Override
		public void replicate(String filePath, ServerInfo server) {
			servers.get(server).add(filePath);
		}

		@Override
		public void delete(String filePath) {
			filesDeletedFromThisServer.add(filePath);
		}

		@Override
		public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
			Set<String> neededFiles = new HashSet<>();
			Set<String> currentFiles = servers.get(server);
			for (String s : forUpload) {
				if (!currentFiles.contains(s)) {
					neededFiles.add(s);
				}
				result.onResult(neededFiles);
			}
		}

		@Override
		public void updateServerMap(Set<ServerInfo> bootstrap, ResultCallback<Set<ServerInfo>> result) {
			result.onResult(servers.keySet());
		}

		@Override
		public void scheduleTemporaryFileDeletion(String filePath) {
			scheduledDeletions.add(filePath);
		}

		@Override
		public void scan(ResultCallback<Set<String>> callback) {
			callback.onResult(new HashSet<>(Arrays.asList(a, b, c, d, e, f, g)));
		}

		@Override
		public void updateSystem() {
			// ignored
		}
	}
}

