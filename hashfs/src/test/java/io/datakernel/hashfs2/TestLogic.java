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

import static io.datakernel.hashfs2.Config.defaultConfig;
import static org.junit.Assert.*;

public class TestLogic {

	private final ServerInfo local = new ServerInfo(0, new InetSocketAddress("http://127.0.0.1", 1234), 0.1);
	private final ServerInfo server1 = new ServerInfo(1, new InetSocketAddress("http://127.0.0.1", 2345), 0.1);
	private final ServerInfo server2 = new ServerInfo(2, new InetSocketAddress("http://127.0.0.1", 3456), 0.1);
	private final ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("http://127.0.0.1", 4567), 0.1);

	{
		local.updateState(ServerStatus.RUNNING, 1);
		server1.updateState(ServerStatus.RUNNING, 1);
		server2.updateState(ServerStatus.RUNNING, 1);
		server3.updateState(ServerStatus.RUNNING, 1);
	}

	private final Config config = defaultConfig.setupLogic(10 * 1000, 2, 1);

	private final Set<ServerInfo> bootstrap = new HashSet<>(Arrays.asList(local, server1, server2, server3));

	private final String a = "a.txt";
	private final String b = "b.txt";
	private final String c = "c.txt";
	private final String d = "d.txt";
	private final String e = "e.txt";
	private final String f = "f.txt";
	private final String g = "g.txt";
	private final String newFile = "new file.txt";

	@Test
	public void testUpdate() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = LogicImpl.init(bootstrap, cMock, hMock, local, config);
		logic.update();

		Set<String> real1 = cMock.servers2files.get(server1);
		Set<String> real2 = cMock.servers2files.get(server2);
		Set<String> real3 = cMock.servers2files.get(server3);

		Set<String> expected1 = new HashSet<>(Arrays.asList(d, g, a));
		Set<String> expected2 = new HashSet<>(Arrays.asList(a, b, e, f));
		Set<String> expected3 = new HashSet<>(Arrays.asList(b, c, g));

		assertEquals(expected1, real1);
		assertEquals(expected2, real2);
		assertEquals(expected3, real3);
	}

	@Test
	public void testUpload() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = LogicImpl.init(bootstrap, cMock, hMock, local, config);
		logic.update();

		assertFalse(logic.canUpload(b));

		assertTrue(logic.canUpload(newFile));
		logic.onUploadStart(newFile);
		logic.onUploadComplete(newFile);
		assertTrue(cMock.scheduledDeletions.contains(newFile) && cMock.scheduledDeletions.size() == 1);
		assertTrue(logic.canApprove(newFile));
		logic.onApprove(newFile);
		logic.update();

		Set<String> real2 = cMock.servers2files.get(server2);

		assertTrue(real2.contains(newFile));
	}

	@Test
	public void testDelete() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = LogicImpl.init(bootstrap, cMock, hMock, local, config);
		logic.update();
		// first time due to the fact that there was only minimum safe replicas quantity in the whole system
		// logic left the file and it is still accessible from this server unless this node manage to replicate it
		logic.onReplicationComplete(a, server1);
		logic.onReplicationComplete(a, server2);
		assertTrue(logic.canDelete(a));
		logic.update();
		assertFalse(logic.canDelete(a));

		logic.onReplicationComplete(e, server2);
		assertTrue(logic.canDelete(e));
		logic.onDeletionStart(e);
		logic.onDeleteComplete(e);
		assertTrue(logic.canUpload(e));
		logic.update();

		assertTrue(cMock.servers2deletedFiles.get(server2).contains(e));

		logic.onReplicationComplete(f, server2);
		logic.onDownloadStart(f);
		assertFalse(logic.canDelete(f));
		logic.onDownloadComplete(f);
		assertTrue(logic.canDelete(f));
	}

	@Test
	public void testDownload() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = LogicImpl.init(bootstrap, cMock, hMock, local, config);
		logic.update();
		logic.update();
		assertFalse(logic.canDownload(a));
	}

	@Test
	public void testOffer() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = LogicImpl.init(bootstrap, cMock, hMock, local, config);
		logic.update();

		logic.onReplicationComplete(a, server1);
		logic.onReplicationComplete(a, server2);
		logic.onReplicationComplete(d, server1);
		logic.onReplicationComplete(e, server2);
		logic.onReplicationComplete(c, server3);
		logic.onReplicationComplete(f, server2);
		logic.onReplicationComplete(g, server1);
		logic.onReplicationComplete(g, server3);
		logic.onReplicationComplete(b, server2);
		logic.onReplicationComplete(b, server3);

		logic.update();

		logic.onOfferRequest(new HashSet<>(Arrays.asList(a, b, c, d, e, f, g)), new HashSet<String>(), new ResultCallback<Set<String>>() {
			@Override
			public void onResult(Set<String> result) {
				assertEquals(new HashSet<>(Arrays.asList(a, b, g)), result);
			}

			@Override
			public void onException(Exception ignored) {
				fail("Can't sto here");
			}
		});
	}

	@Test
	public void testAlive() {
		HashingMock hMock = new HashingMock();
		CommandsMock cMock = new CommandsMock();
		Logic logic = LogicImpl.init(bootstrap, cMock, hMock, local, config);

		logic.onShowAliveRequest(new ResultCallback<Set<ServerInfo>>() {
			@Override
			public void onResult(Set<ServerInfo> result) {
				assertEquals(new HashSet<>(bootstrap), result);
			}

			@Override
			public void onException(Exception exception) {
				fail("Can't end here");
			}
		});
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
		Map<ServerInfo, Set<String>> servers2files = new HashMap<>();
		Map<ServerInfo, Set<String>> servers2deletedFiles = new HashMap<>();
		Set<String> filesDeletedFromThisServer = new HashSet<>();
		Set<String> scheduledDeletions = new HashSet<>();

		{
			servers2files.put(local, new HashSet<String>());
			servers2files.put(server1, new HashSet<String>());
			servers2files.put(server2, new HashSet<String>());
			servers2files.put(server3, new HashSet<String>());

			servers2deletedFiles.put(local, new HashSet<String>());
			servers2deletedFiles.put(server1, new HashSet<String>());
			servers2deletedFiles.put(server2, new HashSet<String>());
			servers2deletedFiles.put(server3, new HashSet<String>());
		}

		@Override
		public void replicate(String filePath, ServerInfo server) {
			servers2files.get(server).add(filePath);
		}

		@Override
		public void delete(String filePath) {
			filesDeletedFromThisServer.add(filePath);
		}

		@Override
		public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result) {
			Set<String> neededFiles = new HashSet<>();
			Set<String> currentFiles = servers2files.get(server);
			for (String s : forUpload) {
				if (!currentFiles.contains(s)) {
					neededFiles.add(s);
				}
			}
			result.onResult(neededFiles);
			for (String s : forDeletion) {
				servers2deletedFiles.get(server).add(s);
			}
		}

		@Override
		public void updateServerMap(Set<ServerInfo> bootstrap) {
			// ignored
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
		public void postUpdate() {
			// ignored
		}
	}
}

