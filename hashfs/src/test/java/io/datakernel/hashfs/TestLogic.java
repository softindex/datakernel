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

package io.datakernel.hashfs;

import io.datakernel.async.ResultCallback;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.*;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static org.junit.Assert.*;

public class TestLogic {

	private final ServerInfo local = new ServerInfo(0, new InetSocketAddress("http://127.0.0.1", 1234), 0.1);
	private final ServerInfo server1 = new ServerInfo(1, new InetSocketAddress("http://127.0.0.1", 2345), 0.1);
	private final ServerInfo server2 = new ServerInfo(2, new InetSocketAddress("http://127.0.0.1", 3456), 0.1);
	private final ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("http://127.0.0.1", 4567), 0.1);

	{
		local.updateState(1);
		server1.updateState(1);
		server2.updateState(1);
		server3.updateState(1);
	}

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
		CommandsMock cMock = new CommandsMock();
		final Logic logic = getLogic(cMock);
		logic.update(1);

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
		CommandsMock cMock = new CommandsMock();
		final Logic logic = getLogic(cMock);
		logic.update(1);

		assertFalse(logic.canUpload(b));

		assertTrue(logic.canUpload(newFile));
		logic.onUploadStart(newFile);
		logic.onUploadComplete(newFile);
		assertTrue(cMock.scheduledDeletions.contains(newFile) && cMock.scheduledDeletions.size() == 1);
		assertTrue(logic.canApprove(newFile));
		logic.onApprove(newFile);
		logic.update(1);

		Set<String> real2 = cMock.servers2files.get(server2);

		assertTrue(real2.contains(newFile));
	}

	@Test
	public void testDelete() {
		CommandsMock cMock = new CommandsMock();
		final Logic logic = getLogic(cMock);
		logic.update(1);
		logic.onReplicationStart(a);
		logic.onReplicationStart(a);
		// first time due to the fact that there was only minimum safe replicas quantity in the whole system
		// logic left the file and it is still accessible from this server unless this node manage to replicate it
		logic.onReplicationComplete(server1, a);
		logic.onReplicationComplete(server2, a);
		assertTrue(logic.canDelete(a));
		logic.update(1);
		assertFalse(logic.canDelete(a));

		logic.onReplicationStart(e);
		logic.onReplicationComplete(server2, e);
		assertTrue(logic.canDelete(e));
		logic.onDeletionStart(e);
		logic.onDeleteComplete(e);
		assertTrue(logic.canUpload(e));
		logic.update(1);

		assertTrue(cMock.servers2deletedFiles.get(server2).contains(e));

		logic.onReplicationStart(f);
		logic.onReplicationComplete(server2, f);
		logic.onDownloadStart(f);
		assertFalse(logic.canDelete(f));
		logic.onDownloadComplete(f);
		assertTrue(logic.canDelete(f));
	}

	@Test
	public void testDownload() {
		CommandsMock cMock = new CommandsMock();
		final Logic logic = getLogic(cMock);
		logic.update(1);
		logic.update(1);
		logic.update(1);
		assertFalse(logic.canDownload(a));
	}

	@Test
	public void testOffer() {
		CommandsMock cMock = new CommandsMock();
		final Logic logic = getLogic(cMock);
		logic.update(1);

		logic.onReplicationComplete(server1, a);
		logic.onReplicationComplete(server2, a);
		logic.onReplicationComplete(server1, d);
		logic.onReplicationComplete(server2, e);
		logic.onReplicationComplete(server3, c);
		logic.onReplicationComplete(server2, f);
		logic.onReplicationComplete(server1, g);
		logic.onReplicationComplete(server3, g);
		logic.onReplicationComplete(server2, b);
		logic.onReplicationComplete(server3, b);

		logic.update(1);

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
		CommandsMock cMock = new CommandsMock();
		final Logic logic = getLogic(cMock);

		logic.onShowAliveRequest(1, new ResultCallback<Set<ServerInfo>>() {
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

	private Logic getLogic(Commands commands) {
		HashingStrategy hashing = new HashingMock();
		final Logic logic = LogicImpl.buildInstance(local, bootstrap)
				.setMaxReplicaQuantity(2)
				.setHashing(hashing)
				.build();
		logic.wire(commands);
		logic.start(ignoreCompletionCallback());
		return logic;
	}

	class HashingMock implements HashingStrategy {
		@Override
		public List<ServerInfo> sortServers(String fileName, Collection<ServerInfo> servers) {
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
		public void replicate(ServerInfo server, String fileName) {
			servers2files.get(server).add(fileName);
		}

		@Override
		public void delete(String fileName) {
			filesDeletedFromThisServer.add(fileName);
		}

		@Override
		public void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> callback) {
			Set<String> neededFiles = new HashSet<>();
			Set<String> currentFiles = servers2files.get(server);
			for (String s : forUpload) {
				if (!currentFiles.contains(s)) {
					neededFiles.add(s);
				}
			}
			callback.onResult(neededFiles);
			for (String s : forDeletion) {
				servers2deletedFiles.get(server).add(s);
			}
		}

		@Override
		public void updateServerMap(Set<ServerInfo> bootstrap) {
			// ignored
		}

		@Override
		public void scheduleCommitCancel(String fileName, long waitTime) {
			scheduledDeletions.add(fileName);
		}

		@Override
		public void scan(ResultCallback<Set<String>> callback) {
			callback.onResult(new HashSet<>(Arrays.asList(a, b, c, d, e, f, g)));
		}

		@Override
		public void scheduleUpdate() {
			// ignored
		}
	}
}

