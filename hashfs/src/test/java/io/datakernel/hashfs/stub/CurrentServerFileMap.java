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

package io.datakernel.hashfs.stub;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.protocol.gson.HashFsGsonServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CurrentServerFileMap implements ServerStatusListener {

	private static final Logger logger = LoggerFactory.getLogger(CurrentServerFileMap.class);

	private final SetMultimap<Integer, String> currentServerFiles = HashMultimap.create();
	private SetMultimap<Integer, String> expectedState;
	private CompletionCallback onStateReceived;

	@Override
	public void onFileUploaded(int serverId, List<String> fileName) {
		addFiles(serverId, fileName);
	}

	@Override
	public void onFileDeletedByServer(int serverId, List<String> fileName) {
		removeFiles(serverId, fileName);
	}

	@Override
	public void onFileDeletedByUser(int serverId, List<String> fileName) {
		removeFiles(serverId, fileName);
	}

	private void addFiles(int serverId, List<String> fileName) {
		for (String file : fileName) {
			currentServerFiles.put(serverId, file);
		}
		checkExpectedState();
	}

	private void removeFiles(int serverId, List<String> fileName) {
		if (!currentServerFiles.containsKey(serverId)) return;
		for (String file : fileName) {
			currentServerFiles.get(serverId).remove(file);
		}
		checkExpectedState();
	}

	private void checkExpectedState() {
		if (currentServerFiles.asMap().equals(expectedState.asMap()) && onStateReceived != null) {
			onStateReceived.onComplete();
		}
	}

	public void setExpectedState(HashMultimap<Integer, String> expectedState, CompletionCallback callback) {
		this.expectedState = expectedState;
		this.onStateReceived = callback;
	}

	public void setExpectedStateWithClose(HashMultimap<Integer, String> expectedState, final NioEventloop eventloop, final List<HashFsGsonServer> servers, final AtomicBoolean finishStatus) {
		this.expectedState = expectedState;
		this.onStateReceived = new CompletionCallback() {

			private void closeServers(boolean closeValue) {
				for (HashFsGsonServer server : servers) {
					server.closeAll();
				}
				eventloop.breakEventloop();
				closeValue &= finishStatus.get();
				finishStatus.set(closeValue);
			}

			@Override
			public void onComplete() {
				logger.info("Expected state waited. Shutdown servers.");
				closeServers(true);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Expected state waited error ", exception);
				closeServers(false);
			}
		};
	}

	public void setExpectedStateWithBreak(HashMultimap<Integer, String> expectedState, final NioEventloop eventloop, final AtomicBoolean finishStatus) {
		this.expectedState = expectedState;
		this.onStateReceived = new CompletionCallback() {

			private void closeServers(boolean closeValue) {
				eventloop.breakEventloop();
				closeValue &= finishStatus.get();
				finishStatus.set(closeValue);
			}

			@Override
			public void onComplete() {
				logger.info("Expected state waited. Break.");
				closeServers(true);
			}

			@Override
			public void onException(Exception exception) {
				closeServers(false);
			}
		};
	}
}
