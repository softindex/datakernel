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

import io.datakernel.hashfs.ServerInfo;
import io.datakernel.hashfs.ServerStatusNotifier;

import java.util.Collections;
import java.util.List;

public class SimpleServerStatusNotifier implements ServerStatusNotifier {

	private final ServerStatusListener statusListener;

	public SimpleServerStatusNotifier(ServerStatusListener statusListener) {
		this.statusListener = statusListener;
	}

	@Override
	public void onInit(ServerInfo myId, List<String> files) {
		statusListener.onFileUploaded(myId.serverId, files);
	}

	@Override
	public void onFileUploaded(ServerInfo myId, String fileName) {
		statusListener.onFileUploaded(myId.serverId, Collections.singletonList(fileName));
	}

	@Override
	public void onFileDeletedByServer(ServerInfo myId, String fileName) {
		statusListener.onFileDeletedByServer(myId.serverId, Collections.singletonList(fileName));
	}

	@Override
	public void onFileDeletedByUser(ServerInfo myId, String fileName) {
		statusListener.onFileDeletedByUser(myId.serverId, Collections.singletonList(fileName));
	}
}
