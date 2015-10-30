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

import java.util.Set;

public interface Commands {
	void replicate(String filePath, ServerInfo server);

	void delete(String filePath);

	void offer(ServerInfo server, Set<String> forUpload, Set<String> forDeletion, ResultCallback<Set<String>> result);

	void updateServerMap(Set<ServerInfo> bootstrap);

	void scheduleTemporaryFileDeletion(String filePath, long waitTime);

	void scan(ResultCallback<Set<String>> callback);

	void postUpdate();
}
