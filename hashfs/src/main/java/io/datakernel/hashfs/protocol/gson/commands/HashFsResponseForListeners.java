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

package io.datakernel.hashfs.protocol.gson.commands;

import java.util.Collections;
import java.util.List;

public class HashFsResponseForListeners extends HashFsResponse {

	public final int serverId;
	public final List<String> fileName;
	public final Operation operation;

	public HashFsResponseForListeners(int serverId, String fileName, Operation operation) {
		this(serverId, Collections.singletonList(fileName), operation);
	}

	public HashFsResponseForListeners(int serverId, List<String> fileName, Operation operation) {
		this.serverId = serverId;
		this.fileName = fileName;
		this.operation = operation;
	}

	public enum Operation {UPLOADED, DELETED_BY_SERVER, DELETED_BY_USER}

}
