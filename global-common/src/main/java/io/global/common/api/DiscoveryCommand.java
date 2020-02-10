/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.global.common.api;

import io.datakernel.http.HttpPathPart;

public enum DiscoveryCommand implements HttpPathPart {
	ANNOUNCE("announce"),
	FIND("find"),
	FIND_ALL("findAll"),
	SHARE_KEY("shareKey"),
	GET_SHARED_KEY("getSharedKey"),
	GET_SHARED_KEYS("getSharedKeys");

	private final String pathPart;

	DiscoveryCommand(String pathPart) {
		this.pathPart = pathPart;
	}

	@Override
	public String toString() {
		return pathPart;
	}
}
