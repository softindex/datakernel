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

package io.datakernel.http.middleware;

import io.datakernel.http.HttpMethod;

import java.util.List;
import java.util.regex.Pattern;

final class HttpRequestSuccessHandler {
	private final HttpMethod method;
	private final Pattern pattern;
	private final HttpSuccessHandler handler;
	private final List<String> groupNames;

	public HttpRequestSuccessHandler(HttpSuccessHandler handler, HttpMethod method, Pattern pattern, List<String> groupNames) {
		this.pattern = pattern;
		this.handler = handler;
		this.method = method;
		this.groupNames = groupNames;
	}

	public HttpSuccessHandler getHandler() {
		return handler;
	}

	public HttpMethod getMethod() {
		return method;
	}

	public Pattern getPattern() {
		return pattern;
	}

	public List<String> getGroupNames() {
		return groupNames;
	}
}
