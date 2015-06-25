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

import io.datakernel.http.HttpRequest;

import java.util.Map;

public abstract class MiddlewareRequestContext extends AbstractMiddlewareRequestContext {
	private final Map<String, String> urlParameters;
	private final String urlTrail;

	MiddlewareRequestContext(Map<String, String> urlParameters, String urlTrail, Map<Object, Object> attachment) {
		super(attachment);
		this.urlParameters = urlParameters;
		this.urlTrail = urlTrail;
	}

	abstract public void next(HttpRequest request);

	public String getUrlParameter(String name) {
		return urlParameters.get(name);
	}

	public String getUrlTrail() {
		return urlTrail;
	}
}
