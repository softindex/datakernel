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
import io.datakernel.http.HttpResponse;

import java.util.Map;

abstract class AbstractMiddlewareRequestContext {
	private final Map<Object, Object> attachment;

	abstract public void next(Exception exception, HttpRequest request);

	abstract public void send(HttpResponse response);

	AbstractMiddlewareRequestContext(Map<Object, Object> attachment) {
		this.attachment = attachment;
	}

	public Map<Object, Object> getAttachment() {
		return attachment;
	}

	public Object getAttachmentForKey(Object key) {
		return attachment.get(key);
	}

	public void setAttachment(Object key, Object value) {
		attachment.put(key, value);
	}
}
