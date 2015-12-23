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

package io.datakernel.http;

import io.datakernel.http.server.AsyncHttpServlet;

import java.nio.charset.Charset;

public abstract class StaticServlet implements AsyncHttpServlet {
	public static final Charset DEFAULT_TXT_ENCODING = Charset.forName("UTF-8");
	public static final String DEFAULT_INDEX_FILE_NAME = "index.html"; // response for get request asking for root
	protected ContentTypeResolver resolver;

	public StaticServlet() {

	}

	public void setContentTypeResolver(ContentTypeResolver resolver) {
		this.resolver = resolver;
	}

	protected String getTrail(HttpRequest request) {
		String trail = request.getRelativePath();
		if (request.getMethod() == HttpMethod.GET && trail.equals("/")) {
			trail = DEFAULT_INDEX_FILE_NAME;
		} else {
			trail = trail.substring(1); // removing initial '/'
		}
		return trail;
	}

	protected ContentType defineContentType(String trail) {
		int pos = trail.lastIndexOf(".");
		if (pos != -1) {
			trail = trail.substring(pos + 1);
		}
		ContentType type = null;
		if (resolver != null) {
			type = resolver.defineContentType(trail);
		}
		if (type == null) {
			type = ContentType.getByExt(trail);
		}
		if (type != null && ContentType.isText(type)) {
			type = type.setCharsetEncoding(DEFAULT_TXT_ENCODING);
		}
		return type == null ? ContentType.ANY : type;
	}
}