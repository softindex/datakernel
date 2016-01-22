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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;

import java.net.InetAddress;
import java.util.*;

import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.util.Utils.nullToEmpty;
import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represent the HTTP request which {@link AsyncHttpClient} send to {@link AsyncHttpServer}. It must have only one owner in
 * each  part of time. After creating in server {@link HttpResponse} it will be recycled and you can not
 * use it later.
 */
public final class HttpRequest extends HttpMessage {
	private final HttpMethod method;
	private HttpUri url;
	private InetAddress remoteAddress;
	private Map<String, String> urlParameters;
	private int pos;

	private HttpRequest(HttpMethod method) {
		this.method = method;
	}

	public static HttpRequest create(HttpMethod method) {
		assert method != null;
		return new HttpRequest(method);
	}

	public static HttpRequest get(String url) {
		return create(GET).url(url);
	}

	public static HttpRequest post(String url) {
		return create(HttpMethod.POST).url(url);
	}

	// common builder methods
	public HttpRequest header(HttpHeader header, ByteBuf value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	public HttpRequest header(HttpHeader header, byte[] value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	public HttpRequest header(HttpHeader header, String value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	public HttpRequest body(byte[] array) {
		assert !recycled;
		return body(ByteBuf.wrap(array));
	}

	public HttpRequest body(ByteBuf body) {
		assert !recycled;
		setBody(body);
		return this;
	}

	// specific builder methods
	public HttpRequest accept(AcceptMediaType... value) {
		return accept(Arrays.asList(value));
	}

	public HttpRequest accept(List<AcceptMediaType> value) {
		assert !recycled;
		addHeader(ofAcceptContentTypes(HttpHeaders.ACCEPT, value));
		return this;
	}

	public HttpRequest acceptCharsets(AcceptCharset... values) {
		return acceptCharsets(Arrays.asList(values));
	}

	public HttpRequest acceptCharsets(List<AcceptCharset> values) {
		assert !recycled;
		addHeader(ofCharsets(HttpHeaders.ACCEPT_CHARSET, values));
		return this;
	}

	public HttpRequest clientCookies(HttpCookie... cookie) {
		return clientCookies(Arrays.asList(cookie));
	}

	public HttpRequest clientCookies(List<HttpCookie> cookies) {
		assert !recycled;
		addHeader(ofCookies(COOKIE, cookies));
		return this;
	}

	public HttpRequest contentType(ContentType value) {
		assert !recycled;
		setHeader(ofContentType(HttpHeaders.CONTENT_TYPE, value));
		return this;
	}

	public HttpRequest date(Date value) {
		assert !recycled;
		setHeader(ofDate(HttpHeaders.DATE, value));
		return this;
	}

	public HttpRequest ifModifiedSince(Date value) {
		assert !recycled;
		setHeader(ofDate(IF_MODIFIED_SINCE, value));
		return this;
	}

	public HttpRequest ifUnModifiedSince(Date value) {
		assert !recycled;
		setHeader(ofDate(IF_UNMODIFIED_SINCE, value));
		return this;
	}

	public HttpRequest url(HttpUri url) {
		assert !recycled;
		this.url = url;
		if (!url.isPartial()) {
			setHeader(HttpHeaders.HOST, url.getHostAndPort());
		}
		return this;
	}

	public HttpRequest url(String url) {
		assert !recycled;
		return url(HttpUri.ofUrl(url));
	}

	public HttpRequest remoteAddress(InetAddress inetAddress) {
		assert !recycled;
		this.remoteAddress = inetAddress;
		return this;
	}

	// getters
	public List<AcceptMediaType> getAccept() {
		assert !recycled;
		List<AcceptMediaType> list = new ArrayList<>();
		List<Value> headers = getHeaders(ACCEPT);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			AcceptMediaType.parse(value.array, value.offset, value.size, list);
		}
		return list;
	}

	public List<AcceptCharset> getAcceptCharsets() {
		assert !recycled;
		List<AcceptCharset> charsets = new ArrayList<>();
		List<Value> headers = getHeaders(ACCEPT_CHARSET);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			AcceptCharset.parse(value.array, value.offset, value.size, charsets);
		}
		return charsets;
	}

	public List<HttpCookie> getCookies() {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<Value> headers = getHeaders(COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			HttpCookie.parseSimple(value.array, value.offset, value.offset + value.size, cookie);
		}
		return cookie;
	}

	public Date getIfModifiedSince() {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeader(IF_MODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	public Date getIfUnModifiedSince() {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeader(IF_UNMODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	// internal
	public Map<String, String> getParameters() {
		assert !recycled;
		return url.getParameters();
	}

	int getPos() {
		return pos;
	}

	void setPos(int pos) {
		this.pos = pos;
	}

	public HttpMethod getMethod() {
		assert !recycled;
		return method;
	}

	public HttpUri getUrl() {
		assert !recycled;
		return url;
	}

	public InetAddress getRemoteAddress() {
		assert !recycled;
		return remoteAddress;
	}

	public String getPath() {
		assert !recycled;
		return url.getPath();
	}

	String getRelativePath() {
		assert !recycled;
		String path = url.getPath();
		if (pos < path.length()) {
			return path.substring(pos);
		}
		return "";
	}

	private final static int LONGEST_HTTP_METHOD_SIZE = 12;
	private static final byte[] HTTP_1_1 = encodeAscii(" HTTP/1.1");
	private static final int HTTP_1_1_SIZE = HTTP_1_1.length;

	public String getParameter(String name) {
		assert !recycled;
		return url.getParameter(name);
	}

	public String getUrlParameter(String key) {
		return urlParameters == null ? null : urlParameters.get(key);
	}

	String pollUrlPart() {
		String path = url.getPath();
		if (pos < path.length()) {
			int start = pos + 1;
			pos = path.indexOf('/', start);
			String part;
			if (pos == -1) {
				part = path.substring(start);
				pos = path.length();
			} else {
				part = path.substring(start, pos);
			}
			return part;
		} else {
			return "";
		}
	}

	void removeUrlParameter(String key) {
		urlParameters.remove(key);
	}

	void putUrlParameter(String key, String value) {
		if (urlParameters == null) {
			urlParameters = new HashMap<>();
		}
		urlParameters.put(key, value);
	}

	ByteBuf write() {
		assert !recycled;
		if (body != null || method != GET) {
			setHeader(HttpHeaders.ofDecimal(HttpHeaders.CONTENT_LENGTH, body == null ? 0 : body.remaining()));
		}
		int estimatedSize = estimateSize(LONGEST_HTTP_METHOD_SIZE
				+ 1 // SPACE
				+ url.getPathAndQuery().length())
				+ HTTP_1_1_SIZE;
		ByteBuf buf = ByteBufPool.allocate(estimatedSize);

		method.write(buf);
		buf.put(SP);
		putAscii(buf, url.getPathAndQuery());
		buf.put(HTTP_1_1);

		writeHeaders(buf);
		writeBody(buf);

		buf.flip();
		return buf;
	}

	@Override
	public String toString() {
		String host = nullToEmpty(getHeaderString(HttpHeaders.HOST));
		if (url == null)
			return host;
		return host + url.getPathAndQuery();
	}
}
