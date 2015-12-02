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
import io.datakernel.util.ByteBufStrings;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.*;

import static com.google.common.base.Strings.nullToEmpty;
import static io.datakernel.http.HttpHeader.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represent the HTTP request which {@link HttpClientAsync} send to {@link AsyncHttpServer}. It must have only one owner in
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

	public HttpRequest body(ByteBuf body) {
		assert !recycled;
		setBody(body);
		return this;
	}

	// specific builder methods
	public HttpRequest setAccept(List<ContentType> value) {
		assert !recycled;
		addContentTypeHeader(HttpHeader.ACCEPT, value);
		return this;
	}

	public HttpRequest setAccept(ContentType value) {
		assert !recycled;
		addContentTypeHeader(HttpHeader.ACCEPT, value);
		return this;
	}

	public HttpRequest setContentType(List<ContentType> value) {
		assert !recycled;
		addContentTypeHeader(HttpHeader.CONTENT_TYPE, value);
		return this;
	}

	public HttpRequest setContentType(ContentType value) {
		assert !recycled;
		addContentTypeHeader(HttpHeader.CONTENT_TYPE, value);
		return this;
	}

	public HttpRequest setAcceptCharsetPairs(List<HttpUtils.Pair<Charset>> values) {
		assert !recycled;
		addCharsetPairHeader(HttpHeader.ACCEPT_CHARSET, values);
		return this;
	}

	public HttpRequest setAcceptCharset(List<Charset> values) {
		assert !recycled;
		addCharsetRawHeader(HttpHeader.ACCEPT_CHARSET, values);
		return this;
	}

	public HttpRequest setAcceptCharset(Charset value) {
		assert !recycled;
		addCharsetHeader(HttpHeader.ACCEPT_CHARSET, value);
		return this;
	}

	public HttpRequest setContentLength(int value) {
		assert !recycled;
		setHeader(HttpHeader.CONTENT_LENGTH, value);
		return this;
	}

	public HttpRequest setDate(Date value) {
		assert !recycled;
		setHeader(HttpHeader.DATE, value);
		return this;
	}

	public HttpRequest setIfModifiedSince(Date value) {
		assert !recycled;
		setHeader(HttpHeader.IF_MODIFIED_SINCE, value);
		return this;
	}

	public HttpRequest setIfUnModifiedSince(Date value) {
		assert !recycled;
		setHeader(HttpHeader.IF_UNMODIFIED_SINCE, value);
		return this;
	}

	public HttpRequest cookie(HttpCookie cookie) {
		assert !recycled;
		addCookieHeader(COOKIE, Collections.singletonList(cookie));
		return this;
	}

	public HttpRequest cookie(List<HttpCookie> cookies) {
		assert !recycled;
		addCookieHeader(COOKIE, cookies);
		return this;
	}

	public HttpRequest url(HttpUri url) {
		assert !recycled;
		this.url = url;
		if (!url.isPartial()) {
			setHeader(HttpHeader.HOST, url.getHostAndPort());
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
	public List<HttpCookie> getCookies() {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<String> headers = getHeaderStrings(COOKIE);
		for (String header : headers) {
			HttpCookie.parseSimple(header, cookie);
		}
		return cookie;
	}

	public List<HttpUtils.Pair<Charset>> getAcceptPairCharsets() {
		assert !recycled;
		List<HttpUtils.Pair<Charset>> charsets = new ArrayList<>();
		List<String> headers = getHeaderStrings(ACCEPT_CHARSET);
		for (String header : headers) {
			CharsetUtils.parse(header, charsets);
		}
		return charsets;
	}

	public List<Charset> getAcceptCharsets() {
		assert !recycled;
		List<Charset> charsets = new ArrayList<>();
		List<String> headers = getHeaderStrings(ACCEPT_CHARSET);
		for (String header : headers) {
			charsets.addAll(CharsetUtils.parse(header));
		}
		return charsets;
	}

	public List<ContentType> getAccept() {
		assert !recycled;
		List<ContentType> cts = new ArrayList<>();
		List<String> headers = getHeaderStrings(ACCEPT);
		for (String header : headers) {
			ContentType.parse(header, cts);
		}
		return cts;
	}

	public Date getIfModifiedSince() {
		assert !recycled;
		String value = getHeaderString(HttpHeader.IF_MODIFIED_SINCE);
		if (value != null && !value.equals("")) {
			long timestamp = HttpDate.parse(ByteBufStrings.encodeAscii(value), 0);
			return new Date(timestamp);
		}
		return null;
	}

	public Date getIfUnModifiedSince() {
		assert !recycled;
		String value = getHeaderString(HttpHeader.IF_UNMODIFIED_SINCE);
		if (value != null && !value.equals("")) {
			long timestamp = HttpDate.parse(ByteBufStrings.encodeAscii(value), 0);
			return new Date(timestamp);
		}
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
			setHeader(HttpHeader.ofDecimal(HttpHeader.CONTENT_LENGTH, body == null ? 0 : body.remaining()));
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
		String host = nullToEmpty(getHeaderString(HttpHeader.HOST));
		if (url == null)
			return host;
		return host + url.getPathAndQuery();
	}
}
