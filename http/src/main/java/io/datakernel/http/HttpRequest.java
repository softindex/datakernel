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
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpUtils.nullToEmpty;
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
		setHeader(header, value);
		return this;
	}

	public HttpRequest header(HttpHeader header, byte[] value) {
		setHeader(header, value);
		return this;
	}

	public HttpRequest header(HttpHeader header, String value) {
		setHeader(header, value);
		return this;
	}

	public HttpRequest body(byte[] array) {
		return body(ByteBuf.wrap(array));
	}

	public HttpRequest body(ByteBuf body) {
		setBody(body);
		return this;
	}

	// specific builder methods
	public HttpRequest accept(List<AcceptMediaType> value) {
		addHeader(ofAcceptContentTypes(HttpHeaders.ACCEPT, value));
		return this;
	}

	public HttpRequest accept(AcceptMediaType... value) {
		return accept(Arrays.asList(value));
	}

	public HttpRequest acceptCharsets(List<AcceptCharset> values) {
		addHeader(ofCharsets(HttpHeaders.ACCEPT_CHARSET, values));
		return this;
	}

	public HttpRequest acceptCharsets(AcceptCharset... values) {
		return acceptCharsets(Arrays.asList(values));
	}

	public HttpRequest cookies(List<HttpCookie> cookies) {
		addHeader(ofCookies(COOKIE, cookies));
		return this;
	}

	public HttpRequest cookies(HttpCookie... cookie) {
		return cookies(Arrays.asList(cookie));
	}

	public HttpRequest cookie(HttpCookie cookie) {
		return cookies(Collections.singletonList(cookie));
	}

	public HttpRequest contentType(ContentType contentType) {
		setHeader(ofContentType(HttpHeaders.CONTENT_TYPE, contentType));
		return this;
	}

	public HttpRequest date(Date date) {
		setHeader(ofDate(HttpHeaders.DATE, date));
		return this;
	}

	public HttpRequest ifModifiedSince(Date date) {
		setHeader(ofDate(IF_MODIFIED_SINCE, date));
		return this;
	}

	public HttpRequest ifUnModifiedSince(Date date) {
		setHeader(ofDate(IF_UNMODIFIED_SINCE, date));
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
		return url(HttpUri.ofUrl(url));
	}

	public HttpRequest remoteAddress(InetAddress inetAddress) {
		assert !recycled;
		this.remoteAddress = inetAddress;
		return this;
	}

	// getters
	public List<AcceptMediaType> parseAccept() {
		assert !recycled;
		List<AcceptMediaType> list = new ArrayList<>();
		List<Value> headers = getHeaderValues(ACCEPT);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			AcceptMediaType.parse(value.array, value.offset, value.size, list);
		}
		return list;
	}

	public List<AcceptCharset> parseAcceptCharsets() {
		assert !recycled;
		List<AcceptCharset> charsets = new ArrayList<>();
		List<Value> headers = getHeaderValues(ACCEPT_CHARSET);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			AcceptCharset.parse(value.array, value.offset, value.size, charsets);
		}
		return charsets;
	}

	@Override
	public List<HttpCookie> parseCookies() {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<Value> headers = getHeaderValues(COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			HttpCookie.parseSimple(value.array, value.offset, value.offset + value.size, cookie);
		}
		return cookie;
	}

	public Date parseIfModifiedSince() {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_MODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	public Date parseIfUnModifiedSince() {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_UNMODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	// internal
	public Map<String, String> getParameters() {
		assert !recycled;
		if (method == POST && getContentType() != null
				&& getContentType().getMediaType() == MediaTypes.X_WWW_FORM_URLENCODED
				&& body.position() != body.limit()) {
			return ensurePostParameters();
		}
		return url.getParameters();
	}

	private Map<String, String> ensurePostParameters() {
		Map<String, String> parameters = url.getParameters();
		parameters.putAll(HttpUtils.parse(decodeAscii(getBody())));
		return parameters;
	}

	public String getParameter(String name) {
		assert !recycled;
		if (method == POST && getContentType() != null
				&& getContentType().getMediaType() == MediaTypes.X_WWW_FORM_URLENCODED) {
			return ensurePostParameters().get(name);
		}
		return url.getParameter(name);
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
		String host = nullToEmpty(getHeader(HttpHeaders.HOST));
		if (url == null)
			return host;
		return host + url.getPathAndQuery();
	}
}
