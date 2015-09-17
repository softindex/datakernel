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

import java.net.HttpCookie;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Strings.nullToEmpty;
import static io.datakernel.http.HttpHeader.CONTENT_LENGTH;
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

	/**
	 * Return the new HttpRequest with {@link HttpMethod}.
	 *
	 * @param method method for request
	 * @return the  new HttpRequest with request method.
	 */
	public static HttpRequest create(HttpMethod method) {
		assert method != null;
		return new HttpRequest(method);
	}

	/**
	 * Creates the new HttpRequest with method get and URL from argument
	 *
	 * @param url URL for new HttpRequest
	 * @return the  new HttpRequest
	 */
	public static HttpRequest get(String url) {
		return create(GET).url(url);
	}

	/**
	 * Creates the new HttpRequest with method post and URL from argument
	 *
	 * @param url URL for new HttpRequest
	 * @return the  new HttpRequest
	 */
	public static HttpRequest post(String url) {
		return create(HttpMethod.POST).url(url);
	}

	// common builder methods

	/**
	 * Sets the header for this HttpRequest
	 *
	 * @param value value of header
	 * @return this HttpRequest
	 */
	public HttpRequest header(HttpHeaderValue value) {
		assert !recycled;
		setHeader(value);
		return this;
	}

	/**
	 * Adds the header with value as ByteBuf for this HttpRequest
	 *
	 * @param header header for this HttpRequest
	 * @param value  value of this header
	 * @return this HttpRequest
	 */
	public HttpRequest header(HttpHeader header, ByteBuf value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	/**
	 * Adds the header with value as array of bytes for this HttpRequest
	 *
	 * @param header header for this HttpRequest
	 * @param value  value of this header
	 * @return this HttpRequest
	 */
	public HttpRequest header(HttpHeader header, byte[] value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	/**
	 * Adds the header with value as string for this HttpRequest
	 *
	 * @param header header for this HttpRequest
	 * @param value  value of this header
	 * @return this HttpRequest
	 */
	public HttpRequest header(HttpHeader header, String value) {
		assert !recycled;
		setHeader(header, value);
		return this;
	}

	/**
	 * Adds the collection of headers to this HttpRequest
	 *
	 * @param headers collection with headers and its values
	 * @return this HttpRequest
	 */
	public HttpRequest headers(Collection<HttpHeaderValue> headers) {
		assert !recycled;
		setHeaders(headers);
		return this;
	}

	/**
	 * Sets the body to this HttpRequest as ByteBuf
	 *
	 * @param body new body
	 * @return this HttpRequest
	 */
	public HttpRequest body(ByteBuf body) {
		assert !recycled;
		setBody(body);
		return this;
	}

	/**
	 * Sets the header CONTENT_TYPE
	 *
	 * @param contentType value of header CONTENT_TYPE
	 * @return this HttpRequest
	 */
	public HttpRequest contentType(String contentType) {
		assert !recycled;
		setHeader(HttpHeader.CONTENT_TYPE, contentType);
		return this;
	}

	// specific builder methods

	/**
	 * Adds the header COOKIE
	 *
	 * @param cookie value of header COOKIE
	 * @return this HttpRequest
	 */
	public HttpRequest cookie(HttpCookie cookie) {
		assert !recycled;
		addHeader(HttpHeader.COOKIE, cookie.toString());
		return this;
	}

	/**
	 * Adds the header COOKIE
	 *
	 * @param cookies collection with cookies for setting
	 * @return this HttpResponse
	 */
	public HttpRequest cookie(Collection<HttpCookie> cookies) {
		assert !recycled;
		String s = HttpUtils.cookiesToString(cookies);
		addHeader(HttpHeader.COOKIE, s);
		return this;
	}

	/**
	 * Sets URL for this HttpRequest
	 *
	 * @param url new URL
	 * @return this HttpRequest
	 */
	public HttpRequest url(HttpUri url) {
		assert !recycled;
		this.url = url;
		if (!url.isPartial()) {
			setHeader(HttpHeader.HOST, url.getHostAndPort());
		}
		return this;
	}

	/**
	 * Sets the  URL as string for this HttpRequest
	 *
	 * @param url new URL
	 * @return this HttpRequest
	 */
	public HttpRequest url(String url) {
		assert !recycled;
		return url(HttpUri.ofUrl(url));
	}

	/**
	 * Gets parameters form URL of this HttpRequest
	 *
	 * @return collections with parameters
	 */
	public Map<String, String> getParameters() {
		assert !recycled;
		return url.getParameters();
	}

	public HttpRequest remoteAddress(InetAddress inetAddress) {
		assert !recycled;
		this.remoteAddress = inetAddress;
		return this;
	}

	// getters

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

	/**
	 * Creates HttpRequest as ByteBuf
	 *
	 * @return HttpRequest as ByteBuf
	 */
	public ByteBuf write() {
		assert !recycled;
		if (body != null || method != GET) {
			setHeader(HttpHeader.ofDecimal(CONTENT_LENGTH, body == null ? 0 : body.remaining()));
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
