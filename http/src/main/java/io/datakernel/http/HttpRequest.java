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
import io.datakernel.exception.ParseException;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.GzipProcessor.toGzip;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpUtils.nullToEmpty;

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
	private Map<String, String> bodyParameters;
	private int pos;
	private boolean gzip = false;

	// region builders
	private HttpRequest(HttpMethod method) {
		this.method = method;
	}

	public static HttpRequest ofMethod(HttpMethod method) {
		assert method != null;
		return new HttpRequest(method);
	}

	public static HttpRequest get(String url) {
		return ofMethod(GET).withUrl(url);
	}

	public static HttpRequest post(String url) {
		return ofMethod(POST).withUrl(url);
	}

	// common builder methods
	public HttpRequest withHeader(HttpHeader header, ByteBuf value) {
		addHeader(header, value);
		return this;
	}

	public HttpRequest withHeader(HttpHeader header, byte[] value) {
		addHeader(header, value);
		return this;
	}

	public HttpRequest withHeader(HttpHeader header, String value) {
		addHeader(header, value);
		return this;
	}

	public HttpRequest withBody(byte[] array) {
		setBody(array);
		return this;
	}

	public HttpRequest withBody(ByteBuf body) {
		setBody(body);
		return this;
	}

	// specific builder methods
	public HttpRequest withAccept(List<AcceptMediaType> value) {
		setAccept(value);
		return this;
	}

	public HttpRequest withAccept(AcceptMediaType... value) {
		setAccept(value);
		return this;
	}

	public HttpRequest withAcceptCharsets(List<AcceptCharset> values) {
		setAcceptCharsets(values);
		return this;
	}

	public HttpRequest withAcceptCharsets(AcceptCharset... values) {
		setAcceptCharsets(values);
		return this;
	}

	public HttpRequest withCookies(List<HttpCookie> cookies) {
		addCookies(cookies);
		return this;
	}

	public HttpRequest withCookies(HttpCookie... cookie) {
		addCookies(cookie);
		return this;
	}

	public HttpRequest withCookie(HttpCookie cookie) {
		addCookie(cookie);
		return this;
	}

	public HttpRequest withContentType(ContentType contentType) {
		setContentType(contentType);
		return this;
	}

	public HttpRequest withContentType(MediaType mime) {
		setContentType(mime);
		return this;
	}

	public HttpRequest withContentType(MediaType mime, Charset charset) {
		setContentType(mime, charset);
		return this;
	}

	public HttpRequest withDate(Date date) {
		setDate(date);
		return this;
	}

	public HttpRequest withIfModifiedSince(Date date) {
		setIfModifiedSince(date);
		return this;
	}

	public HttpRequest withIfUnModifiedSince(Date date) {
		setIfUnModifiedSince(date);
		return this;
	}

	public HttpRequest withUrl(HttpUri url) {
		setUrl(url);
		return this;
	}

	public HttpRequest withUrl(String url) {
		setUrl(url);
		return this;
	}

	public HttpRequest withRemoteAddress(InetAddress inetAddress) {
		setRemoteAddress(inetAddress);
		return this;
	}

	public HttpRequest withGzipCompression() {
		setGzipCompression();
		return this;
	}
	// endregion

	// region setters
	public void setAccept(List<AcceptMediaType> value) {
		addHeader(ofAcceptContentTypes(HttpHeaders.ACCEPT, value));
	}

	public void setAccept(AcceptMediaType... value) {
		setAccept(Arrays.asList(value));
	}

	public void setAcceptCharsets(List<AcceptCharset> values) {
		addHeader(ofCharsets(HttpHeaders.ACCEPT_CHARSET, values));
	}

	public void setAcceptCharsets(AcceptCharset... values) {
		setAcceptCharsets(Arrays.asList(values));
	}

	public void addCookies(List<HttpCookie> cookies) {
		addHeader(ofCookies(COOKIE, cookies));
	}

	public void addCookies(HttpCookie... cookie) {
		addCookies(Arrays.asList(cookie));
	}

	public void addCookie(HttpCookie cookie) {
		addCookies(Collections.singletonList(cookie));
	}

	public void setContentType(ContentType contentType) {
		setHeader(ofContentType(HttpHeaders.CONTENT_TYPE, contentType));
	}

	public void setContentType(MediaType mime) {
		setContentType(ContentType.of(mime));
	}

	public void setContentType(MediaType mime, Charset charset) {
		setContentType(ContentType.of(mime, charset));
	}

	public void setDate(Date date) {
		setHeader(ofDate(HttpHeaders.DATE, date));
	}

	public void setIfModifiedSince(Date date) {
		setHeader(ofDate(IF_MODIFIED_SINCE, date));
	}

	public void setIfUnModifiedSince(Date date) {
		setHeader(ofDate(IF_UNMODIFIED_SINCE, date));
	}

	public void setUrl(HttpUri url) {
		assert !recycled;
		this.url = url;
		if (!url.isPartial()) {
			setHeader(HttpHeaders.ofString(HttpHeaders.HOST, url.getHostAndPort()));
		}
	}

	public void setUrl(String url) {
		setUrl(HttpUri.ofUrl(url));
	}

	public void setRemoteAddress(InetAddress inetAddress) {
		assert !recycled;
		this.remoteAddress = inetAddress;
	}

	public void setGzipCompression() {
		setHeader(HttpHeaders.ofString(CONTENT_ENCODING, "gzip"));
		gzip = true;
	}
	// endregion

	// region getters
	public List<AcceptMediaType> parseAccept() throws ParseException {
		assert !recycled;
		List<AcceptMediaType> list = new ArrayList<>();
		List<Value> headers = getHeaderValues(ACCEPT);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			AcceptMediaType.parse(value.array, value.offset, value.size, list);
		}
		return list;
	}

	public List<AcceptCharset> parseAcceptCharsets() throws ParseException {
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
	public List<HttpCookie> parseCookies() throws ParseException {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<Value> headers = getHeaderValues(COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			HttpCookie.parseSimple(value.array, value.offset, value.offset + value.size, cookie);
		}
		return cookie;
	}

	public Date parseIfModifiedSince() throws ParseException {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_MODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	public Date parseIfUnModifiedSince() throws ParseException {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_UNMODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}
	// endregion

	// region internal
	public Map<String, String> getParameters() throws ParseException {
		assert !recycled;
		return url.getParameters();
	}

	public Map<String, String> getPostParameters() throws ParseException {
		assert !recycled;
		if (method == POST && getContentType() != null
				&& getContentType().getMediaType() == MediaTypes.X_WWW_FORM_URLENCODED
				&& body.readPosition() != body.writePosition()) {
			if (bodyParameters == null) {
				bodyParameters = HttpUtils.extractParameters(decodeAscii(getBody()));
			}
			return bodyParameters;
		} else {
			return Collections.emptyMap();
		}
	}

	public String getPostParameter(String name) throws ParseException {
		return getPostParameters().get(name);
	}

	public String getParameter(String name) throws ParseException {
		assert !recycled;
		return url.getParameter(name);
	}

	int getPos() {
		return pos;
	}

	void setPos(int pos) {
		this.pos = pos;
	}

	public boolean isHttps() {
		return getUrl().getSchema().equals("https");
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
			if (gzip) {
				try {
					body = toGzip(body);
				} catch (ParseException ignored) {
					throw new AssertionError("Can't encode http request body");
				}
			}
			setHeader(HttpHeaders.ofDecimal(HttpHeaders.CONTENT_LENGTH, body == null ? 0 : body.readRemaining()));
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

		return buf;
	}

	@Override
	public String toString() {
		String host = nullToEmpty(getHeader(HttpHeaders.HOST));
		if (url == null)
			return host;
		return host + url.getPathAndQuery();
	}

	public String getFullUrl() {
		String host = nullToEmpty(getHeader(HttpHeaders.HOST));
		if (url == null)
			return host;
		return host + url.getPathAndQuery();
	}
	// endregion
}
