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

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.GzipProcessorUtils.toGzip;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;

/**
 * Represents the HTTP request which {@link AsyncHttpClient} sends to
 * {@link AsyncHttpServer}. It must have only one owner in each  part of time.
 * After creating an {@link HttpResponse} in a server, it will be recycled and
 * can not be used later.
 * <p>
 * {@code HttpRequest} class provides methods which can be used intuitively for
 * creating and configuring an HTTP request.
 */
public final class HttpRequest extends HttpMessage {
	private final HttpMethod method;
	private UrlParser url;
	private InetAddress remoteAddress;

	private Map<String, String> bodyParameters;
	private Map<String, String> pathParameters;

	// region builders
	private HttpRequest(HttpMethod method) {
		this.method = method;
	}

	public static HttpRequest of(HttpMethod method, String url) throws IllegalArgumentException {
		assert method != null;
		HttpRequest request = new HttpRequest(method);
		request.setUrl(url);
		return request;
	}

	static HttpRequest of(HttpMethod method, UrlParser url) {
		assert method != null;
		HttpRequest request = new HttpRequest(method);
		request.url = url;
		return request;
	}

	public static HttpRequest get(String url) {
		return HttpRequest.of(GET, url);
	}

	public static HttpRequest post(String url) {
		return HttpRequest.of(POST, url);
	}

	// common builder methods
	public HttpRequest withUrl(String url) throws IllegalArgumentException {
		setUrl(url);
		return this;
	}

	public HttpRequest withRemoteAddress(InetAddress inetAddress) {
		setRemoteAddress(inetAddress);
		return this;
	}

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

	public HttpRequest withContentType(MediaType mime, String charset) {
		return withContentType(mime, Charset.forName(charset));
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

	public HttpRequest withBodyGzipCompression() {
		super.setBodyGzipCompression();
		return this;
	}

	public HttpRequest withAcceptEncodingGzip() {
		setHeader(HttpHeaders.ofString(ACCEPT_ENCODING, "gzip"));
		return this;
	}
	// endregion

	// region setters
	public void setUrl(String url) throws IllegalArgumentException {
		assert !recycled;
		this.url = UrlParser.of(url);
		if (!this.url.isRelativePath()) {
			assert this.url.getHostAndPort() != null; // sadly no advanced contracts yet
			setHeader(HttpHeaders.ofString(HttpHeaders.HOST, this.url.getHostAndPort()));
		}
	}

	public void setRemoteAddress(InetAddress inetAddress) {
		assert !recycled;
		this.remoteAddress = inetAddress;
	}

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

	public void setAcceptEncodingGzip() {
		setHeader(HttpHeaders.ofString(ACCEPT_ENCODING, "gzip"));
	}
	// endregion

	// region getters
	public HttpMethod getMethod() {
		assert !recycled;
		return method;
	}

	public InetAddress getRemoteAddress() {
		assert !recycled;
		return remoteAddress;
	}

	@Nullable
	public String getFullUrl() {
		if (!url.isRelativePath()) {
			return url.toString();
		}
		String host = getHost();
		if (host == null) {
			return null;
		}
		return "http://" + host + url.getPathAndQuery();
	}

	public boolean isHttps() {
		return url.isHttps();
	}

	UrlParser getUrl() {
		assert !recycled;
		return url;
	}

	@Nullable
	public String getHostAndPort() {
		return url.getHostAndPort();
	}

	@Nullable
	public String getHost() {
		String host = getHeader(HttpHeaders.HOST);
		if ((host == null) || host.isEmpty())
			return null;
		return host;
	}

	public String getPath() {
		assert !recycled;
		return url.getPath();
	}

	public String getPathAndQuery() {
		assert !recycled;
		return url.getPathAndQuery();
	}

	public String getQuery() {
		return url.getQuery();
	}

	public String getFragment() {
		return url.getFragment();
	}

	@Nullable
	public String getQueryParameter(String key) {
		assert !recycled;
		return url.getQueryParameter(key);
	}

	public List<String> getQueryParameters(String key) {
		assert !recycled;
		return url.getQueryParameters(key);
	}

	public Iterable<QueryParameter> getQueryParametersIterable() {
		assert !recycled;
		return url.getQueryParametersIterable();
	}

	public Map<String, String> getQueryParameters() {
		assert !recycled;
		return url.getQueryParameters();
	}

	public String getPostParameter(String key) {
		assert !recycled;
		parseBodyParams();
		return bodyParameters.get(key);
	}

	public Map<String, String> getPostParameters() {
		assert !recycled;
		parseBodyParams();
		return bodyParameters;
	}

	@Nullable
	public String getPathParameter(String key) {
		assert !recycled;
		return pathParameters != null ? pathParameters.get(key) : null;
	}

	public Map<String, String> getPathParameters() {
		assert !recycled;
		return pathParameters != null ? pathParameters : Collections.emptyMap();
	}

	public List<AcceptMediaType> getAccept() {
		assert !recycled;
		List<AcceptMediaType> list = new ArrayList<>();
		List<Value> headers = getHeaderValues(ACCEPT);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			try {
				AcceptMediaType.parse(value.array, value.offset, value.size, list);
			} catch (ParseException e) {
				return Collections.emptyList();
			}
		}
		return list;
	}

	public List<AcceptCharset> getAcceptCharsets() {
		assert !recycled;
		List<AcceptCharset> charsets = new ArrayList<>();
		List<Value> headers = getHeaderValues(ACCEPT_CHARSET);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			try {
				AcceptCharset.parse(value.array, value.offset, value.size, charsets);
			} catch (ParseException e) {
				return Collections.emptyList();
			}
		}
		return charsets;
	}

	@Nullable
	public Date getIfModifiedSince() {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_MODIFIED_SINCE);
		if (header != null) {
			try {
				return new Date(HttpDate.parse(header.array, header.offset));
			} catch (ParseException e) {
				return null;
			}
		}
		return null;
	}

	@Nullable
	public Date getIfUnModifiedSince() {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_UNMODIFIED_SINCE);
		if (header != null)
			try {
				return new Date(HttpDate.parse(header.array, header.offset));
			} catch (ParseException e) {
				return null;
			}
		return null;
	}

	@Override
	public List<HttpCookie> getCookies() {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<Value> headers = getHeaderValues(COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			try {
				HttpCookie.parseSimple(value.array, value.offset, value.offset + value.size, cookie);
			} catch (ParseException e) {
				return new ArrayList<>();
			}
		}
		return cookie;
	}

	public boolean isAcceptEncodingGzip() {
		String acceptEncoding = this.getHeader(HttpHeaders.ACCEPT_ENCODING);
		return acceptEncoding != null && acceptEncoding.contains("gzip");
	}
	// endregion

	// region internal
	private void parseBodyParams() {
		if (bodyParameters != null)
			return;
		if (method == POST
				&& getContentType() != null
				&& getContentType().getMediaType() == MediaTypes.X_WWW_FORM_URLENCODED
				&& body.readPosition() != body.writePosition()) {
			bodyParameters = UrlParser.parseQueryIntoMap(decodeAscii(getBody()));
		} else {
			bodyParameters = Collections.emptyMap();
		}
	}

	int getPos() {
		return url.pos;
	}

	void setPos(int pos) {
		url.pos = (short) pos;
	}

	String getPartialPath() {
		assert !recycled;
		return url.getPartialPath();
	}

	String pollUrlPart() {
		assert !recycled;
		return url.pollUrlPart();
	}

	void removePathParameter(String key) {
		pathParameters.remove(key);
	}

	void putPathParameter(String key, String value) {
		if (pathParameters == null) {
			pathParameters = new HashMap<>();
		}
		pathParameters.put(key, value);
	}

	private final static int LONGEST_HTTP_METHOD_SIZE = 12;
	private static final byte[] HTTP_1_1 = encodeAscii(" HTTP/1.1");
	private static final int HTTP_1_1_SIZE = HTTP_1_1.length;
	private static final byte[] GZIP_BYTES = encodeAscii("gzip");

	@Override
	public ByteBuf toByteBuf() {
		assert !recycled;
		if (body != null || method != GET) {
			if (useGzip && body != null && body.readRemaining() > 0) {
				body = toGzip(body);
				setHeader(asBytes(CONTENT_ENCODING, GZIP_BYTES));
			}
			setHeader(HttpHeaders.ofDecimal(HttpHeaders.CONTENT_LENGTH, body == null ? 0 : body.readRemaining()));
		}
		int estimatedSize = estimateSize(LONGEST_HTTP_METHOD_SIZE
				+ 1 // SPACE
				+ url.getPathAndQueryLength())
				+ HTTP_1_1_SIZE;
		ByteBuf buf = ByteBufPool.allocate(estimatedSize);

		method.write(buf);
		buf.put(SP);
		url.writePathAndQuery(buf);
		buf.put(HTTP_1_1);

		writeHeaders(buf);

		writeBody(buf);

		return buf;
	}

	@Nullable
	@Override
	public String toString() {
		return getFullUrl();
	}
	// endregion
}
