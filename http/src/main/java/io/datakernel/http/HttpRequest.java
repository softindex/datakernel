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
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.util.Initializable;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.*;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * Represents the HTTP request which {@link AsyncHttpClient} sends to
 * {@link AsyncHttpServer}. It must have only one owner in each  part of time.
 * After creating an {@link HttpResponse} in a server, it will be recycled and
 * can not be used later.
 * <p>
 * {@code HttpRequest} class provides methods which can be used intuitively for
 * creating and configuring an HTTP request.
 */
public final class HttpRequest extends HttpMessage implements Initializable<HttpRequest> {
	private final HttpMethod method;
	private UrlParser url;
	private InetAddress remoteAddress;

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
		assert !isRecycled();
		this.url = UrlParser.of(url);
		if (!this.url.isRelativePath()) {
			assert this.url.getHostAndPort() != null; // sadly no advanced contracts yet
			setHeader(HttpHeaders.ofString(HttpHeaders.HOST, this.url.getHostAndPort()));
		}
	}

	public void setRemoteAddress(InetAddress inetAddress) {
		assert !isRecycled();
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
		assert !isRecycled();
		return method;
	}

	public InetAddress getRemoteAddress() {
		assert !isRecycled();
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
		assert !isRecycled();
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
		assert !isRecycled();
		return url.getPath();
	}

	public String getPathAndQuery() {
		assert !isRecycled();
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
		assert !isRecycled();
		return url.getQueryParameter(key);
	}

	public List<String> getQueryParameters(String key) {
		assert !isRecycled();
		return url.getQueryParameters(key);
	}

	public Iterable<QueryParameter> getQueryParametersIterable() {
		assert !isRecycled();
		return url.getQueryParametersIterable();
	}

	public Map<String, String> getQueryParameters() {
		assert !isRecycled();
		return url.getQueryParameters();
	}

	public Map<String, String> getPostParameters() {
		assert !isRecycled();
		checkNotNull(body);
		if (method == POST
				&& getContentType() != null
				&& getContentType().getMediaType() == MediaTypes.X_WWW_FORM_URLENCODED
				&& this.body.readPosition() != this.body.writePosition()) {
			return UrlParser.parseQueryIntoMap(decodeAscii(body.array(), body.readPosition(), body.readRemaining()));
		} else {
			return Collections.emptyMap();
		}
	}

	@Nullable
	public String getPathParameter(String key) {
		assert !isRecycled();
		return pathParameters != null ? pathParameters.get(key) : null;
	}

	public Map<String, String> getPathParameters() {
		assert !isRecycled();
		return pathParameters != null ? pathParameters : Collections.emptyMap();
	}

	public List<AcceptMediaType> getAccept() {
		assert !isRecycled();
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
		assert !isRecycled();
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
		assert !isRecycled();
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
		assert !isRecycled();
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
		assert !isRecycled();
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

	int getPos() {
		return url.pos;
	}

	void setPos(int pos) {
		url.pos = (short) pos;
	}

	String getPartialPath() {
		assert !isRecycled();
		return url.getPartialPath();
	}

	String pollUrlPart() {
		assert !isRecycled();
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

	@SuppressWarnings("unchecked")
	Stage<HttpRequest> ensureBody() {
		return (Stage<HttpRequest>) doEnsureBody();
	}

	@Override
	protected int estimateSize() {
		return estimateSize(LONGEST_HTTP_METHOD_SIZE
				+ 1 // SPACE
				+ url.getPathAndQueryLength())
				+ HTTP_1_1_SIZE;
	}

	@Override
	protected void writeTo(ByteBuf buf) {
		method.write(buf);
		buf.put(SP);
		url.writePathAndQuery(buf);
		buf.put(HTTP_1_1);
		writeHeaders(buf);
	}

	@Nullable
	@Override
	public String toString() {
		return getFullUrl();
	}
	// endregion
}
