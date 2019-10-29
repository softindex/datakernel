/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.common.Initializable;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.http.HttpHeaderValue.HttpHeaderValueOfSimpleCookies;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.common.Utils.nullToEmpty;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpMethod.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

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
	private final static int LONGEST_HTTP_METHOD_SIZE = 12;
	private static final byte[] HTTP_1_1 = encodeAscii(" HTTP/1.1");
	private static final int HTTP_1_1_SIZE = HTTP_1_1.length;
	@NotNull
	private final HttpMethod method;
	private UrlParser url;
	private InetAddress remoteAddress;
	private Map<String, String> pathParameters;
	private Map<String, String> queryParameters;
	private Map<String, String> postParameters;

	// region creators
	HttpRequest(@NotNull HttpMethod method, @Nullable UrlParser url) {
		this.method = method;
		this.url = url;
	}

	@NotNull
	public static HttpRequest of(@NotNull HttpMethod method, @NotNull String url) {
		HttpRequest request = new HttpRequest(method, null);
		request.setUrl(url);
		return request;
	}

	@NotNull
	public static HttpRequest get(@NotNull String url) {
		return HttpRequest.of(GET, url);
	}

	@NotNull
	public static HttpRequest post(@NotNull String url) {
		return HttpRequest.of(POST, url);
	}

	@NotNull
	public static HttpRequest put(@NotNull String url) {
		return HttpRequest.of(PUT, url);
	}

	@NotNull
	public HttpRequest withHeader(@NotNull HttpHeader header, @NotNull String value) {
		addHeader(header, value);
		return this;
	}

	@NotNull
	public HttpRequest withHeader(@NotNull HttpHeader header, @NotNull byte[] value) {
		addHeader(header, value);
		return this;
	}

	@NotNull
	public HttpRequest withHeader(@NotNull HttpHeader header, @NotNull HttpHeaderValue value) {
		addHeader(header, value);
		return this;
	}

	@NotNull
	public HttpRequest withBody(@NotNull byte[] array) {
		setBody(array);
		return this;
	}

	@NotNull
	public HttpRequest withBody(@NotNull ByteBuf body) {
		setBody(body);
		return this;
	}

	@NotNull
	public HttpRequest withBodyStream(@NotNull ChannelSupplier<ByteBuf> stream) {
		setBodyStream(stream);
		return this;
	}

	@Override
	public void addCookies(@NotNull List<HttpCookie> cookies) {
		assert !isRecycled();
		headers.add(COOKIE, new HttpHeaderValueOfSimpleCookies(cookies));
	}

	@Override
	public void addCookie(@NotNull HttpCookie cookie) {
		addCookies(singletonList(cookie));
	}

	@NotNull
	public HttpRequest withCookies(@NotNull List<HttpCookie> cookies) {
		addCookies(cookies);
		return this;
	}

	@NotNull
	public HttpRequest withCookies(@NotNull HttpCookie... cookie) {
		addCookies(cookie);
		return this;
	}

	@NotNull
	public HttpRequest withCookie(@NotNull HttpCookie cookie) {
		addCookie(cookie);
		return this;
	}

	@NotNull
	public HttpRequest withBodyGzipCompression() {
		setBodyGzipCompression();
		return this;
	}
	// endregion

	@NotNull
	@Contract(pure = true)
	public HttpMethod getMethod() {
		assert !isRecycled();
		return method;
	}

	@Contract(pure = true)
	public InetAddress getRemoteAddress() {
		assert !isRecycled();
		return remoteAddress;
	}

	void setRemoteAddress(@NotNull InetAddress inetAddress) {
		remoteAddress = inetAddress;
	}

	public boolean isHttps() {
		return url.isHttps();
	}

	UrlParser getUrl() {
		assert !isRecycled();
		return url;
	}

	void setUrl(@NotNull String url) {
		assert !isRecycled();
		this.url = UrlParser.of(url);
		if (!this.url.isRelativePath()) {
			assert this.url.getHostAndPort() != null; // sadly no advanced contracts yet
			addHeader(HOST, this.url.getHostAndPort());
		}
	}

	@Nullable
	public String getHostAndPort() {
		return url.getHostAndPort();
	}

	@NotNull
	public String getPath() {
		assert !isRecycled();
		return url.getPath();
	}

	@NotNull
	public String getPathAndQuery() {
		assert !isRecycled();
		return url.getPathAndQuery();
	}

	@Nullable
	private Map<String, String> parsedCookies;

	@NotNull
	public Map<String, String> getCookies() {
		if (parsedCookies != null) {
			return parsedCookies;
		}
		Map<String, String> cookies = new LinkedHashMap<>();
		for (HttpCookie cookie : getHeader(COOKIE, HttpHeaderValue::toSimpleCookies)) {
			cookies.put(cookie.getName(), cookie.getValue());
		}
		return parsedCookies = cookies;
	}

	@Nullable
	public String getCookie(@NotNull String cookie) {
		return getCookies().get(cookie);
	}

	@NotNull
	public String getQuery() {
		return url.getQuery();
	}

	@NotNull
	public String getFragment() {
		return url.getFragment();
	}

	@NotNull
	public Map<String, String> getQueryParameters() {
		assert !isRecycled();
		if (queryParameters != null) {
			return queryParameters;
		}
		queryParameters = url.getQueryParameters();
		return queryParameters;
	}

	@Nullable
	public String getQueryParameter(@NotNull String key) {
		assert !isRecycled();
		return url.getQueryParameter(key);
	}

	@NotNull
	public List<String> getQueryParameters(@NotNull String key) {
		assert !isRecycled();
		return url.getQueryParameters(key);
	}

	@NotNull
	public Iterable<QueryParameter> getQueryParametersIterable() {
		assert !isRecycled();
		return url.getQueryParametersIterable();
	}

	@Nullable
	public String getPostParameter(String name) {
		return getPostParameters().get(name);
	}

	@NotNull
	public Map<String, String> getPostParameters() {
		if (postParameters != null) return postParameters;
		if (body == null) throw new NullPointerException("Body must be loaded to parse post parameters");
		return postParameters =
				containsPostParameters() ?
						UrlParser.parseQueryIntoMap(decodeAscii(body.array(), body.head(), body.readRemaining())) :
						emptyMap();
	}

	private boolean containsPostParameters() {
		ByteBuf buf = getHeaderBuf(CONTENT_TYPE);
		if (buf == null) {
			return false;
		}
		try {
			ContentType contentType = HttpHeaderValue.toContentType(buf);
			if (method != POST || contentType.getMediaType() != MediaTypes.X_WWW_FORM_URLENCODED) {
				return false;
			}
		} catch (ParseException e) {
			return false;
		}
		return true;
	}

	@NotNull
	public Map<String, String> getPathParameters() {
		assert !isRecycled();
		return pathParameters != null ? pathParameters : emptyMap();
	}

	@NotNull
	public String getPathParameter(@NotNull String key) {
		assert !isRecycled();
		if (pathParameters != null) {
			String pathParameter = pathParameters.get(key);
			if (pathParameter != null) {
				return pathParameter;
			}
		}
		throw new IllegalArgumentException("No path parameter '" + key + "' found");
	}

	public Promise<Void> handleMultipart(MultipartDataHandler multipartDataHandler) {
		String contentType = getHeader(CONTENT_TYPE);
		if (contentType == null || !contentType.startsWith("multipart/form-data; boundary=")) {
			return Promise.ofException(HttpException.ofCode(400, "Content type is not multipart/form-data"));
		}
		String boundary = contentType.substring(30);
		if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
			boundary = boundary.substring(1, boundary.length() - 1);
		}
		return MultipartParser.create(boundary)
				.split(getBodyStream(), multipartDataHandler);
	}

	int getPos() {
		return url.pos;
	}

	void setPos(int pos) {
		url.pos = (short) pos;
	}

	@NotNull
	public String getRelativePath() {
		assert !isRecycled();
		String partialPath = url.getPartialPath();
		return partialPath.startsWith("/") ? partialPath.substring(1) : partialPath; // strip first '/'
	}

	String pollUrlPart() {
		assert !isRecycled();
		return url.pollUrlPart();
	}

	void removePathParameter(String key) {
		pathParameters.remove(key);
	}

	void putPathParameter(String key, @NotNull String value) {
		if (pathParameters == null) {
			pathParameters = new HashMap<>();
		}
		pathParameters.put(key, UrlParser.urlDecode(value));
	}

	@Override
	protected int estimateSize() {
		return estimateSize(LONGEST_HTTP_METHOD_SIZE
				+ 1 // SPACE
				+ url.getPathAndQueryLength())
				+ HTTP_1_1_SIZE;
	}

	@Override
	protected void writeTo(@NotNull ByteBuf buf) {
		method.write(buf);
		buf.put(SP);
		url.writePathAndQuery(buf);
		buf.put(HTTP_1_1);
		writeHeaders(buf);
	}

	@Override
	public String toString() {
		if (url.isRelativePath()) {
			String host = getHeader(HOST);
			return nullToEmpty(host) + url.getPathAndQuery();
		}
		return url.toString();
	}
}
