/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.http;

import io.datakernel.annotation.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class UrlBuilder {
	public static class Query {
		private final Map<String, String> query = new LinkedHashMap<>();

		// region creators
		private Query() {
		}

		public Query with(String key, Object value) {
			query.put(key, urlencode(value.toString()));
			return this;
		}

		public Query with(String key, Iterable<Object> values) {
			values.forEach(value -> query.put(key, urlencode(value.toString())));
			return this;
		}
		// endregion

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			query.forEach((k, v) -> sb.append(k).append('=').append(v).append('&'));
			sb.setLength(sb.length() - 1); // drop last &
			return sb.toString();
		}
	}

	private final String scheme;
	private final String path;
	@Nullable
	private String userInfo;
	@Nullable
	private String host;
	@Nullable
	private String port;
	@Nullable
	private String query;
	@Nullable
	private String fragment;

	// region creators
	private UrlBuilder(String scheme, String path) {
		this.scheme = scheme;
		this.path = urlencode(path);
	}

	public static UrlBuilder of(String scheme, String path) {
		return new UrlBuilder(scheme + ':', path);
	}

	public UrlBuilder withAuthority(String host) {
		this.host = host;
		return this;
	}

	public UrlBuilder withAuthority(String host, int port) {
		assert port >= 0 && port <= 49151; // exclude ephemeral ports (https://tools.ietf.org/html/rfc6335#section-6)
		this.port = Integer.toString(port);
		return withAuthority(host);
	}

	public UrlBuilder withAuthority(String userInfo, String host) {
		this.userInfo = userInfo;
		return withAuthority(host);
	}

	public UrlBuilder withAuthority(String userInfo, String host, int port) {
		this.userInfo = userInfo;
		return withAuthority(host, port);
	}

	public UrlBuilder withAuthority(InetSocketAddress address) {
		return withAuthority(address.isUnresolved() ? address.getHostName() : address.getAddress().getHostAddress(), address.getPort());
	}

	public UrlBuilder withAuthority(String userInfo, InetSocketAddress address) {
		String host;
		if (address.isUnresolved()) {
			host = address.getHostName();
		} else {
			InetAddress inetAddress = address.getAddress();
			host = inetAddress.getHostAddress();
			if (inetAddress instanceof Inet6Address) {
				host = '[' + host + ']';
			}
		}
		return withAuthority(userInfo, host, address.getPort());
	}

	public UrlBuilder withQuery(String query) {
		this.query = query;
		return this;
	}

	public UrlBuilder withQuery(Query query) {
		return withQuery(query.toString());
	}

	public UrlBuilder withFragment(String fragment) {
		this.fragment = fragment;
		return this;
	}
	// endregion

	public static Query query() {
		return new Query();
	}

	public static UrlBuilder http(String path) {
		return new UrlBuilder("http:", path);
	}

	public static UrlBuilder https(String path) {
		return new UrlBuilder("https:", path);
	}

	public static UrlBuilder relative(String path) {
		return new UrlBuilder("", path);
	}

	public static UrlBuilder relative() {
		return new UrlBuilder("", "");
	}

	private static String urlencode(String str) {
		try {
			return URLEncoder.encode(str, UTF_8.name());
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError("Apparently, UTF-8 no longer exists", e);
		}
	}

	public String build() {
		return toString();
	}

	@Override
	public String toString() {
		String s = scheme;
		if (host != null) {
			s += "//";
			if (userInfo != null) {
				s += userInfo + '@';
			}
			s += host;
			if (port != null) {
				s += ':' + port;
			}
			s += '/';
		}
		s += path;
		if (query != null) {
			s += '?' + query;
		}
		if (fragment != null) {
			s += '#' + fragment;
		}
		return s;
	}
}
