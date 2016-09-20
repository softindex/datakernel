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

import io.datakernel.exception.ParseException;

import java.util.Map;

import static java.lang.Integer.parseInt;

/**
 * Util for parsing a HTTP URI from a String
 */
public final class HttpUri {

	private static final String SCHEMA_DELIM = "://";
	private static final String HTTP = "http";
	private static final String HTTPS = "https";
	private static final char IPV6_OPENING_BRACKET = '[';
	private static final String IPV6_CLOSING_SECTION_WITH_PORT = "]:";

	/**
	 * Parses a string, treats all URIs starting with / as paths
	 *
	 * @param uri URI for parsing
	 * @return new URI
	 */
	public static HttpUri ofUrl(String uri) {
		return new HttpUri(uri, true);
	}

	/**
	 * Parses a string without prior scheme
	 *
	 * @param uri URI for parsing
	 * @return new URI
	 */
	static HttpUri parseUrl(String uri) throws ParseException {
		try {
			return new HttpUri(uri, false);
		} catch (RuntimeException e) {
			throw new ParseException(e);
		}
	}

	private final String schema;
	private final String uri;
	private final String hostPort;
	private final String host;
	private final int port;
	private final String pathAndQuery;
	private final String path;
	private final String query;
	private Map<String, String> params;

	private HttpUri(String uri, boolean strict) {
		this.uri = uri;
		int index = uri.indexOf(SCHEMA_DELIM);
		if (index < 0 || index > 5) {
			if (strict)
				throw new IllegalArgumentException("Partial URI is not allowed: " + uri);
			hostPort = null;
			host = null;
			port = -1;
			pathAndQuery = uri.isEmpty() ? "/" : uri;
			schema = "";
		} else {
			schema = uri.substring(0, index);
			if (!(schema.equals(HTTP) || schema.equals(HTTPS)))
				throw new IllegalArgumentException("Unsupported schema: " + schema);
			index += SCHEMA_DELIM.length();
			int slash = uri.indexOf('/', index);
			hostPort = (slash == -1) ? uri.substring(index) : uri.substring(index, slash);

			if (uri.charAt(index) == IPV6_OPENING_BRACKET) {
				// parse IPv6
				int closingSection = hostPort.indexOf(IPV6_CLOSING_SECTION_WITH_PORT);
				if (closingSection != -1) {
					host = hostPort.substring(0, closingSection + 1);
					port = parseInt(hostPort.substring(closingSection + 2));
				} else {
					host = hostPort;
					port = schema.equals(HTTPS) ? 443 : 80;
				}
			} else {
				// parse IPv4
				int colon = hostPort.indexOf(':');
				if (colon != -1) {
					host = hostPort.substring(0, colon);
					port = parseInt(hostPort.substring(colon + 1));
				} else {
					host = hostPort;
					port = schema.equals(HTTPS) ? 443 : 80;
				}
			}

			pathAndQuery = (slash == -1) ? "/" : uri.substring(slash);
		}
		index = pathAndQuery.indexOf('?');
		if (index < 0) {
			path = pathAndQuery;
			query = "";
		} else {
			path = pathAndQuery.substring(0, index);
			query = pathAndQuery.substring(index + 1);
		}
	}

	public boolean isPartial() {
		return host == null;
	}

	public String getSchema() {
		return schema;
	}

	public String getUri() {
		return uri;
	}

	public String getHostAndPort() {
		return hostPort;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getPathAndQuery() {
		return pathAndQuery;
	}

	public String getPath() {
		return path;
	}

	public String getQuery() {
		return query;
	}

	public String getParameter(String name) throws ParseException {
		parseParams();
		return params.get(name);
	}

	public Map<String, String> getParameters() throws ParseException {
		parseParams();
		return params;
	}

	private void parseParams() throws ParseException {
		if (params != null)
			return;
		params = HttpUtils.extractParameters(query);
	}

	@Override
	public String toString() {
		return uri;
	}

}
