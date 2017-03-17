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
import io.datakernel.exception.ParseException;

import java.net.URLDecoder;
import java.util.*;

import static java.util.Collections.emptyList;

public final class HttpUrl {
	private static final class KeyValueQueryIterable implements Iterable<QueryParameter> {
		private class QueryParamIterator implements Iterator<QueryParameter> {
			private int keyStart;
			private int currentRecord = 0;

			public QueryParamIterator(int keyStart) {
				this.keyStart = keyStart;
			}

			@Override
			public boolean hasNext() {
				return currentRecord < positions.length;
			}

			@Override
			public QueryParameter next() {
				try {
					return doNext();
				} catch (IndexOutOfBoundsException e) {
					throw new NoSuchElementException();
				}
			}

			private QueryParameter doNext() {
				int record = positions[currentRecord++];
				int keyEnd = record >> 16;
				int valueEnd = record & 0xFFFF;
				String key = src.substring(keyStart, keyEnd);
				String value = valueEnd == keyEnd ? "" : decodeQuietly(src.substring(keyEnd + 1, valueEnd), enc);
				keyStart = valueEnd + 1;
				return new QueryParameter(key, value);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		}

		private final String src;
		private final int[] positions;
		private final String enc;
		private final short keyStart;

		KeyValueQueryIterable(String src, int[] positions, short keyStart, String enc) {
			this.src = src;
			this.positions = positions;
			this.enc = enc;
			this.keyStart = keyStart;
		}

		@Override
		public Iterator<QueryParameter> iterator() {
			return new QueryParamIterator(keyStart);
		}
	}

	private static final char IPV6_OPENING_BRACKET = '[';
	private static final String IPV6_CLOSING_SECTION_WITH_PORT = "]:";
	private static final String SCHEMA_DELIM = "://";
	private static final char SLASH = '/';
	private static final char COLON = ':';
	private static final char QUESTION_MARK = '?';
	private static final char NUMBER_SIGN = '#';
	private static final char QUERY_DELIMITER = '&';
	private static final char QUERY_SEPARATOR = '=';
	private static final String QUERY_ENCODING = "UTF-8";

	private static final String HTTP = "http";
	private static final String HTTPS = "https";

	private final String raw;

	private int portValue = -1;
	private boolean https = false;

	private short host = -1;
	private short path = -1;
	private short port = -1;
	private short pathEnd = -1;
	private short query = -1;
	private short fragment = -1;
	short pos;

	int[] queryPositions;

	// creators
	public static HttpUrl of(String url) throws IllegalArgumentException {
		HttpUrl httpUrl = new HttpUrl(url);
		httpUrl.parseQuietly(false);
		return httpUrl;
	}

	static HttpUrl parse(String url) throws ParseException {
		HttpUrl httpUrl = new HttpUrl(url);
		httpUrl.parse(true);
		return httpUrl;
	}

	private HttpUrl(String raw) {
		this.raw = raw;
	}

	private void parseQuietly(boolean isRelativePathAllowed) {
		try {
			parse(isRelativePathAllowed);
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private void parse(boolean isRelativePathAllowed) throws ParseException {
		short index = (short) raw.indexOf(SCHEMA_DELIM);
		if (index < 0 || index > 5) {
			if (!isRelativePathAllowed)
				throw new ParseException("Partial URI is not allowed: " + raw);
			index = 0;
		} else {
			if (index == 5 && raw.startsWith(HTTPS)) {
				https = true;
			} else if (index == 4 && raw.startsWith(HTTP)) {
				https = false;
			} else {
				throw new ParseException("Unsupported schema: " + raw.substring(0, index));
			}
			index += SCHEMA_DELIM.length();
			host = index;
			if (raw.indexOf(IPV6_OPENING_BRACKET, index) != -1) {                   // parse IPv6
				int closingSection = raw.indexOf(IPV6_CLOSING_SECTION_WITH_PORT, index);
				port = (short) (closingSection != -1 ? closingSection + 2 : closingSection);
			} else {                                                                // parse IPv4
				int colon = raw.indexOf(COLON, index);
				port = (short) (colon != -1 ? colon + 1 : -1);
			}
		}
		path = (short) raw.indexOf(SLASH, index);
		pos = path;

		query = (short) raw.indexOf(QUESTION_MARK);
		if (query != -1) {
			pathEnd = query;
			query += 1;
		} else {
			pathEnd = (short) raw.length();
		}

		fragment = (short) raw.indexOf(NUMBER_SIGN, index);
		fragment = fragment == -1 ? fragment : (short) (fragment + 1);

		if (port != -1) {
			int portEnd = path == -1 ? query == -1 ? fragment == -1 ? raw.length() : fragment : query : path;
			portValue = toInt(raw, port, portEnd);
		} else {
			if (host != -1) {
				portValue = (https ? 443 : 80);
			}
		}
	}

	// getters
	boolean isRelativePath() {
		return host == -1;
	}

	public boolean isHttps() {
		return https;
	}

	public String getHostAndPort() {
		if (host == -1) {
			return null;
		} else {
			int end = path != -1 ? path : query != -1 ? query - 1 : raw.length();
			return raw.substring(host, end);
		}
	}

	public String getHost() {
		if (host == -1) {
			return null;
		} else {
			int end = port != -1 ? port - 1 : path != -1 ? path : query != -1 ? query - 1 : raw.length();
			return raw.substring(host, end);
		}
	}

	public int getPort() {
		return portValue;
	}

	public String getPathAndQuery() {
		if (path == -1) {
			return "/";
		} else {
			int queryEnd = fragment == -1 ? raw.length() : fragment - 1;
			return raw.substring(path, queryEnd);
		}
	}

	public String getPath() {
		if (path == -1) {
			return "/";
		} else {
			return raw.substring(path, pathEnd);
		}
	}

	public String getQuery() {
		if (query == -1) {
			return "";
		} else {
			int queryEnd = fragment == -1 ? raw.length() : fragment;
			return raw.substring(query, queryEnd);
		}
	}

	public String getFragment() {
		if (fragment == -1) {
			return "";
		} else {
			return raw.substring(fragment, raw.length());
		}
	}

	int getPathAndQueryLength() {
		if (path == -1) {
			return 1;
		} else {
			int queryEnd = fragment == -1 ? raw.length() : fragment;
			return queryEnd - path;
		}
	}

	void writePathAndQuery(ByteBuf buf) {
		if (path == -1) {
			buf.put((byte) '/');
		} else {
			int queryEnd = fragment == -1 ? raw.length() : fragment - 1;
			for (int i = path; i < queryEnd; i++) {
				buf.put((byte) raw.charAt(i));
			}
		}
	}

	// work with parameters
	public String getQueryParameter(String key) {
		if (query == -1) {
			return null;
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return findParameter(raw, queryPositions, query, key);
	}

	public List<String> getQueryParameters(String key) {
		if (query == -1) {
			return emptyList();
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return findParameters(raw, queryPositions, query, key);
	}

	public Iterable<QueryParameter> getQueryParametersIterable() {
		if (query == -1) {
			return emptyList();
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return findParameters(raw, queryPositions, query);
	}

	public Map<String, String> getQueryParameters() {
		HashMap<String, String> map = new HashMap<>();
		for (QueryParameter queryParameter : getQueryParametersIterable()) {
			map.put(queryParameter.getKey(), queryParameter.getValue());
		}
		return map;
	}

	void parseQueryParameters() {
		int queryEnd = fragment == -1 ? raw.length() : fragment;
		queryPositions = doParseParameters(raw, query, queryEnd);
	}

	static int[] doParseParameters(String src, int start, int end) {
		assert src.length() >= end;
		assert start != -1;
		assert src.length() <= 0xFFFF;

		int n = 1; // if not empty -> at least 1 parameter exist

		// looking for parameters quantity
		for (int i = start; i < end; i++) {
			if (src.charAt(i) == QUERY_DELIMITER) n++;
		}
		int[] positions = new int[n];

		// adding parameters
		int ke = -1;    // key end
		int ve;         // val end
		int k = 0;      // parameter #
		int record;     // temporary variable
		for (int i = start; i < end; i++) {
			char c = src.charAt(i);
			if (c == QUERY_SEPARATOR) {
				ke = i;
			}
			if (c == QUERY_DELIMITER || i + 1 == end) {
				if (i + 1 == end) {
					i++;
				}
				if (ke == -1) {
					ke = i;
				}
				ve = i;
				record = (ke << 16) | (ve & 0xFFFF);
				positions[k++] = record;
				ke = -1;
			}
		}
		return positions;
	}

	static String findParameter(String src, int[] parsedPositions, int startPosition, String key) {
		int keyStart = startPosition;
		for (int record : parsedPositions) {
			int keyEnd = record >> 16;
			int valueEnd = record & 0xFFFF;
			if (isEqual(key, src, keyStart, keyEnd)) {
				if (valueEnd == keyEnd) {
					return "";
				}
				return decodeQuietly(src.substring(keyEnd + 1, valueEnd), QUERY_ENCODING);
			}
			keyStart = valueEnd + 1;
		}
		return null;
	}

	static List<String> findParameters(String src, int[] parsedPositions, int startPosition, String key) {
		int keyStart = startPosition;
		List<String> container = null;
		for (int record : parsedPositions) {
			int keyEnd = record >> 16;
			int valueEnd = record & 0xFFFF;

			if (isEqual(key, src, keyStart, keyEnd)) {
				if (container == null) {
					container = new ArrayList<>();
				}
				if (valueEnd == keyEnd) {
					container.add("");
				} else {
					container.add(decodeQuietly(src.substring(keyEnd + 1, valueEnd), QUERY_ENCODING));
				}
			}
			keyStart = valueEnd + 1;
		}
		return container;
	}

	static Iterable<QueryParameter> findParameters(String src, int[] parsedPositions, short startPosition) {
		return new KeyValueQueryIterable(src, parsedPositions, startPosition, QUERY_ENCODING);
	}

	// work with path
	public String getRelativePath() {
		if (pos == -1) {
			return "";
		}
		return raw.substring(pos, pathEnd);
	}

	String pollUrlPart() {
		if (pos < pathEnd) {
			int start = pos + 1;
			pos = (short) raw.indexOf('/', start);
			String part;
			if (pos == -1) {
				part = raw.substring(start, pathEnd);
				pos = (short) raw.length();
			} else {
				part = raw.substring(start, pos);
			}
			return part;
		} else {
			return "";
		}
	}

	// utils
	private static String decodeQuietly(String string, String enc) {
		try {
			return URLDecoder.decode(string, enc);
		} catch (Exception e) {
			return null;
		}
	}

	private static boolean isEqual(String key, String raw, int start, int end) {
		if (end - start != key.length()) {
			return false;
		}
		for (int i = 0; i < key.length(); i++) {
			if (key.charAt(i) != raw.charAt(start + i))
				return false;
		}
		return true;
	}

	private static int toInt(String str, int pos, int end) throws ParseException {
		if (pos == end) {
			throw new ParseException("Empty port value");
		}
		if ((end - pos) > 5) {
			throw new ParseException("Bad port: " + str.substring(pos, end));
		}

		int result = 0;
		for (int i = pos; i < end; i++) {
			int c = (str.charAt(i) - '0');
			if (c < 0 || c > 9)
				throw new ParseException("Bad port: " + str.substring(pos, end));
			result = c + result * 10;
		}

		if (result > 0xFFFF) {
			throw new ParseException("Bad port: " + str.substring(pos, end));
		}

		return result;
	}

	@Override
	public String toString() {
		return raw;
	}
}
