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
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

public class HttpUrlTest {
	@Test
	public void testIPv6() {
		// with port
		HttpUrl url = HttpUrl.of("http://[0:0:0:0:0:0:0:1]:52142");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]:52142", url.getHostAndPort());
		assertEquals(52142, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		// without port
		url = HttpUrl.of("http://[0:0:0:0:0:0:0:1]");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHostAndPort());
		assertEquals(80, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		// with query
		url = HttpUrl.of("http://[0:0:0:0:0:0:0:1]:52142/path1/path2?aa=bb&zz=a+b");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]:52142", url.getHostAndPort());
		assertEquals(52142, url.getPort());
		assertEquals("/path1/path2?aa=bb&zz=a+b", url.getPathAndQuery());
		assertEquals("/path1/path2", url.getPath());
		assertEquals("aa=bb&zz=a+b", url.getQuery());

		url = HttpUrl.of("http://[0:0:0:0:0:0:0:1]/?");
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHost());
		assertEquals("[0:0:0:0:0:0:0:1]", url.getHostAndPort());
		assertEquals(80, url.getPort());
		assertEquals("/?", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testFullUrl() {
		HttpUrl url = HttpUrl.of("http://abc.com");
		assertFalse(url.isRelativePath());
		assertEquals("abc.com", url.getHostAndPort());
		assertEquals("abc.com", url.getHost());
		assertEquals(80, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		url = HttpUrl.of("http://zzz.abc.com:8080/path1/path2?aa=bb&zz=a+b");
		assertFalse(url.isRelativePath());
		assertEquals("zzz.abc.com:8080", url.getHostAndPort());
		assertEquals("zzz.abc.com", url.getHost());
		assertEquals(8080, url.getPort());
		assertEquals("/path1/path2?aa=bb&zz=a+b", url.getPathAndQuery());
		assertEquals("/path1/path2", url.getPath());
		assertEquals("aa=bb&zz=a+b", url.getQuery());

		url = HttpUrl.of("http://zzz.abc.com/?");
		assertFalse(url.isRelativePath());
		assertEquals("zzz.abc.com", url.getHostAndPort());
		assertEquals("zzz.abc.com", url.getHost());
		assertEquals(80, url.getPort());
		assertEquals("/?", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testPartialUrl() throws ParseException {
		HttpUrl url = HttpUrl.parse("/path1/path2?aa=bb&zz=a+b");
		assertTrue(url.isRelativePath());
		assertNull(url.getHostAndPort());
		assertNull(url.getHost());
		assertEquals(-1, url.getPort());
		assertEquals("/path1/path2?aa=bb&zz=a+b", url.getPathAndQuery());
		assertEquals(25, url.getPathAndQueryLength());
		assertEquals("/path1/path2", url.getPath());
		assertEquals("aa=bb&zz=a+b", url.getQuery());

		url = HttpUrl.parse("");
		assertTrue(url.isRelativePath());
		assertNull(url.getHostAndPort());
		assertNull(url.getHost());
		assertEquals(-1, url.getPort());
		assertEquals("/", url.getPathAndQuery());
		assertEquals("/", url.getPath());
		assertEquals("", url.getQuery());

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test(expected = ParseException.class)
	public void testInvalidScheme() throws ParseException {
		HttpUrl.parse("ftp://abc.com/");
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidPartialUrl() {
		HttpUrl.of("/path").isRelativePath();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test(expected = ParseException.class)
	public void testBadPort() throws ParseException {
		HttpUrl.parse("http://hello-world.com:80ab/path");
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testPollUrlPart() {
		HttpUrl uri = HttpUrl.of("http://example.com/a/b/c/index.html");
		assertEquals("a", uri.pollUrlPart());
		assertEquals("/b/c/index.html", uri.getRelativePath());
		assertEquals("b", uri.pollUrlPart());
		assertEquals("/c/index.html", uri.getRelativePath());
		assertEquals("c", uri.pollUrlPart());
		assertEquals("index.html", uri.pollUrlPart());
		assertEquals("", uri.pollUrlPart());
	}

	@Test
	public void testFragment() {
		HttpUrl url = HttpUrl.of("http://example.com/a/b/c/index.html?q=1&key=value#section-2.1");
		assertEquals("example.com", url.getHost());
		assertEquals("/a/b/c/index.html?q=1&key=value", url.getPathAndQuery());
		assertEquals("section-2.1", url.getFragment());
	}

	@Test(expected = NoSuchElementException.class)
	public void testQuery() {
//                                00000000001111111111222222222233333333334444444444555555555566
//  							  01234567890123456789012345678901234567890123456789012345678901
		HttpUrl url = HttpUrl.of("http://abc.com/?key=value&key&key=value2&key3=another_value&k");
		url.parseQueryParameters();

		assertArrayEquals(new int[]{1245209, 1900573, 2162728, 2949179, 3997757}, url.queryPositions);

		assertEquals("value", url.getQueryParameter("key"));
		assertEquals("another_value", url.getQueryParameter("key3"));
		assertEquals("", url.getQueryParameter("k"));
		assertEquals(null, url.getQueryParameter("missing"));

		List<String> parameters = url.getQueryParameters("key");
		assertEquals(Arrays.asList("value", "", "value2"), parameters);

		Iterable<QueryParameter> queryParameters = url.getQueryParametersIterable();
		assertNotNull(queryParameters);
		Iterator<QueryParameter> paramsIterator = queryParameters.iterator();
		assertEquals(new QueryParameter("key", "value"), paramsIterator.next());
		assertEquals(new QueryParameter("key", ""), paramsIterator.next());
		assertEquals(new QueryParameter("key", "value2"), paramsIterator.next());
		assertEquals(new QueryParameter("key3", "another_value"), paramsIterator.next());
		assertEquals(new QueryParameter("k", ""), paramsIterator.next());

		paramsIterator.next();
	}
}
