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
import io.datakernel.loader.StaticLoader;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.loader.StaticLoader.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class StaticServletsTest {
	public static final String EXPECTED_CONTENT = "Test";

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder tmpFolder = new TemporaryFolder();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	private static Path resourcesPath;
	private static File resourcesFile;

	@BeforeClass
	public static void setup() throws IOException {
		resourcesPath = tmpFolder.newFolder("static").toPath();
		resourcesFile = resourcesPath.toFile();

		Files.write(resourcesPath.resolve("index.html"), encodeAscii(EXPECTED_CONTENT));
	}

	@Test
	public void testPathLoader() {
		StaticServlet staticServlet = StaticServlet.create(ofPath(resourcesPath));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html")));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundPathLoader() {
		StaticServlet staticServlet = StaticServlet.create(ofPath(resourcesPath));
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/unknownFile.txt")));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testFileLoader() {
		StaticServlet staticServlet = StaticServlet.create(ofFile(resourcesFile));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html")));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundCachedFileLoader() {
		StaticServlet staticServlet = StaticServlet.create(ofFile(resourcesFile));
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt")));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testClassPath() {
		StaticServlet staticServlet = StaticServlet.create(ofClassPath("/"));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt")));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundClassPath() {
		StaticServlet staticServlet = StaticServlet.create(ofClassPath( "/"));
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html")));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testRelativeClassPath() {
		StaticServlet staticServlet = StaticServlet.create(ofClassPath(null, getClass().getClassLoader(), "/"));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt")));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testRelativeClassPathWithInnerPath() {
		StaticLoader resourceLoader = ofClassPath(null, getClass().getClassLoader(), "/dir/");
		StaticServlet staticServlet = StaticServlet.create(resourceLoader);
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/test.txt")));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundRelativeClassPath() {
		StaticLoader resourceLoader = ofClassPath(null, getClass().getClassLoader(), "/");
		StaticServlet staticServlet = StaticServlet.create(resourceLoader);
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/unknownFile.txt")));

		assertEquals(404, e.getCode());
	}
}
