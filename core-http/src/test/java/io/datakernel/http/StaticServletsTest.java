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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.loader.FileNamesLoadingService;
import io.datakernel.loader.ResourcesNameLoadingService;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public final class StaticServletsTest {
	public static final String EXPECTED_CONTENT = "Test";

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@ClassRule
	public static final TemporaryFolder tmpFolder = new TemporaryFolder();

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
		StaticLoader resourceLoader = StaticLoaders.ofPath(Executors.newSingleThreadExecutor(), resourcesPath);
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html")));
		ByteBuf body = await(response.getBody());

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundPathLoader() {
		StaticLoader resourceLoader = StaticLoaders.ofPath(Executors.newSingleThreadExecutor(), resourcesPath);
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/unknownFile.txt")));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testFileLoader() {
		StaticLoader resourceLoader = StaticLoaders.ofFile(Executors.newSingleThreadExecutor(), resourcesFile);
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html")));
		ByteBuf body = await(response.getBody());

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundCachedFileLoader() {
		StaticLoader resourceLoader = StaticLoaders.ofFile(Executors.newSingleThreadExecutor(), resourcesFile);
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt")));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testClassPath() {
		StaticLoader resourceLoader = StaticLoaders.ofClassPath(Executors.newSingleThreadExecutor());
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt")));
		ByteBuf body = await(response.getBody());

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundClassPath() {
		StaticLoader resourceLoader = StaticLoaders.ofClassPath(Executors.newSingleThreadExecutor());
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html")));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testRelativeClassPath() {
		StaticLoader resourceLoader = StaticLoaders.ofClassPath(Executors.newSingleThreadExecutor(), getClass());
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt")));
		ByteBuf body = await(response.getBody());

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundRelativeClassPath() {
		StaticLoader resourceLoader = StaticLoaders.ofClassPath(Executors.newSingleThreadExecutor(), StaticServlet.class);
		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), resourceLoader);
		HttpException e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/unknownFile.txt")));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testResourcesNameLoadingService() {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		ResourcesNameLoadingService preDownloadResources = ResourcesNameLoadingService.create(Eventloop.getCurrentEventloop(),
				Executors.newSingleThreadExecutor(), classLoader, "dir2");
		preDownloadResources.start();

		Eventloop.getCurrentEventloop().run();

		StaticLoader testLoader = name -> name.equals("dir2/testFile.txt") ?
				Promise.of(ByteBufStrings.wrapAscii(EXPECTED_CONTENT)) :
				Promise.ofException(new NoSuchFileException(name));

		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), testLoader.filter(preDownloadResources::contains));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/dir2/testFile.txt")));
		ByteBuf body = await(response.getBody());

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNamesLoadingService() {
		Executor executor = Executors.newSingleThreadExecutor();

		FileNamesLoadingService fileService = FileNamesLoadingService.create(Eventloop.getCurrentEventloop(), executor, resourcesPath);
		fileService.start();

		Eventloop.getCurrentEventloop().run();

		StaticLoader testLoader = name -> name.equals("index.html") ?
				Promise.of(ByteBufStrings.wrapAscii(EXPECTED_CONTENT)) :
				Promise.ofException(new NoSuchFileException(name));

		StaticServlet staticServlet = StaticServlet.create(Eventloop.getCurrentEventloop(), testLoader.filter(fileService::contains));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html")));
		ByteBuf body = await(response.getBody());

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}
}
