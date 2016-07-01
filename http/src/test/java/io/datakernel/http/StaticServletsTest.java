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

import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.async.AsyncCallbacks.ignoreResultCallback;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class StaticServletsTest {
	public static final String EXPECTED_CONTENT = "This is a test string";

	@ClassRule
	public static final TemporaryFolder tmpFolder = new TemporaryFolder();
	private static Path resources;

	@BeforeClass
	public static void setup() throws IOException {
		Path root = tmpFolder.getRoot().toPath();
		resources = tmpFolder.newFolder("static").toPath();

		// creating `secure` file
		Files.write(root.resolve("cant_touch.txt"), encodeAscii("The content of this file should not be seen!"));

		// creating several common files
		Files.write(resources.resolve("index.html"), encodeAscii(EXPECTED_CONTENT));
		Files.write(resources.resolve("test.txt"), encodeAscii(EXPECTED_CONTENT));
		Files.write(resources.resolve("pom.xml"), encodeAscii(EXPECTED_CONTENT));
	}

	@Test
	public void testStaticServletForFiles() throws InterruptedException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		final List<String> res = new ArrayList<>();

		StaticServletForFiles servlet = StaticServletForFiles.create(eventloop, executor, resources);
		servlet.doServeAsync("index.html", new ForwardingResultCallback<ByteBuf>(ignoreResultCallback()) {
			@Override
			public void onResult(ByteBuf result) {
				res.add(decodeAscii(result));
				result.recycle();
			}
		});
		eventloop.run();
		executor.shutdown();
		assertEquals(1, res.size());
		assertEquals(EXPECTED_CONTENT, res.get(0));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testStaticServletForFilesAccessToRestrictedFile() {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		final List<Exception> res = new ArrayList<>();

		StaticServletForFiles servlet = StaticServletForFiles.create(eventloop, executor, resources);
		servlet.serveAsync(HttpRequest.get("http://127.0.0.1/../cant_touch.txt"), new AsyncHttpServlet.Callback() {
			@Override
			public void onResult(HttpResponse result) {
			}

			@Override
			public void onHttpError(HttpServletError httpServletError) {
				res.add(httpServletError);
			}
		});
		eventloop.run();
		executor.shutdown();
		assertEquals(1, res.size());
		assertEquals(404, ((HttpServletError) res.get(0)).getCode());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testStaticServletForResourcesAccessToRestrictedFile() {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		final List<Exception> res = new ArrayList<>();

		StaticServletForResources servlet = StaticServletForResources.create(eventloop, executor, "./");
		servlet.serveAsync(HttpRequest.get("http://127.0.0.1/../cant_touch.txt"), new AsyncHttpServlet.Callback() {
			@Override
			public void onResult(HttpResponse result) {
			}

			@Override
			public void onHttpError(HttpServletError httpServletError) {
				res.add(httpServletError);
			}
		});
		eventloop.run();
		executor.shutdown();
		assertEquals(1, res.size());
		assertEquals(404, ((HttpServletError) res.get(0)).getCode());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testStaticServletForFilesFileNotFound() {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		final List<Exception> res = new ArrayList<>();

		StaticServletForFiles servlet = StaticServletForFiles.create(eventloop, executor, resources);
		servlet.serveAsync(HttpRequest.get("http://127.0.0.1/file/not/found.txt"), new AsyncHttpServlet.Callback() {
			@Override
			public void onResult(HttpResponse result) {
				// empty
			}

			@Override
			public void onHttpError(HttpServletError httpServletError) {
				res.add(httpServletError);
			}
		});
		eventloop.run();
		executor.shutdown();

		assertEquals(1, res.size());
		assertEquals(404, ((HttpServletError) res.get(0)).getCode());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testStaticServletForResourcesFileNotFound() {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		final List<Exception> res = new ArrayList<>();

		StaticServletForResources servlet = StaticServletForResources.create(eventloop, executor, "./");
		servlet.serveAsync(HttpRequest.get("http://127.0.0.1/file/not/found.txt"), new AsyncHttpServlet.Callback() {
			@Override
			public void onResult(HttpResponse result) {
				// empty
			}

			@Override
			public void onHttpError(HttpServletError httpServletError) {
				res.add(httpServletError);
			}
		});
		eventloop.run();
		executor.shutdown();
		assertEquals(1, res.size());
		assertEquals(404, ((HttpServletError) res.get(0)).getCode());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
