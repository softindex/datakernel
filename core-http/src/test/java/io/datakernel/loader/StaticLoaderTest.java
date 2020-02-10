package io.datakernel.loader;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Paths;

import static io.datakernel.http.loader.StaticLoader.NOT_FOUND_EXCEPTION;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class StaticLoaderTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testMap() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(newCachedThreadPool(), "/")
				.map(file -> file + ".txt");
		ByteBuf file = await(staticLoader.load("testFile"));
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFileNotFoundClassPath() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(newCachedThreadPool(), "/");
		StacklessException exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testFileNotFoundPath() {
		StaticLoader staticLoader = StaticLoader.ofPath(newCachedThreadPool(), Paths.get("/"));
		StacklessException exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testLoadClassPathFile() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(newCachedThreadPool(), "/");
		ByteBuf file = await(staticLoader.load("testFile.txt"));
		assertNotNull(file);
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFilterFileClassPath() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(newCachedThreadPool(), "/")
				.filter(file -> !file.equals("testFile.txt"));
		StacklessException exception = awaitException(staticLoader.load("testFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testClassPathWithDiffRoot() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(newCachedThreadPool(), "/");
		ByteBuf buf = await(staticLoader.load("/testFile.txt"));
		assertNotNull(buf);
		buf = await(staticLoader.load("/testFile.txt/"));
		assertNotNull(buf);
		buf = await(staticLoader.load("testFile.txt/"));
		assertNotNull(buf);
		buf = await(staticLoader.load("testFile.txt"));
		assertNotNull(buf);
	}

	@Test
	public void testFilterFilePath() {
		StaticLoader staticLoader = StaticLoader.ofPath(newCachedThreadPool(), Paths.get("/"))
				.filter(file -> !file.equals("testFile.txt"));
		StacklessException exception = awaitException(staticLoader.load("testFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testClassPathWithDir() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(newCachedThreadPool(), "dir");
		ByteBuf file = await(staticLoader.load("test.txt"));
		assertNotNull(file);
	}
}
