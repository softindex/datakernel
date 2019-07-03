package io.datakernel.loader;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.StacklessException;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.ExecutorRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Paths;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.loader.StaticLoader.NOT_FOUND_EXCEPTION;
import static io.datakernel.test.rules.ExecutorRule.getExecutor;
import static org.junit.Assert.*;

public class StaticLoaderTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final ExecutorRule executorRule = new ExecutorRule();

	@Test
	public void testMap() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(getExecutor(), "/")
				.map(file -> file + ".txt");
		ByteBuf file = await(staticLoader.load("testFile"));
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFileNotFoundClassPath() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(getExecutor(), "/");
		StacklessException exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testFileNotFoundPath() {
		StaticLoader staticLoader = StaticLoader.ofPath(getExecutor(), Paths.get("/"));
		StacklessException exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testLoadClassPathFile() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(getExecutor(), "/");
		ByteBuf file = await(staticLoader.load("testFile.txt"));
		assertNotNull(file);
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFilterFileClassPath() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(getExecutor(), "/")
				.filter(file -> !file.equals("testFile.txt"));
		StacklessException exception = awaitException(staticLoader.load("testFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testClassPathWithDiffRoot() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(getExecutor(), "/");
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
		StaticLoader staticLoader = StaticLoader.ofPath(getExecutor(), Paths.get("/"))
				.filter(file -> !file.equals("testFile.txt"));
		StacklessException exception = awaitException(staticLoader.load("testFile.txt"));
		assertEquals(NOT_FOUND_EXCEPTION, exception);
	}

	@Test
	public void testClassPathWithDir() {
		StaticLoader staticLoader = StaticLoader.ofClassPath(getExecutor(), "dir");
		ByteBuf file = await(staticLoader.load("test.txt"));
		assertNotNull(file);
	}
}
