package io.datakernel.http.decoder;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.list;
import static org.junit.Assert.*;

public class HttpDecodeErrorsTest {
	@Test
	public void testMergeWithTheSame() {
		HttpDecodeErrors tree = HttpDecodeErrors.create();
		tree.with(list(HttpDecodeErrors.Error.of("test1"),
				HttpDecodeErrors.Error.of("test2"),
				HttpDecodeErrors.Error.of("test3"),
				HttpDecodeErrors.Error.of("test4")));

		HttpDecodeErrors tree2 = HttpDecodeErrors.create();
		tree.with(list(HttpDecodeErrors.Error.of("test11"),
				HttpDecodeErrors.Error.of("test22"),
				HttpDecodeErrors.Error.of("test33"),
				HttpDecodeErrors.Error.of("test44")));

		tree.merge(tree2);
		assertEquals(8, tree.toMultimap().get("").size());
	}

	@Test
	public void test() {
		HttpDecodeErrors tree = HttpDecodeErrors.of("Test");
		assertTrue(tree.hasErrors());

		assertEquals(Collections.emptySet(), tree.getChildren());
		assertEquals(1, tree.toMap().size());
		assertNull(tree.getChild("$"));
		assertEquals(1, tree.toMap().size());
	}

	@Test
	public void testMap() {
		HttpDecodeErrors tree = HttpDecodeErrors.create();
		tree.with("test", HttpDecodeErrors.of("tmp1")
				.with("test2", HttpDecodeErrors.of("tmp2")));
		tree.with("test3", HttpDecodeErrors.of("tmp3"));
		Map<String, String> errors = tree.toMap();
		assertEquals(3, errors.size());
		assertNotNull(errors.get("test"));
		assertNotNull(errors.get("test.test2"));
		assertNotNull(errors.get("test3"));

		Map<String, String> errorsWithSeparator = tree.toMap("-");
		assertNotNull(errorsWithSeparator.get("test-test2"));
	}

	@Test
	public void testMultiMap() {
		HttpDecodeErrors tree = HttpDecodeErrors.create();
		tree.with("test", HttpDecodeErrors.of("tmp1")
				.with("test2", HttpDecodeErrors.of("tmp2")));
		tree.with("test3", HttpDecodeErrors.of("tmp3"));

		Map<String, List<String>> errors = tree.toMultimap();
		assertEquals(3, errors.size());
		assertEquals(1, errors.get("test").size());
	}
}
