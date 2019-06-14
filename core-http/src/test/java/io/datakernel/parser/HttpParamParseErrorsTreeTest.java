package io.datakernel.parser;

import io.datakernel.http.parser.HttpParamParseErrorsTree;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.list;
import static org.junit.Assert.*;

public class HttpParamParseErrorsTreeTest {
	@Test
	public void testMergeWithTheSame() {
		HttpParamParseErrorsTree tree = HttpParamParseErrorsTree.create();
		tree.with(list(HttpParamParseErrorsTree.Error.of("test1"),
				HttpParamParseErrorsTree.Error.of("test2"),
				HttpParamParseErrorsTree.Error.of("test3"),
				HttpParamParseErrorsTree.Error.of("test4")));

		HttpParamParseErrorsTree tree2 = HttpParamParseErrorsTree.create();
		tree.with(list(HttpParamParseErrorsTree.Error.of("test11"),
				HttpParamParseErrorsTree.Error.of("test22"),
				HttpParamParseErrorsTree.Error.of("test33"),
				HttpParamParseErrorsTree.Error.of("test44")));

		tree.merge(tree2);
		assertEquals(8, tree.toMultimap().get("").size());
	}

	@Test
	public void test() {
		HttpParamParseErrorsTree tree = HttpParamParseErrorsTree.of("Test");
		assertTrue(tree.hasErrors());

		assertEquals(Collections.emptySet(), tree.getChildren());
		assertEquals(1, tree.toMap().size());
		assertNull(tree.getChild("$"));
		assertEquals(1, tree.toMap().size());
	}

	@Test
	public void testMap() {
		HttpParamParseErrorsTree tree = HttpParamParseErrorsTree.create();
		tree.with("test", HttpParamParseErrorsTree.of("tmp1")
				.with("test2", HttpParamParseErrorsTree.of("tmp2")));
		tree.with("test3", HttpParamParseErrorsTree.of("tmp3"));
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
		HttpParamParseErrorsTree tree = HttpParamParseErrorsTree.create();
		tree.with("test", HttpParamParseErrorsTree.of("tmp1")
				.with("test2", HttpParamParseErrorsTree.of("tmp2")));
		tree.with("test3", HttpParamParseErrorsTree.of("tmp3"));

		Map<String, List<String>> errors = tree.toMultimap();
		assertEquals(3, errors.size());
		assertEquals(1, errors.get("test").size());
	}
}
