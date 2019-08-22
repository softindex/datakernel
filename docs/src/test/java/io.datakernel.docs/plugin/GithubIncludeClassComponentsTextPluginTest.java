package io.datakernel.docs.plugin;

import io.datakernel.docs.dao.ResourceDao;
import io.datakernel.docs.plugin.text.GithubIncludeClassComponentsTextPlugin;
import io.datakernel.docs.plugin.text.PluginApplyException;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class GithubIncludeClassComponentsTextPluginTest {
	private static final String EMPTY = "";
	private static GithubIncludeClassComponentsTextPlugin plugin;
	private static String TEST_CLASS =
			"public final class GithubIncludeClassComponentsTextPluginTest {\n" +
					"\tprivate static GithubIncludeClassComponentsTextPlugin plugin;\n" +
					"\tprivate static int i = 10;\n" +
					"\tprivate static Lambda<Integer> lambda = (params) -> {(\"test\"+params)" +
					"\n};\n" +
					"\tpublic static final strictfp Optional<Double> field = (Double)Pair.of().get();\n" +
					"\t<T> void someMethod(Test param, Test<T> param2) { test = 10; \n" +
					"}" +
					"random text {}.-;<.>\n" +
					"}";

	@BeforeClass
	public static void init() {
		ResourceDao resourceDao = new ResourceDao() {
			@Override
			public String getResource(String resourceName) {
				return TEST_CLASS;
			}

			@Override
			public boolean exist(String resourceName) {
				return true;
			}
		};
		plugin = new GithubIncludeClassComponentsTextPlugin(resourceDao);
	}

	@Test
	public void testMethod() throws PluginApplyException {
		String result = plugin.apply(EMPTY, asList("className", "method", "someMethod"));
		assertEquals("<T> void someMethod(Test param, Test<T> param2) { test = 10; \n}", result);
	}

	@Test
	public void testAttribute() throws PluginApplyException {
		String result = plugin.apply(EMPTY, asList("className", "attribute", "field"));
		assertEquals("public static final strictfp Optional<Double> field = (Double)Pair.of().get();", result);
	}

	@Test()
	public void testAttributeLambda() throws PluginApplyException {
		String result = plugin.apply(EMPTY, asList("className", "attribute", "lambda"));
		assertEquals("private static Lambda<Integer> lambda = (params) -> {(\"test\"+params)\n};", result);
	}

	@Test
	public void testClassName() throws PluginApplyException {
		String result = plugin.apply(EMPTY, asList("className", "class", "GithubIncludeClassComponentsTextPluginTest"));
		assertEquals(TEST_CLASS, result);
	}
}
