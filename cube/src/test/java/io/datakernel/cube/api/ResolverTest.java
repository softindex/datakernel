package io.datakernel.cube.api;

import io.datakernel.aggregation_db.api.NameResolver;
import io.datakernel.codegen.utils.DefiningClassLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

public class ResolverTest {
	public static class Record {
		public int id;
		public String name;

		public Record(int id) {
			this.id = id;
		}
	}

	public static class TestConstantResolver implements NameResolver {
		@Override
		public List<Object> resolveByKey(List<String> keyNames, List<List<Object>> keys) {
			List<Object> names = newArrayList();
			for (List<Object> key : keys) {
				names.add(key.get(0).toString() + key.get(1).toString());
			}
			return names;
		}
	}

	@Test
	public void testResolve() throws Exception {
		List<Object> records = Arrays.asList((Object) new Record(1), new Record(2), new Record(3));

		Map<String, NameResolver> nameResolvers = newHashMap();
		nameResolvers.put("name", new TestConstantResolver());

		Map<String, List<String>> nameKeys = newHashMap();
		nameKeys.put("name", Arrays.asList("id", "constantId"));

		Map<String, Class<?>> nameTypes = newHashMap();
		nameTypes.put("name", String.class);

		Map<String, Object> keyConstants = newHashMap();
		keyConstants.put("constantId", "ab");

 		Resolver resolver = new Resolver(new DefiningClassLoader(), nameResolvers);


		List<Object> resultRecords = resolver.resolve(records, Record.class, nameKeys, nameTypes, keyConstants);


		assertEquals("1ab", ((Record) resultRecords.get(0)).name);
		assertEquals("2ab", ((Record) resultRecords.get(1)).name);
		assertEquals("3ab", ((Record) resultRecords.get(2)).name);
		assertEquals(1, ((Record) resultRecords.get(0)).id);
		assertEquals(2, ((Record) resultRecords.get(1)).id);
		assertEquals(3, ((Record) resultRecords.get(2)).id);
	}
}