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

package io.datakernel.cube.api;

import io.datakernel.aggregation_db.PrimaryKey;
import io.datakernel.codegen.utils.DefiningClassLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.junit.Assert.assertEquals;

public class ResolverTest {
	public static class Record {
		public int id;
		public String name1;
		public String name2;

		public Record(int id) {
			this.id = id;
		}
	}

	public static class TestConstantResolver implements AttributeResolver {
		@Override
		public Map<PrimaryKey, Object[]> resolve(Set<PrimaryKey> keys, List<String> attributes) {
			Map<PrimaryKey, Object[]> result = newHashMap();
			for (PrimaryKey key : keys) {
				String name1 = key.get(0).toString() + key.get(1).toString();
				String name2 = "~" + name1;
				result.put(key, new Object[]{name1, name2});
			}
			return result;
		}
	}

	@Test
	public void testResolve() throws Exception {
		List<Object> records = Arrays.asList((Object) new Record(1), new Record(2), new Record(3));
		TestConstantResolver testAttributeResolver = new TestConstantResolver();

		Map<String, AttributeResolver> attributeResolvers = newLinkedHashMap();
		attributeResolvers.put("name1", testAttributeResolver);
		attributeResolvers.put("name2", testAttributeResolver);

		Map<AttributeResolver, List<String>> resolverKeys = newHashMap();
		resolverKeys.put(testAttributeResolver, Arrays.asList("id", "constantId"));

		Map<String, Class<?>> attributeTypes = newLinkedHashMap();
		attributeTypes.put("name1", String.class);
		attributeTypes.put("name2", String.class);

		Map<String, Object> keyConstants = newHashMap();
		keyConstants.put("constantId", "ab");

 		Resolver resolver = new Resolver(attributeResolvers);

		List<Object> resultRecords = resolver.resolve(records, Record.class, attributeTypes, resolverKeys, keyConstants,
				new DefiningClassLoader());

		assertEquals("1ab", ((Record) resultRecords.get(0)).name1);
		assertEquals("2ab", ((Record) resultRecords.get(1)).name1);
		assertEquals("3ab", ((Record) resultRecords.get(2)).name1);
		assertEquals("~1ab", ((Record) resultRecords.get(0)).name2);
		assertEquals("~2ab", ((Record) resultRecords.get(1)).name2);
		assertEquals("~3ab", ((Record) resultRecords.get(2)).name2);
		assertEquals(1, ((Record) resultRecords.get(0)).id);
		assertEquals(2, ((Record) resultRecords.get(1)).id);
		assertEquals(3, ((Record) resultRecords.get(2)).id);
	}
}