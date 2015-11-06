package io.datakernel.cube.api;

import io.datakernel.aggregation_db.api.NameResolver;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.util.Preconditions;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.cube.api.CommonUtils.generateGetter;
import static io.datakernel.cube.api.CommonUtils.generateSetter;

public final class Resolver {
	private final DefiningClassLoader classLoader;
	private final Map<String, NameResolver> nameResolvers;

	public Resolver(DefiningClassLoader classLoader, Map<String, NameResolver> nameResolvers) {
		this.classLoader = classLoader;
		this.nameResolvers = nameResolvers;
	}

	public List<Object> resolve(List<Object> records, Class<?> resultClass,
	                            Map<String, List<String>> nameKeys,
	                            Map<String, Class<?>> nameTypes,
	                            final Map<String, Object> keyConstants) {
		if (nameKeys.isEmpty())
			return records;

		for (Map.Entry<String, List<String>> mapping : nameKeys.entrySet()) {
			String resultDimensionName = mapping.getKey();
			List<String> keyDimensionNames = mapping.getValue();

			FieldGetter[] keyGetters = createKeyGetters(keyDimensionNames, keyConstants, resultClass);

			List<List<Object>> keys = retrieveKeyTuples(records, keyGetters);

			NameResolver nameResolver = nameResolvers.get(resultDimensionName);
			Preconditions.checkNotNull(nameResolver, "Resolver is not defined for " + resultDimensionName);

			List<Object> resolvedNames = nameResolver.resolveByKey(keyDimensionNames, keys);
			copyResolvedNamesToRecords(resolvedNames, records, resultClass, resultDimensionName, nameTypes);
		}

		return records;
	}

	private void copyResolvedNamesToRecords(List<Object> resolvedNames, List<Object> records, Class<?> resultClass,
	                                        String resultDimensionName, Map<String, Class<?>> nameTypes) {
		Preconditions.check(resolvedNames.size() == records.size(),
				String.format("Name resolver returned incorrect number (%d) of resolved names (required: %d)", resolvedNames.size(), records.size()));
		FieldSetter fieldSetter = generateSetter(classLoader, resultClass, resultDimensionName, nameTypes.get(resultDimensionName));
		for (int i = 0; i < resolvedNames.size(); i++) {
			fieldSetter.set(records.get(i), resolvedNames.get(i));
		}
	}

	private List<List<Object>> retrieveKeyTuples(List<Object> records, FieldGetter[] keyGetters) {
		List<List<Object>> keys = newArrayList();
		for (Object record : records) {
			List<Object> key = newArrayList();
			for (int i = 0; i < keyGetters.length; i++) {
				key.add(keyGetters[i].get(record));
			}
			keys.add(key);
		}
		return keys;
	}

	private FieldGetter[] createKeyGetters(List<String> keyDimensionNames, final Map<String, Object> keyConstants, Class<?> resultClass) {
		FieldGetter[] keyGetters = new FieldGetter[keyDimensionNames.size()];
		for (int i = 0; i < keyDimensionNames.size(); i++) {
			final String key = keyDimensionNames.get(i);
			if (keyConstants.containsKey(key)) {
				keyGetters[i] = new FieldGetter() {
					@Override
					public Object get(Object obj) {
						return keyConstants.get(key);
					}
				};
			} else {
				keyGetters[i] = generateGetter(classLoader, resultClass, key);
			}
		}
		return keyGetters;
	}
}
