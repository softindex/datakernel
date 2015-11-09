package io.datakernel.cube.api;

import io.datakernel.aggregation_db.PrimaryKey;
import io.datakernel.aggregation_db.api.AttributeResolver;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.datakernel.cube.api.CommonUtils.generateGetter;
import static io.datakernel.cube.api.CommonUtils.generateSetter;

public final class Resolver {
	private final DefiningClassLoader classLoader;
	private final Map<String, AttributeResolver> attributeResolvers;

	public Resolver(DefiningClassLoader classLoader, Map<String, AttributeResolver> attributeResolvers) {
		this.classLoader = classLoader;
		this.attributeResolvers = attributeResolvers;
	}

	public List<Object> resolve(List<Object> records, Class<?> resultClass,
	                            Map<String, List<String>> attributeKeys,
	                            Map<String, Class<?>> attributeTypes) {
		return resolve(records, resultClass, attributeKeys, attributeTypes, null);
	}

	public List<Object> resolve(List<Object> records, Class<?> resultClass,
	                            Map<String, List<String>> attributeKeys,
	                            Map<String, Class<?>> attributeTypes,
	                            Map<String, Object> keyConstants) {
		if (attributeKeys.isEmpty())
			return records;

		for (Map.Entry<String, List<String>> mapping : attributeKeys.entrySet()) {
			String attributeName = mapping.getKey();
			List<String> keyNames = mapping.getValue();

			FieldGetter[] keyGetters = createKeyGetters(keyNames, keyConstants, resultClass);

			List<PrimaryKey> keys = retrieveKeyTuples(records, keyGetters);

			AttributeResolver attributeResolver = attributeResolvers.get(attributeName);
			Preconditions.checkNotNull(attributeResolver, "Resolver is not defined for " + attributeName);

			Map<PrimaryKey, Object> resolvedAttributes = attributeResolver.resolve(keys);
			copyResolvedNamesToRecords(resolvedAttributes, keys, records, resultClass, attributeName, attributeTypes);
		}

		return records;
	}

	private void copyResolvedNamesToRecords(Map<PrimaryKey, Object> resolvedAttributes, List<PrimaryKey> keys,
	                                        List<Object> records, Class<?> resultClass,
	                                        String attributeName, Map<String, Class<?>> attributeTypes) {
		Preconditions.check(resolvedAttributes.size() == records.size(),
				String.format("Name resolver returned incorrect number (%d) of resolved names (required: %d)", resolvedAttributes.size(), records.size()));
		FieldSetter fieldSetter = generateSetter(classLoader, resultClass, attributeName, attributeTypes.get(attributeName));
		for (int i = 0; i < records.size(); i++) {
			fieldSetter.set(records.get(i), resolvedAttributes.get(keys.get(i)));
		}
	}

	private List<PrimaryKey> retrieveKeyTuples(List<Object> records, FieldGetter[] keyGetters) {
		List<PrimaryKey> keys = new ArrayList<>(records.size());
		for (Object record : records) {
			Object[] key = new Object[keyGetters.length];
			for (int i = 0; i < keyGetters.length; i++) {
				key[i] = keyGetters[i].get(record);
			}
			keys.add(PrimaryKey.ofArray(key));
		}
		return keys;
	}

	private FieldGetter[] createKeyGetters(List<String> keyNames, final Map<String, Object> keyConstants, Class<?> resultClass) {
		FieldGetter[] keyGetters = new FieldGetter[keyNames.size()];
		for (int i = 0; i < keyNames.size(); i++) {
			final String key = keyNames.get(i);
			if (keyConstants != null && keyConstants.containsKey(key)) {
				final Object keyConstant = keyConstants.get(key);
				keyGetters[i] = new FieldGetter() {
					@Override
					public Object get(Object obj) {
						return keyConstant;
					}
				};
			} else {
				keyGetters[i] = generateGetter(classLoader, resultClass, key);
			}
		}
		return keyGetters;
	}
}
