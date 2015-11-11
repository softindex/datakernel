package io.datakernel.cube.api;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.datakernel.aggregation_db.PrimaryKey;
import io.datakernel.codegen.utils.DefiningClassLoader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.cube.api.CommonUtils.generateGetter;
import static io.datakernel.cube.api.CommonUtils.generateSetter;

public final class Resolver {
	private final DefiningClassLoader classLoader;
	private final Map<String, AttributeResolver> attributeResolvers;

	public Resolver(DefiningClassLoader classLoader, Map<String, AttributeResolver> attributeResolvers) {
		this.classLoader = classLoader;
		this.attributeResolvers = attributeResolvers;
	}

	public List<Object> resolve(List<Object> records, Class<?> recordClass,
	                            Map<String, Class<?>> attributeTypes,
	                            Map<AttributeResolver, List<String>> resolverKeys,
	                            Map<String, Object> keyConstants) {
		if (attributeTypes.isEmpty())
			return records;

		ListMultimap<AttributeResolver, String> resolverAttributes = groupAttributesByResolvers(attributeTypes.keySet());

		for (AttributeResolver resolver : resolverAttributes.keySet()) {
			List<String> attributes = resolverAttributes.get(resolver);
			List<String> key = resolverKeys.get(resolver);

			FieldGetter[] keyGetters = createKeyGetters(key, keyConstants, recordClass);
			List<PrimaryKey> keys = retrieveKeyTuples(records, keyGetters);
			Set<PrimaryKey> uniqueKeys = newHashSet(keys);

			Map<PrimaryKey, Object[]> resolvedAttributes = resolver.resolve(uniqueKeys, attributes);
			copyResolvedNamesToRecords(resolvedAttributes, keys, records, recordClass, attributes, attributeTypes);
		}

		return records;
	}

	private ListMultimap<AttributeResolver, String> groupAttributesByResolvers(Set<String> attributes) {
		ListMultimap<AttributeResolver, String> resolverAttributes = ArrayListMultimap.create();
		for (String attribute : attributes) {
			resolverAttributes.put(attributeResolvers.get(attribute), attribute);
		}
		return resolverAttributes;
	}

	private void copyResolvedNamesToRecords(Map<PrimaryKey, Object[]> resolvedAttributes, List<PrimaryKey> keys,
	                                        List<Object> records, Class<?> resultClass,
	                                        List<String> attributes, Map<String, Class<?>> attributeTypes) {
		FieldSetter[] fieldSetters = createFieldSetters(attributes, resultClass, attributeTypes);
		for (int i = 0; i < records.size(); i++) {
			Object record = records.get(i);
			Object[] resolvedValues = resolvedAttributes.get(keys.get(i));
			for (int j = 0; j < fieldSetters.length; ++j) {
				fieldSetters[j].set(record, resolvedValues[j]);
			}
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

	private FieldSetter[] createFieldSetters(List<String> attributes, Class<?> resultClass, Map<String, Class<?>> attributeTypes) {
		FieldSetter[] fieldSetters = new FieldSetter[attributes.size()];
		for (int i = 0; i < attributes.size(); ++i) {
			String attributeName = attributes.get(i);
			fieldSetters[i] = generateSetter(classLoader, resultClass, attributeName, attributeTypes.get(attributeName));
		}
		return fieldSetters;
	}

	private FieldGetter[] createKeyGetters(List<String> keyNames, final Map<String, Object> keyConstants, Class<?> resultClass) {
		FieldGetter[] keyGetters = new FieldGetter[keyNames.size()];
		for (int i = 0; i < keyNames.size(); ++i) {
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
