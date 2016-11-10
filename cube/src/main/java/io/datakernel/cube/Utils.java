package io.datakernel.cube;

import io.datakernel.aggregation.AggregationUtils;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.async.CompletionCallback;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.cube.attributes.AttributeResolver;
import io.datakernel.util.WithParameter;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.WithUtils.get;

class Utils {
	private Utils() {
	}

	public static Map<String, FieldType> projectMeasures(Map<String, Measure> aggregateFunctionMap, List<String> fields) {
		Map<String, FieldType> map = new LinkedHashMap<>();
		for (String key : aggregateFunctionMap.keySet()) {
			map.put(key, aggregateFunctionMap.get(key).getFieldType());
		}
		return AggregationUtils.projectFields(map, fields);
	}

	public static Class<?> createResultClass(Collection<String> attributes, Collection<String> measures,
	                                         Cube cube, DefiningClassLoader classLoader) {
		ClassBuilder<Object> builder = ClassBuilder.create(classLoader, Object.class);
		for (String attribute : attributes) {
			builder = builder.withField(attribute.replace('.', '$'), cube.getAttributeType(attribute));
		}
		for (String measure : measures) {
			builder = builder.withField(measure, cube.getMeasureInternalType(measure));
		}
		return builder.build();
	}

	static boolean startsWith(List<String> list, List<String> prefix) {
		if (prefix.size() >= list.size())
			return false;

		for (int i = 0; i < prefix.size(); ++i) {
			if (!list.get(i).equals(prefix.get(i)))
				return false;
		}

		return true;
	}

	@SuppressWarnings("unchecked")
	public static <R> void resolveAttributes(final List<R> results, final AttributeResolver attributeResolver,
	                                              final List<String> recordDimensions, final List<String> recordAttributes,
	                                              final Class<R> recordClass, DefiningClassLoader classLoader,
	                                              CompletionCallback callback) {

		final AttributeResolver.KeyFunction keyFunction = ClassBuilder.create(classLoader, AttributeResolver.KeyFunction.class)
				.withMethod("extractKey", get(new WithParameter<Expression>() {
					@Override
					public Expression get() {
						ExpressionSequence extractKey = ExpressionSequence.create();
						Expression key = let(newArray(Object.class, value(recordDimensions.size())));
						for (int i = 0; i < recordDimensions.size(); i++) {
							String dimension = recordDimensions.get(i);
							extractKey.add(setArrayItem(key, value(i),
									cast(field(cast(arg(0), recordClass), dimension), Object.class)));
						}
						return extractKey.add(key);
					}
				}))
				.buildClassAndCreateNewInstance();

		final AttributeResolver.AttributesFunction attributesFunction = ClassBuilder.create(classLoader, AttributeResolver.AttributesFunction.class)
				.withMethod("applyAttributes", get(new WithParameter<Expression>() {
					@Override
					public Expression get() {
						ExpressionSequence applyAttributes = ExpressionSequence.create();
						for (int i = 0; i < recordAttributes.size(); i++) {
							String attribute = recordAttributes.get(i);
							applyAttributes.add(set(
									field(cast(arg(0), recordClass), attribute.replace('.', '$')),
									getArrayItem(arg(1), value(i))));
						}
						return applyAttributes;
					}
				}))
				.buildClassAndCreateNewInstance();

		attributeResolver.resolveAttributes((List) results, keyFunction, attributesFunction, callback);
	}

}
