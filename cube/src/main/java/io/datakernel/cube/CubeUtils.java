package io.datakernel.cube;

import io.datakernel.aggregation_db.AggregationUtils;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.processor.AggregateFunction;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.cube.api.AttributeResolver;
import io.datakernel.cube.api.RecordScheme;
import io.datakernel.util.WithParameter;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.WithUtils.get;

public class CubeUtils {
	private CubeUtils() {
	}

	public static RecordScheme recordScheme(Map<String, Class<?>> attributeTypes, Map<String, Class<?>> measureTypes,
	                                        List<String> attributes, List<String> measures) {
		RecordScheme recordScheme = RecordScheme.create();
		for (String attribute : attributes) {
			recordScheme = recordScheme.withField(attribute, attributeTypes.get(attribute));
		}
		for (String measure : measures) {
			recordScheme = recordScheme.withField(measure, measureTypes.get(measure));
		}
		return recordScheme;
	}

	public static Map<String, FieldType> projectMeasures(Map<String, AggregateFunction> aggregateFunctionMap, List<String> fields) {
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

	public interface KeyFunction<R> {
		Object[] extractKey(R record);
	}

	public interface AttributesFunction<R> {
		void applyAttributes(R record, Object[] attributes);
	}

	@SuppressWarnings("unchecked")
	public static <R> void resolveAttributes(List<R> results, final AttributeResolver attributeResolver,
	                                         final List<String> recordDimensions, final List<String> recordAttributes,
	                                         final Class<R> recordClass, DefiningClassLoader classLoader) {

		KeyFunction<R> keyFunction = ClassBuilder.create(classLoader, KeyFunction.class)
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

		AttributesFunction<R> attributesFunction = ClassBuilder.create(classLoader, AttributesFunction.class)
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

		for (R result : results) {
			Object[] key = keyFunction.extractKey(result);
			Object[] attributesRecord = attributeResolver.resolveAttributes(key);
			attributesFunction.applyAttributes(result, attributesRecord);
		}
	}

}
