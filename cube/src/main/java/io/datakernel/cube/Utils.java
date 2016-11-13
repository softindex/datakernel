package io.datakernel.cube;

import io.datakernel.async.CompletionCallback;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.cube.attributes.AttributeResolver;
import io.datakernel.util.WithValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.codegen.Expressions.*;

class Utils {
	private Utils() {
	}

	public static Class<?> createResultClass(Collection<String> attributes, Collection<String> measures,
	                                         Cube cube, DefiningClassLoader classLoader) {
		ClassBuilder<Object> builder = ClassBuilder.create(classLoader, Object.class);
		for (String attribute : attributes) {
			builder = builder.withField(attribute.replace('.', '$'), cube.getAttributeInternalType(attribute));
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
				.withMethod("extractKey", new WithValue<Expression>() {
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
				}.get())
				.buildClassAndCreateNewInstance();

		final ArrayList<String> resolverAttributes = newArrayList(attributeResolver.getAttributeTypes().keySet());
		final AttributeResolver.AttributesFunction attributesFunction = ClassBuilder.create(classLoader, AttributeResolver.AttributesFunction.class)
				.withMethod("applyAttributes", new WithValue<Expression>() {
					@Override
					public Expression get() {
						ExpressionSequence applyAttributes = ExpressionSequence.create();
						for (String attribute : recordAttributes) {
							String attributeName = attribute.substring(attribute.indexOf('.') + 1);
							int resolverAttributeIndex = resolverAttributes.indexOf(attributeName);
							applyAttributes.add(set(
									field(cast(arg(0), recordClass), attribute.replace('.', '$')),
									getArrayItem(arg(1), value(resolverAttributeIndex))));
						}
						return applyAttributes;
					}
				}.get())
				.buildClassAndCreateNewInstance();

		attributeResolver.resolveAttributes((List) results, keyFunction, attributesFunction, callback);
	}

}
