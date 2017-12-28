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

package io.datakernel.cube;

import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.cube.attributes.AttributeResolver;
import io.datakernel.util.WithValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

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
	public static <R> CompletionStage<Void> resolveAttributes(final List<R> results, final AttributeResolver attributeResolver,
	                                                          final List<String> recordDimensions, final List<String> recordAttributes,
	                                                          final Map<String, Object> fullySpecifiedDimensions,
	                                                          final Class<R> recordClass, DefiningClassLoader classLoader) {
		final Object[] fullySpecifiedDimensionsArray = new Object[recordDimensions.size()];
		for (int i = 0; i < recordDimensions.size(); i++) {
			String dimension = recordDimensions.get(i);
			if (fullySpecifiedDimensions.containsKey(dimension)) {
				fullySpecifiedDimensionsArray[i] = fullySpecifiedDimensions.get(dimension);
			}
		}
		final AttributeResolver.KeyFunction keyFunction = ClassBuilder.create(classLoader, AttributeResolver.KeyFunction.class)
				.withMethod("extractKey", ((WithValue<Expression>) () -> {
					ExpressionSequence extractKey = ExpressionSequence.create();
					Expression key = let(newArray(Object.class, value(recordDimensions.size())));
					for (int i = 0; i < recordDimensions.size(); i++) {
						String dimension = recordDimensions.get(i);
						extractKey.add(setArrayItem(key, value(i),
								fullySpecifiedDimensions.containsKey(dimension) ?
										getArrayItem(value(fullySpecifiedDimensionsArray), value(i)) :
										cast(field(cast(arg(0), recordClass), dimension), Object.class)));
					}
					return extractKey.add(key);
				}).get())
				.buildClassAndCreateNewInstance();

		final ArrayList<String> resolverAttributes = new ArrayList<>(attributeResolver.getAttributeTypes().keySet());
		final AttributeResolver.AttributesFunction attributesFunction = ClassBuilder.create(classLoader, AttributeResolver.AttributesFunction.class)
				.withMethod("applyAttributes", ((WithValue<Expression>) () -> {
					ExpressionSequence applyAttributes = ExpressionSequence.create();
					for (String attribute : recordAttributes) {
						String attributeName = attribute.substring(attribute.indexOf('.') + 1);
						int resolverAttributeIndex = resolverAttributes.indexOf(attributeName);
						applyAttributes.add(set(
								field(cast(arg(0), recordClass), attribute.replace('.', '$')),
								getArrayItem(arg(1), value(resolverAttributeIndex))));
					}
					return applyAttributes;
				}).get())
				.buildClassAndCreateNewInstance();

		return attributeResolver.resolveAttributes((List) results, keyFunction, attributesFunction);
	}

}
