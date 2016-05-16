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

import com.google.common.collect.ImmutableMap;
import io.datakernel.codegen.Expression;
import io.datakernel.util.Function;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;

public final class ReportingConfiguration {
	private Map<String, ReportingDSLExpression> computedMeasures = newHashMap();
	private Map<String, Class<?>> attributeTypes = newHashMap();
	private Map<String, AttributeResolver> attributeResolvers = newHashMap();
	private Map<AttributeResolver, List<String>> resolverKeys = newHashMap();
	private Map<String, Function> dimensionPostFilteringFunctions = newHashMap();

	private Map<String, String> attributeDimensions = newHashMap();

	public ReportingConfiguration addComputedMeasure(String name, ReportingDSLExpression expression) {
		this.computedMeasures.put(name, expression);
		return this;
	}

	public ReportingConfiguration setComputedMeasures(Map<String, ReportingDSLExpression> computedMeasures) {
		this.computedMeasures = newHashMap(computedMeasures);
		return this;
	}

	public ReportingConfiguration addResolvedAttributeForKey(String name, List<String> key, Class<?> type, AttributeResolver resolver) {
		this.attributeTypes.put(name, type);
		this.attributeResolvers.put(name, resolver);
		this.resolverKeys.put(resolver, key);
		return this;
	}

	public ReportingConfiguration addResolvedAttributeForDimension(String name, String dimension, Class<?> type, AttributeResolver resolver) {
		this.attributeTypes.put(name, type);
		this.attributeResolvers.put(name, resolver);
		this.attributeDimensions.put(name, dimension);
		return this;
	}

	public ReportingConfiguration addDimensionPostFilteringFunction(String dimension, Function function) {
		this.dimensionPostFilteringFunctions.put(dimension, function);
		return this;
	}

	public boolean containsAttribute(String key) {
		return attributeTypes.containsKey(key);
	}

	public AttributeResolver getAttributeResolver(String name) {
		return attributeResolvers.get(name);
	}

	public List<String> getKeyForResolver(AttributeResolver resolver) {
		return resolverKeys.get(resolver);
	}

	public Class<?> getAttributeType(String name) {
		return attributeTypes.get(name);
	}

	public Map<String, AttributeResolver> getResolvers() {
		return ImmutableMap.copyOf(attributeResolvers);
	}

	public Map<String, String> getAttributeDimensions() {
		return attributeDimensions;
	}

	public void setKeyForAttribute(String name, List<String> key) {
		resolverKeys.put(attributeResolvers.get(name), key);
	}

	public boolean containsComputedMeasure(String computedMeasure) {
		return computedMeasures.containsKey(computedMeasure);
	}

	public ReportingDSLExpression getExpressionForMeasure(String computedMeasure) {
		return computedMeasures.get(computedMeasure);
	}

	public Expression getComputedMeasureExpression(String computedMeasure) {
		return computedMeasures.get(computedMeasure).getExpression();
	}

	public Set<String> getComputedMeasures() {
		return computedMeasures.keySet();
	}

	public Set<String> getComputedMeasureDependencies(String computedMeasure) {
		return computedMeasures.get(computedMeasure).getMeasureDependencies();
	}

	public Function getPostFilteringFunctionForDimension(String dimension) {
		return dimensionPostFilteringFunctions.get(dimension);
	}
}
