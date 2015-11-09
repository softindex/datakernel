package io.datakernel.cube.api;

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.api.AttributeResolver;
import io.datakernel.aggregation_db.api.ReportingDSLExpression;
import io.datakernel.codegen.Expression;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;

public final class ReportingConfiguration {
	private Map<String, ReportingDSLExpression> computedMeasures = newHashMap();
	private Map<String, Class<?>> attributeTypes = newLinkedHashMap();
	private Map<String, AttributeResolver> attributeResolvers = newLinkedHashMap();
	private Map<AttributeResolver, List<String>> resolverKeys = newHashMap();

	public ReportingConfiguration addComputedMeasure(String name, ReportingDSLExpression expression) {
		this.computedMeasures.put(name, expression);
		return this;
	}

	public ReportingConfiguration setComputedMeasures(Map<String, ReportingDSLExpression> computedMeasures) {
		this.computedMeasures = newHashMap(computedMeasures);
		return this;
	}

	public ReportingConfiguration addResolvedAttribute(String name, List<String> key, Class<?> type, AttributeResolver resolver) {
		this.attributeTypes.put(name, type);
		this.attributeResolvers.put(name, resolver);
		this.resolverKeys.put(resolver, key);
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

	public boolean containsComputedMeasure(String computedMeasure) {
		return computedMeasures.containsKey(computedMeasure);
	}

	public ReportingDSLExpression getExpressionForMeasure(String computedMeasure) {
		return computedMeasures.get(computedMeasure);
	}

	public Expression getComputedMeasureExpression(String computedMeasure) {
		return computedMeasures.get(computedMeasure).getExpression();
	}
}
