package io.datakernel.cube.api;

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.api.NameResolver;
import io.datakernel.aggregation_db.api.ReportingDSLExpression;
import io.datakernel.codegen.Expression;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public final class ReportingConfiguration {
	private Map<String, ReportingDSLExpression> computedMeasures = newHashMap();
	private Map<String, List<String>> nameKeys = newHashMap();
	private Map<String, Class<?>> nameTypes = newHashMap();
	private Map<String, NameResolver> nameResolvers = newHashMap();

	public ReportingConfiguration addComputedMeasure(String name, ReportingDSLExpression expression) {
		this.computedMeasures.put(name, expression);
		return this;
	}

	public ReportingConfiguration setComputedMeasures(Map<String, ReportingDSLExpression> computedMeasures) {
		this.computedMeasures = newHashMap(computedMeasures);
		return this;
	}

	public ReportingConfiguration addResolvedName(String name, List<String> key, Class<?> type, NameResolver resolver) {
		this.nameKeys.put(name, key);
		this.nameTypes.put(name, type);
		this.nameResolvers.put(name, resolver);
		return this;
	}

	public boolean containsName(String key) {
		return nameKeys.containsKey(key);
	}

	public List<String> getNameKey(String name) {
		return nameKeys.get(name);
	}

	public Class<?> getNameType(String name) {
		return nameTypes.get(name);
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
