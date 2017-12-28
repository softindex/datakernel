package io.datakernel.aggregation.ot;

import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;

import java.util.*;

import static io.datakernel.util.Preconditions.checkArgument;

public final class AggregationStructure {

	private final Map<String, FieldType> keyTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> measureTypes = new LinkedHashMap<>();
	private final List<String> partitioningKey = new ArrayList<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();

	public List<String> getKeys() {
		return new ArrayList<>(keyTypes.keySet());
	}

	public List<String> getMeasures() {
		return new ArrayList<>(measures.keySet());
	}

	public Map<String, FieldType> getKeyTypes() {
		return keyTypes;
	}

	public Map<String, FieldType> getMeasureTypes() {
		return measureTypes;
	}

	public Measure getMeasure(String field) {
		return measures.get(field);
	}

	public FieldType getKeyType(String key) {
		return keyTypes.get(key);
	}

	public FieldType getMeasureType(String field) {
		return measureTypes.get(field);
	}

	public List<String> getPartitioningKey() {
		return partitioningKey;
	}

	public AggregationStructure withKey(String keyId, FieldType type) {
		checkArgument(!keyTypes.containsKey(keyId));
		keyTypes.put(keyId, type);
		return this;
	}

	public AggregationStructure withKeys(Map<String, FieldType> keyTypes) {
		this.keyTypes.putAll(keyTypes);
		return this;
	}

	public AggregationStructure withMeasure(String measureId, Measure aggregateFunction) {
		checkArgument(!measureTypes.containsKey(measureId));
		measureTypes.put(measureId, aggregateFunction.getFieldType());
		measures.put(measureId, aggregateFunction);
		return this;
	}

	public AggregationStructure withMeasures(Map<String, Measure> measures) {
		for (String measureId : measures.keySet()) {
			withMeasure(measureId, measures.get(measureId));
		}
		return this;
	}

	public AggregationStructure withIgnoredMeasure(String measureId, FieldType measureType) {
		checkArgument(!measureTypes.containsKey(measureId));
		measureTypes.put(measureId, measureType);
		return this;
	}

	public AggregationStructure withIgnoredMeasures(Map<String, FieldType> measureTypes) {
		checkArgument(intersection(this.measureTypes.keySet(), measureTypes.keySet()).isEmpty());
		this.measureTypes.putAll(measureTypes);
		return this;
	}

	private static <T> Set<T> intersection(Set<T> a, Set<T> b) {
		final Set<T> set = new HashSet<>();
		for (T x : a) {
			if (b.contains(x)) {
				set.add(x);
			}
		}
		return set;
	}

	public AggregationStructure withPartitioningKey(List<String> partitioningKey) {
		this.partitioningKey.addAll(partitioningKey);
		return this;
	}

	public AggregationStructure withPartitioningKey(String... partitioningKey) {
		this.partitioningKey.addAll(Arrays.asList(partitioningKey));
		return this;
	}

}
