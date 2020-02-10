package io.datakernel.aggregation.ot;

import io.datakernel.aggregation.ChunkIdCodec;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.common.Initializable;

import java.util.*;

import static io.datakernel.common.Preconditions.checkArgument;

@SuppressWarnings("rawtypes")
public final class AggregationStructure implements Initializable<AggregationStructure> {
	private final ChunkIdCodec<?> chunkIdCodec;
	private final Map<String, FieldType> keyTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> measureTypes = new LinkedHashMap<>();
	private final List<String> partitioningKey = new ArrayList<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();

	private AggregationStructure(ChunkIdCodec<?> chunkIdCodec) {
		this.chunkIdCodec = chunkIdCodec;
	}

	public static AggregationStructure create(ChunkIdCodec<?> chunkIdCodec) {
		return new AggregationStructure(chunkIdCodec);
	}

	public AggregationStructure withKey(String keyId, FieldType type) {
		checkArgument(!keyTypes.containsKey(keyId), "Key '%s' has already been added", keyId);
		keyTypes.put(keyId, type);
		return this;
	}

	public AggregationStructure withMeasure(String measureId, Measure aggregateFunction) {
		checkArgument(!measureTypes.containsKey(measureId), "Measure '%s' has already been added", measureId);
		measureTypes.put(measureId, aggregateFunction.getFieldType());
		measures.put(measureId, aggregateFunction);
		return this;
	}

	public AggregationStructure withIgnoredMeasure(String measureId, FieldType measureType) {
		checkArgument(!measureTypes.containsKey(measureId), "Measure '%s' has already been added", measureId);
		measureTypes.put(measureId, measureType);
		return this;
	}

	public AggregationStructure withPartitioningKey(List<String> partitioningKey) {
		this.partitioningKey.addAll(partitioningKey);
		return this;
	}

	public ChunkIdCodec<?> getChunkIdCodec() {
		return chunkIdCodec;
	}

	public AggregationStructure withPartitioningKey(String... partitioningKey) {
		this.partitioningKey.addAll(Arrays.asList(partitioningKey));
		return this;
	}

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

}
