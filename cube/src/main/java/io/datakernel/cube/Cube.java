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

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.primitives.Primitives;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.AggregationMetadataStorage.LoadedChunks;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.processor.AggregateFunction;
import io.datakernel.aggregation_db.util.AsyncResultsTracker.AsyncResultsTrackerMultimap;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.*;
import io.datakernel.cube.CubeMetadataStorage.CubeLoadedChunks;
import io.datakernel.cube.api.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.*;
import static io.datakernel.aggregation_db.AggregationUtils.*;
import static io.datakernel.aggregation_db.util.AsyncResultsTracker.ofMultimap;
import static io.datakernel.codegen.ExpressionComparator.leftField;
import static io.datakernel.codegen.ExpressionComparator.rightField;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.CubeUtils.*;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
@SuppressWarnings("unchecked")
public final class Cube implements ICube, EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(Cube.class);

	public static final int DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD = 300;

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final CubeMetadataStorage cubeMetadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;

	private final Map<String, FieldType> dimensionTypes = new LinkedHashMap<>();
	private final Map<String, AggregateFunction> measures = new LinkedHashMap<>();
	private final Map<String, ComputedMeasure> computedMeasures = newLinkedHashMap();

	private final ComputedMeasure.StoredMeasures storedMeasures = new ComputedMeasure.StoredMeasures() {
		@Override
		public Class<?> getStoredMeasureType(String storedMeasureId) {
			return (Class<?>) measures.get(storedMeasureId).getFieldType().getDataType();
		}

		@Override
		public Expression getStoredMeasureValue(Expression record, String storedMeasureId) {
			return measures.get(storedMeasureId).valueOfAccumulator(field(record, storedMeasureId));
		}
	};

	private final FieldType.FieldConverters fieldConverters = new FieldType.FieldConverters() {
		@Override
		public Object toInternalValue(String field, Object value) {
			FieldType fieldType = getFieldType(field);
			return fieldType == null ? value : fieldType.toInternalValue(value);
		}

		@Override
		public Object toValue(String field, Object internalValue) {
			FieldType fieldType = getFieldType(field);
			return fieldType == null ? internalValue : fieldType.toValue(internalValue);
		}

		private FieldType getFieldType(String field) {
			if (dimensionTypes.containsKey(field)) {
				return dimensionTypes.get(field);
			}
			if (measures.containsKey(field)) {
				return measures.get(field).getFieldType();
			}
			return null;
		}
	};

	private static final class AttributeResolverInfo {
		private final List<String> attributes;
		private final List<String> dimensions;
		private final AttributeResolver resolver;

		private AttributeResolverInfo(List<String> dimensions, List<String> attributes, AttributeResolver resolver) {
			this.attributes = attributes;
			this.dimensions = dimensions;
			this.resolver = resolver;
		}
	}

	private final List<AttributeResolverInfo> attributeResolvers = newArrayList();
	private final Map<String, Class<?>> attributeTypes = new LinkedHashMap<>();
	private final Map<String, AttributeResolverInfo> attributes = new LinkedHashMap<>();

	// settings
	private int aggregationChunkSize = Aggregation.DEFAULT_AGGREGATION_CHUNK_SIZE;
	private int sorterItemsInMemory = Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private int sorterBlockSize = Aggregation.DEFAULT_SORTER_BLOCK_SIZE;
	private int overlappingChunksThreshold = Cube.DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD;
	private int maxIncrementalReloadPeriodMillis = Aggregation.DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS;
	private boolean ignoreChunkReadingExceptions = false;

	private static final class AggregationInfo {
		private final Aggregation aggregation;
		private final List<String> measures;
		private final AggregationPredicate predicate;

		private AggregationInfo(Aggregation aggregation, List<String> measures, AggregationPredicate predicate) {
			this.aggregation = aggregation;
			this.measures = measures;
			this.predicate = predicate;
		}
	}

	// state
	private Map<String, AggregationInfo> aggregations = new LinkedHashMap<>();
	private AggregationKeyRelations childParentRelations = AggregationKeyRelations.create();
	private int lastRevisionId;
	private long lastReloadTimestamp;

	private LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache;

	private Cube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	             CubeMetadataStorage cubeMetadataStorage, AggregationChunkStorage aggregationChunkStorage) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.cubeMetadataStorage = cubeMetadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
	}

	public static Cube create(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                          CubeMetadataStorage cubeMetadataStorage, AggregationChunkStorage aggregationChunkStorage) {
		return new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, aggregationChunkStorage);
	}

	public Cube withAttribute(String attribute, AttributeResolver resolver) {
		return withAttributes(asList(attribute), resolver);
	}

	public Cube withAttributes(List<String> attributes, AttributeResolver resolver) {
		String dimension = null;
		for (String attribute : attributes) {
			int pos = attribute.indexOf('.');
			if (pos == -1)
				throw new IllegalArgumentException();
			String d = attribute.substring(0, pos);
			if (dimension != null && !dimension.equals(d))
				throw new IllegalArgumentException();
			dimension = d;
		}
		return withAttributes(asList(dimension), attributes, resolver);
	}

	public Cube withAttributes(String dimension, List<String> attributes, AttributeResolver resolver) {
		return withAttributes(asList(dimension), attributes, resolver);
	}

	public Cube withAttribute(String dimension, String attribute, AttributeResolver resolver) {
		return withAttributes(asList(dimension), asList(attribute), resolver);
	}

	public Cube withAttributes(List<String> dimensions, List<String> attributes, AttributeResolver resolver) {
		AttributeResolverInfo resolverInfo = new AttributeResolverInfo(dimensions, attributes, resolver);
		attributeResolvers.add(resolverInfo);
		Class<?>[] attributeTypes = resolver.getAttributeTypes();
		for (int i = 0; i < attributes.size(); i++) {
			String attribute = attributes.get(i);
			this.attributeTypes.put(attribute, attributeTypes[i]);
			this.attributes.put(attribute, resolverInfo);
		}
		return this;
	}

	public Cube withCache(LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache) {
		this.classLoaderCache = classLoaderCache;
		return this;
	}

	public Cube withDimension(String dimensionId, FieldType type) {
		checkState(aggregations.isEmpty());
		dimensionTypes.put(dimensionId, type);
		return this;
	}

	public Cube withDimensions(Map<String, FieldType> dimensions) {
		checkState(aggregations.isEmpty());
		dimensionTypes.putAll(dimensions);
		return this;
	}

	public Cube withMeasure(String measureId, AggregateFunction aggregateFunction) {
		checkState(aggregations.isEmpty());
		measures.put(measureId, aggregateFunction);
		return this;
	}

	public Cube withMeasures(Map<String, AggregateFunction> aggregateFunctions) {
		checkState(aggregations.isEmpty());
		this.measures.putAll(aggregateFunctions);
		return this;
	}

	public Cube withComputedMeasure(String measureId, ComputedMeasure computedMeasure) {
		this.computedMeasures.put(measureId, computedMeasure);
		return this;
	}

	public Cube withComputedMeasures(Map<String, ComputedMeasure> computedMeasures) {
		this.computedMeasures.putAll(computedMeasures);
		return this;
	}

	public Cube withRelation(String dimensionId, String parentDimensionId) {
		childParentRelations.withRelation(dimensionId, parentDimensionId);
		return this;
	}

	public Cube withRelations(Map<String, String> childToParentRelations) {
		childParentRelations.withRelations(childToParentRelations);
		return this;
	}

	public static final class AggregationScheme {
		private final String id;
		private List<String> dimensions = new ArrayList<>();
		private List<String> measures = new ArrayList<>();
		private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();
		private List<String> partitioningKey = new ArrayList<>();
		private int aggregationChunkSize;
		private int sorterItemsInMemory;
		private int sorterBlockSize;
		private int overlappingChunksThreshold;
		private int maxIncrementalReloadPeriodMillis;

		public AggregationScheme(String id) {
			this.id = id;
		}

		public static AggregationScheme id(String id) {
			return new AggregationScheme(id);
		}

		public AggregationScheme withDimensions(Collection<String> dimensions) {
			this.dimensions.addAll(dimensions);
			return this;
		}

		public AggregationScheme withDimensions(String... dimensions) {
			return withDimensions(asList(dimensions));
		}

		public AggregationScheme withMeasures(Collection<String> measures) {
			this.measures.addAll(measures);
			return this;
		}

		public AggregationScheme withMeasures(String... measures) {
			return withMeasures(asList(measures));
		}

		public AggregationScheme withPredicate(AggregationPredicate predicate) {
			this.predicate = predicate;
			return this;
		}

		public AggregationScheme withPartitioningKey(List<String> partitioningKey) {
			this.partitioningKey.addAll(partitioningKey);
			return this;
		}

		public AggregationScheme withPartitioningKey(String... partitioningKey) {
			this.partitioningKey.addAll(asList(partitioningKey));
			return this;
		}

		public AggregationScheme withChunkSize(int chunkSize) {
			this.aggregationChunkSize = chunkSize;
			return this;
		}

		public AggregationScheme withSorterItemsInMemory(int sorterItemsInMemory) {
			this.sorterItemsInMemory = sorterItemsInMemory;
			return this;
		}

		public AggregationScheme withSorterBlockSize(int sorterBlockSize) {
			this.sorterBlockSize = sorterBlockSize;
			return this;
		}

		public AggregationScheme withOverlappingChunksThreshold(int overlappingChunksThreshold) {
			this.overlappingChunksThreshold = overlappingChunksThreshold;
			return this;
		}

		public AggregationScheme withMaxIncrementalReloadPeriodMillis(int maxIncrementalReloadPeriodMillis) {
			this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
			return this;
		}
	}

	public Cube withAggregation(AggregationScheme aggregationScheme) {
		addAggregation(aggregationScheme);
		return this;
	}

	private Cube addAggregation(final AggregationScheme scheme) {
		checkArgument(!aggregations.containsKey(scheme.id), "Aggregation '%s' is already defined", scheme.id);

		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorage() {
			@Override
			public void createChunkId(ResultCallback<Long> callback) {
				cubeMetadataStorage.createChunkId(Cube.this, scheme.id, callback);
			}

			@Override
			public void loadChunks(final int lastRevisionId, final ResultCallback<LoadedChunks> callback) {
				cubeMetadataStorage.loadChunks(Cube.this, lastRevisionId, newHashSet(scheme.id), new ForwardingResultCallback<CubeLoadedChunks>(callback) {
					@Override
					public void onResult(CubeLoadedChunks result) {
						loadChunksIntoAggregations(result, true);
						logger.info("Loading chunks for cube completed");
						callback.setResult(new LoadedChunks(lastRevisionId, EMPTY_LIST, EMPTY_LIST));
					}
				});
			}

			@Override
			public void startConsolidation(List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {
				cubeMetadataStorage.startConsolidation(Cube.this, scheme.id, chunksToConsolidate, callback);
			}

			@Override
			public void saveConsolidatedChunks(List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
				cubeMetadataStorage.saveConsolidatedChunks(Cube.this, scheme.id, originalChunks, consolidatedChunks, callback);
			}
		};
		Aggregation aggregation = Aggregation.create(eventloop, executorService, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage);

		for (String dimension : scheme.dimensions) {
			aggregation = aggregation.withKey(dimension, this.dimensionTypes.get(dimension));
		}
		aggregation = aggregation.withMeasures(this.measures).withPartitioningKey(scheme.partitioningKey);

		aggregation.setAggregationChunkSize(scheme.aggregationChunkSize != 0 ? scheme.aggregationChunkSize : this.aggregationChunkSize);
		aggregation.setSorterItemsInMemory(scheme.sorterItemsInMemory != 0 ? scheme.sorterItemsInMemory : this.sorterItemsInMemory);
		aggregation.setSorterBlockSize(scheme.sorterBlockSize != 0 ? scheme.sorterBlockSize : this.sorterBlockSize);
		aggregation.setMaxIncrementalReloadPeriodMillis(scheme.maxIncrementalReloadPeriodMillis != 0 ? scheme.maxIncrementalReloadPeriodMillis : this.maxIncrementalReloadPeriodMillis);
		aggregations.put(scheme.id, new AggregationInfo(aggregation, scheme.measures, scheme.predicate));
		logger.info("Added aggregation {} for id '{}'", aggregation, scheme.id);
		return this;
	}

	public Cube withAggregations(Collection<AggregationScheme> aggregations) {
		for (AggregationScheme aggregation : aggregations) {
			addAggregation(aggregation);
		}
		return this;
	}

	public Class<?> getAttributeType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getInternalDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		return null;
	}

	public Class<?> getMeasureInternalType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getInternalDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(storedMeasures);
		return null;
	}

	public Class<?> getMeasureType(String field) {
		return Primitives.wrap(getMeasureInternalType(field));
	}

	@Override
	public Map<String, Type> getAttributeTypes() {
		Map<String, Type> result = newLinkedHashMap();
		for (String dimension : dimensionTypes.keySet()) {
			result.put(dimension, dimensionTypes.get(dimension).getDataType());
		}
		for (String attribute : attributeTypes.keySet()) {
			result.put(attribute, attributeTypes.get(attribute));
		}
		return result;
	}

	@Override
	public Map<String, Type> getMeasureTypes() {
		Map<String, Type> result = newLinkedHashMap();
		for (String measure : measures.keySet()) {
			result.put(measure, measures.get(measure).getFieldType().getDataType());
		}
		for (String measure : computedMeasures.keySet()) {
			result.put(measure, computedMeasures.get(measure).getType(storedMeasures));
		}
		return result;
	}

	public Aggregation getAggregation(String aggregationId) {
		return aggregations.get(aggregationId).aggregation;
	}

	public void incrementLastRevisionId() {
		++lastRevisionId;
	}

	public int getLastRevisionId() {
		return lastRevisionId;
	}

	public void setLastRevisionId(int lastRevisionId) {
		this.lastRevisionId = lastRevisionId;
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> dimensions, List<String> measures,
	                                      ResultCallback<Multimap<String, AggregationChunk.NewChunk>> callback) {
		return consumer(inputClass, dimensions, measures, null, AggregationPredicates.alwaysTrue(), callback);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this cube.
	 * The returned {@link StreamConsumer} writes to {@link Aggregation}'s chosen using the specified dimensions, measures and input class.
	 *
	 * @param inputClass class of input records
	 * @param dimensions list of dimension names
	 * @param measures   list of measure names
	 * @param callback   callback which is called when records are committed
	 * @param <T>        data records type
	 * @return consumer for streaming data to cube
	 */
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> dimensions, List<String> measures,
	                                      Map<String, String> outputToInputFields, final AggregationPredicate predicate,
	                                      final ResultCallback<Multimap<String, AggregationChunk.NewChunk>> callback) {
		logger.info("Started consuming data. Dimensions: {}. Measures: {}", dimensions, measures);

		final StreamSplitter<T> streamSplitter = StreamSplitter.create(eventloop);
		final AsyncResultsTrackerMultimap<String, AggregationChunk.NewChunk> tracker = ofMultimap(callback);

		for (final Map.Entry<String, AggregationInfo> entry : aggregations.entrySet()) {
			final String aggregationId = entry.getKey();
			final Aggregation aggregation = entry.getValue().aggregation;
			if (!all(aggregation.getKeys(), in(dimensions)))
				continue;
			if (AggregationPredicates.and(entry.getValue().predicate, predicate).simplify() == alwaysFalse())
				continue;

			List<String> aggregationMeasures = Lists.newArrayList(Iterables.filter(entry.getValue().measures, in(measures)));
			if (aggregationMeasures.isEmpty())
				continue;

			tracker.startOperation();
			StreamConsumer<T> groupReducer = aggregation.consumer(inputClass, aggregationMeasures, outputToInputFields,
					new ResultCallback<List<AggregationChunk.NewChunk>>() {
						@Override
						public void onResult(List<AggregationChunk.NewChunk> chunks) {
							tracker.completeWithResults(aggregationId, chunks);
						}

						@Override
						public void onException(Exception e) {
							tracker.completeWithException(e);
						}
					});

			streamSplitter.newOutput().streamTo(groupReducer);
		}
		tracker.shutDown();

		return streamSplitter.getInput();
	}

	public Set<String> getCompatibleMeasures(Collection<String> dimensions, AggregationPredicate predicate) {
		Set<String> result = newLinkedHashSet();
		List<String> allDimensions = newArrayList(concat(dimensions, predicate.getDimensions()));

		for (AggregationInfo aggregationInfo : aggregations.values()) {
			if (!all(allDimensions, in(aggregationInfo.aggregation.getKeys())))
				continue;
			result.addAll(aggregationInfo.measures);
		}

		for (String computedMeasure : computedMeasures.keySet()) {
			if (result.containsAll(computedMeasures.get(computedMeasure).getMeasureDependencies())) {
				result.add(computedMeasure);
			}
		}

		return result;
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from cube for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param resultClass class of output records
	 * @return producer that streams query results
	 */
	public <T> StreamProducer<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate predicate,
	                                            Class<T> resultClass, DefiningClassLoader classLoader) throws QueryException {
		storedMeasures = newArrayList(storedMeasures);
		List<String> allDimensions = newArrayList(concat(dimensions, predicate.getDimensions()));

		final Map<AggregationInfo, Double> scores = new HashMap<>();
		List<AggregationInfo> compatibleAggregations = new ArrayList<>();
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			if (!all(allDimensions, in(aggregationInfo.aggregation.getKeys())))
				continue;
			List<String> compatibleMeasures = newArrayList(filter(storedMeasures, in(aggregationInfo.measures)));
			if (compatibleMeasures.isEmpty())
				continue;

			compatibleAggregations.add(aggregationInfo);
			AggregationQuery aggregationQuery = AggregationQuery.create(dimensions, compatibleMeasures, predicate);
			scores.put(aggregationInfo, aggregationInfo.aggregation.estimateCost(aggregationQuery));
		}

		Collections.sort(compatibleAggregations, new Comparator<AggregationInfo>() {
			@Override
			public int compare(AggregationInfo aggregation1, AggregationInfo aggregation2) {
				return Double.compare(scores.get(aggregation1), scores.get(aggregation2));
			}
		});

		Class resultKeyClass = createKeyClass(projectKeys(dimensionTypes, dimensions), classLoader);

		CubeQueryPlan queryPlan = CubeQueryPlan.create();

		StreamReducer<Comparable, T, Object> streamReducer = StreamReducer.create(eventloop, Ordering.natural());
		StreamProducer<T> queryResultProducer = streamReducer.getOutput();

		for (AggregationInfo aggregationInfo : compatibleAggregations) {
			List<String> compatibleMeasures = newArrayList(filter(storedMeasures, in(aggregationInfo.measures)));
			if (compatibleMeasures.isEmpty())
				continue;
			storedMeasures.removeAll(compatibleMeasures);

			queryPlan.addAggregationMeasures(aggregationInfo.aggregation, compatibleMeasures);

			Class aggregationClass = createRecordClass(projectKeys(dimensionTypes, aggregationInfo.aggregation.getKeys()),
					projectMeasures(measures, compatibleMeasures), classLoader);

			AggregationQuery aggregationQuery = AggregationQuery.create(dimensions, compatibleMeasures, predicate);
			StreamProducer aggregationProducer = aggregationInfo.aggregation.query(aggregationQuery, aggregationClass, classLoader);

			if (storedMeasures.isEmpty() && streamReducer.getInputs().isEmpty()) {
				/*
				If query is fulfilled from the single aggregation,
				just use mapper instead of reducer to copy requested fields.
				 */
				StreamMap.MapperProjection mapper = AggregationUtils.createMapper(aggregationClass, resultClass, dimensions,
						compatibleMeasures, classLoader);
				StreamMap streamMap = StreamMap.create(eventloop, mapper);
				aggregationProducer.streamTo(streamMap.getInput());
				queryResultProducer = streamMap.getOutput();
				queryPlan.setOptimizedAwayReducer(true);
				break;
			}

			Function keyFunction = AggregationUtils.createKeyFunction(aggregationClass, resultKeyClass, dimensions, classLoader);

			StreamReducers.Reducer reducer = aggregationInfo.aggregation.aggregationReducer(aggregationClass, resultClass,
					dimensions, compatibleMeasures, classLoader);

			StreamConsumer streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			aggregationProducer.streamTo(streamReducerInput);
		}

		if (!storedMeasures.isEmpty()) {
			logger.info("Could not build query plan for {}. Incomplete plan: {}", storedMeasures, queryPlan);
		} else {
			logger.info("Picked following aggregations ({}) for {}", queryPlan.getNumberOfAggregations(), queryPlan);
		}

		return queryResultProducer;
	}

	public boolean containsExcessiveNumberOfOverlappingChunks() {
		boolean excessive = false;

		for (AggregationInfo aggregationInfo : aggregations.values()) {
			int numberOfOverlappingChunks = aggregationInfo.aggregation.getNumberOfOverlappingChunks();
			if (numberOfOverlappingChunks > overlappingChunksThreshold) {
				logger.info("Aggregation {} contains {} overlapping chunks", aggregationInfo.aggregation, numberOfOverlappingChunks);
				excessive = true;
			}
		}

		return excessive;
	}

	public void loadChunks(final CompletionCallback callback) {
		final boolean incremental = eventloop.currentTimeMillis() - lastReloadTimestamp <= maxIncrementalReloadPeriodMillis;
		logger.info("Loading chunks for cube (incremental={})", incremental);
		int revisionId = incremental ? lastRevisionId : 0;

		cubeMetadataStorage.loadChunks(this, revisionId, aggregations.keySet(), new ResultCallback<CubeLoadedChunks>() {
			@Override
			public void onResult(CubeLoadedChunks result) {
				loadChunksIntoAggregations(result, incremental);
				logger.info("Loading chunks for cube completed");
				callback.setComplete();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Loading chunks for cube failed", e);
				callback.setException(e);
			}
		});
	}

	private void loadChunksIntoAggregations(CubeLoadedChunks result, boolean incremental) {
		this.lastRevisionId = result.lastRevisionId;
		this.lastReloadTimestamp = eventloop.currentTimeMillis();

		for (Map.Entry<String, AggregationInfo> entry : aggregations.entrySet()) {
			String aggregationId = entry.getKey();
			List<Long> consolidatedChunkIds = result.consolidatedChunkIds.get(aggregationId);
			List<AggregationChunk> newChunks = result.newChunks.get(aggregationId);
			LoadedChunks loadedChunks = new LoadedChunks(result.lastRevisionId,
					consolidatedChunkIds == null ? Collections.<Long>emptyList() : consolidatedChunkIds,
					newChunks == null ? Collections.<AggregationChunk>emptyList() : newChunks);
			Aggregation aggregation = entry.getValue().aggregation;
			aggregation.loadChunks(loadedChunks, incremental);
		}
	}

	public void consolidate(int maxChunksToConsolidate, ResultCallback<Boolean> callback) {
		logger.info("Launching consolidation");
		consolidate(maxChunksToConsolidate, false, new ArrayList<>(this.aggregations.values()).iterator(), callback);
	}

	private void consolidate(final int maxChunksToConsolidate, final boolean found,
	                         final Iterator<AggregationInfo> iterator, final ResultCallback<Boolean> callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				if (iterator.hasNext()) {
					final Aggregation aggregation = iterator.next().aggregation;
					aggregation.consolidateHotSegment(maxChunksToConsolidate, new ResultCallback<Boolean>() {
						@Override
						public void onResult(final Boolean hotSegmentConsolidated) {
							aggregation.consolidateMinKey(maxChunksToConsolidate, new ResultCallback<Boolean>() {
								@Override
								public void onResult(Boolean minKeyConsolidated) {
									consolidate(maxChunksToConsolidate,
											hotSegmentConsolidated || minKeyConsolidated || found, iterator, callback);
								}

								@Override
								public void onException(Exception e) {
									logger.error("Min key consolidation in aggregation '{}' failed", aggregation, e);
									consolidate(maxChunksToConsolidate, found, iterator, callback);
								}
							});
						}

						@Override
						public void onException(Exception e) {
							logger.error("Consolidating hot segment in aggregation '{}' failed", aggregation, e);
							consolidate(maxChunksToConsolidate, found, iterator, callback);
						}
					});
				} else {
					callback.setResult(found);
				}
			}
		});
	}

	public static class DrillDownsAndChains {
		public Set<QueryResult.Drilldown> drilldowns;
		public Set<List<String>> chains;

		public DrillDownsAndChains(Set<QueryResult.Drilldown> drilldowns, Set<List<String>> chains) {
			this.drilldowns = drilldowns;
			this.chains = chains;
		}
	}

	public DrillDownsAndChains getDrillDownsAndChains(Set<String> dimensions, Set<String> measures, AggregationPredicate predicate) {
		Set<QueryResult.Drilldown> drilldowns = newHashSet();
		Set<List<String>> chains = newHashSet();

		List<String> queryDimensions = newArrayList(concat(dimensions, predicate.getFullySpecifiedDimensions().keySet()));

		for (AggregationInfo aggregationInfo : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregationInfo.measures);

			if (!all(queryDimensions, in(aggregationInfo.aggregation.getKeys())) || !any(measures, in(aggregationMeasures)) ||
					AggregationPredicates.and(aggregationInfo.predicate, predicate).simplify() == alwaysFalse())
				continue;

			Set<String> availableMeasures = newHashSet();
			intersection(aggregationMeasures, measures).copyInto(availableMeasures);

			Iterable<String> filteredDimensions = filter(aggregationInfo.aggregation.getKeys(), not(in(queryDimensions)));
			Set<List<String>> filteredChains = childParentRelations.buildDrillDownChains(newHashSet(queryDimensions), filteredDimensions);
			Set<List<String>> allChains = childParentRelations.buildDrillDownChains(Sets.<String>newHashSet(), aggregationInfo.aggregation.getKeys());

			for (List<String> drillDownChain : filteredChains) {
				drilldowns.add(QueryResult.Drilldown.create(drillDownChain, availableMeasures));
			}

			chains.addAll(allChains);
		}

		Set<List<String>> longestChains = childParentRelations.buildLongestChains(chains);

		return new DrillDownsAndChains(drilldowns, longestChains);
	}

	public List<String> buildDrillDownChain(Set<String> usedDimensions, String dimension) {
		return childParentRelations.buildDrillDownChain(usedDimensions, dimension);
	}

	public List<String> buildDrillDownChain(String dimension) {
		return buildDrillDownChain(Sets.<String>newHashSet(), dimension);
	}

	public Map<String, List<AggregationMetadata.ConsolidationDebugInfo>> getConsolidationDebugInfo() {
		Map<String, List<AggregationMetadata.ConsolidationDebugInfo>> m = newHashMap();

		for (Map.Entry<String, AggregationInfo> aggregationEntry : aggregations.entrySet()) {
			m.put(aggregationEntry.getKey(), aggregationEntry.getValue().aggregation.getConsolidationDebugInfo());
		}

		return m;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	// visible for testing
	public void setLastReloadTimestamp(long lastReloadTimestamp) {
		this.lastReloadTimestamp = lastReloadTimestamp;
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			aggregationInfo.aggregation.setLastReloadTimestamp(lastReloadTimestamp);
		}
	}

	// region temp query() method
	@Override
	public void query(CubeQuery cubeQuery, final ResultCallback<QueryResult> resultCallback) throws QueryException {
		new RequestContext().execute(classLoader, cubeQuery, resultCallback);
	}
	// endregion

	// region helper classes
	public interface StringMatcher {
		boolean matches(Object obj, String searchString);
	}

	DefiningClassLoader getLocalClassLoader(ClassLoaderCacheKey key) {
		DefiningClassLoader classLoader = classLoaderCache.get(key);

		if (classLoader != null)
			return classLoader;

		DefiningClassLoader newClassLoader = DefiningClassLoader.create(Cube.this.getClassLoader());
		classLoaderCache.put(key, newClassLoader);
		return newClassLoader;
	}

	class RequestContext {
		DefiningClassLoader queryClassLoader;
		CubeQuery query;
		AggregationPredicate queryPredicate;
		AggregationPredicate queryHaving;
		Set<String> queryDimensions = newLinkedHashSet();
		Set<String> queryMeasures = newLinkedHashSet();
		Map<String, Object> fullySpecifiedDimensions;

		Set<String> resultAttributes = newLinkedHashSet();

		DrillDownsAndChains drillDownsAndChains;

		Set<String> resultMeasures = newLinkedHashSet();
		Set<String> resultStoredMeasures = newLinkedHashSet();
		Set<String> resultComputedMeasures = newLinkedHashSet();

		Class<?> resultClass;
		Predicate<?> havingPredicate;

		List<String> resultOrderings = newArrayList();
		Comparator<?> comparator;

		MeasuresFunction measuresFunction;
		TotalsFunction totalsFunction;

		RecordScheme recordScheme;
		RecordFunction recordFunction;

		void execute(DefiningClassLoader queryClassLoader, CubeQuery query, final ResultCallback<QueryResult> resultCallback) throws QueryException {
			this.queryClassLoader = queryClassLoader;
			this.query = query;
			queryPredicate = query.getPredicate().simplify();
			queryHaving = query.getHaving().simplify();
			fullySpecifiedDimensions = queryPredicate.getFullySpecifiedDimensions();

			prepareDimensions();
			prepareMeasures();
			drillDownsAndChains = getDrillDownsAndChains(queryDimensions, queryMeasures, queryPredicate);

			resultClass = createResultClass(resultAttributes, resultMeasures, Cube.this, queryClassLoader);
			measuresFunction = createMeasuresFunction();
			totalsFunction = createTotalsFunction();
			comparator = createComparator();
			havingPredicate = createHavingPredicate();
			recordScheme = createRecordScheme();
			recordFunction = createRecordFunction();

			StreamConsumers.ToList consumer = StreamConsumers.toList(eventloop);
			StreamProducer queryResultProducer = queryRawStream(newArrayList(queryDimensions), newArrayList(resultStoredMeasures), queryPredicate,
					resultClass, queryClassLoader);
			queryResultProducer.streamTo(consumer);

			consumer.setResultCallback(new ForwardingResultCallback<List<Object>>(resultCallback) {
				@Override
				protected void onResult(List<Object> results) {
					processResults(results, resultCallback);
				}
			});
		}

		void prepareDimensions() throws QueryException {
			for (String attribute : query.getAttributes()) {
				List<String> dimensions = newArrayList();
				if (dimensionTypes.containsKey(attribute)) {
					dimensions = buildDrillDownChain(attribute);
				} else if (attributes.containsKey(attribute)) {
					AttributeResolverInfo resolverInfo = attributes.get(attribute);
					for (String dimension : resolverInfo.dimensions) {
						dimensions.addAll(buildDrillDownChain(dimension));
					}
				} else
					throw new QueryException("Attribute not found: " + attribute);
				queryDimensions.addAll(dimensions);
				resultAttributes.addAll(dimensions);
				resultAttributes.add(attribute);
			}
		}

		void prepareMeasures() throws QueryException {
			queryMeasures.addAll(newArrayList(filter(query.getMeasures(), in(getCompatibleMeasures(queryDimensions, queryPredicate)))));
			for (String queryMeasure : queryMeasures) {
				if (measures.containsKey(queryMeasure)) {
					resultStoredMeasures.add(queryMeasure);
					resultMeasures.add(queryMeasure);
				} else if (computedMeasures.containsKey(queryMeasure)) {
					ComputedMeasure expression = computedMeasures.get(queryMeasure);
					Set<String> dependencies = expression.getMeasureDependencies();
					resultStoredMeasures.addAll(dependencies);
					resultComputedMeasures.add(queryMeasure);
					resultMeasures.addAll(dependencies);
					resultMeasures.add(queryMeasure);
				}
			}
		}

		RecordScheme createRecordScheme() {
			RecordScheme recordScheme = RecordScheme.create();
			for (String attribute : resultAttributes) {
				recordScheme = recordScheme.withField(attribute,
						attributeTypes.get(attribute));
			}
			for (String measure : queryMeasures) {
				recordScheme = recordScheme.withField(measure, getMeasureType(measure));
			}
			return recordScheme;
		}

		RecordFunction createRecordFunction() {
			ExpressionSequence copyAttributes = ExpressionSequence.create();
			ExpressionSequence copyMeasures = ExpressionSequence.create();

			for (String field : recordScheme.getFields()) {
				int fieldIndex = recordScheme.getFieldIndex(field);
				if (resultAttributes.contains(field)) {
					copyAttributes.add(call(arg(1), "put", value(fieldIndex),
							call(arg(2), "toValue", value(field),
									cast(field(cast(arg(0), resultClass), field.replace('.', '$')), Object.class))));
				}
				if (queryMeasures.contains(field)) {
					VarField fieldValue = field(cast(arg(0), resultClass), field);
					copyMeasures.add(call(arg(1), "put", value(fieldIndex),
							call(arg(2), "toValue", value(field),
									cast(measures.containsKey(field) ? measures.get(field).valueOfAccumulator(fieldValue) : fieldValue, Object.class))));
				}
			}

			return ClassBuilder.create(queryClassLoader, RecordFunction.class)
					.withMethod("copyAttributes", copyAttributes)
					.withMethod("copyMeasures", copyMeasures)
					.buildClassAndCreateNewInstance();
		}

		MeasuresFunction createMeasuresFunction() {
			ClassBuilder<MeasuresFunction> builder = ClassBuilder.create(queryClassLoader, MeasuresFunction.class);
			List<Expression> computeSequence = new ArrayList<>();

			for (String computedMeasure : resultComputedMeasures) {
				builder = builder.withField(computedMeasure, computedMeasures.get(computedMeasure).getType(storedMeasures));
				Expression record = cast(arg(0), resultClass);
				computeSequence.add(set(field(record, computedMeasure),
						computedMeasures.get(computedMeasure).getExpression(record, storedMeasures)));
			}
			return builder.withMethod("computeMeasures", sequence(computeSequence))
					.buildClassAndCreateNewInstance();
		}

		private Predicate createHavingPredicate() {
			return ClassBuilder.create(classLoader, Predicate.class)
					.withMethod("apply", boolean.class, singletonList(Object.class),
							queryHaving.createPredicateDef(cast(arg(0), resultClass), fieldConverters))
					.buildClassAndCreateNewInstance();
		}

		@SuppressWarnings("unchecked")
		Comparator<Object> createComparator() {
			ExpressionComparator comparator = ExpressionComparator.create();

			for (CubeQuery.Ordering ordering : query.getOrderings()) {
				String field = ordering.getField();

				if (fullySpecifiedDimensions.containsKey(field))
					continue;

				if (resultMeasures.contains(field) || resultAttributes.contains(field)) {
					String property = field.replace('.', '$');
					comparator = comparator.with(
							ordering.isAsc() ? leftField(resultClass, property) : rightField(resultClass, property),
							ordering.isAsc() ? rightField(resultClass, property) : leftField(resultClass, property),
							true);
					resultOrderings.add(field);
				}
			}

			return ClassBuilder.create(queryClassLoader, Comparator.class)
					.withMethod("compare", comparator)
					.buildClassAndCreateNewInstance();
		}

		@SuppressWarnings("unchecked")
		void processResults(List<Object> results, ResultCallback<QueryResult> callback) {
			Object totals;
			try {
				totals = resultClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}

			int totalCount = results.size();
			if (results.isEmpty()) {
				totalsFunction.zero(totals);
			} else {
				Iterator<Object> iterator = results.iterator();
				Object first = iterator.next();
				measuresFunction.computeMeasures(first);
				totalsFunction.init(totals, first);
				while (iterator.hasNext()) {
					Object next = iterator.next();
					measuresFunction.computeMeasures(next);
					totalsFunction.accumulate(totals, next);
				}
			}
			totalsFunction.computeMeasures(totals);

			for (AttributeResolverInfo resolverInfo : attributeResolvers) {
				List<String> attributes = new ArrayList<>(resolverInfo.attributes);
				attributes.retainAll(resultAttributes);
				if (!attributes.isEmpty()) {
					resolveAttributes(results, resolverInfo.resolver,
							resolverInfo.dimensions, resolverInfo.attributes,
							(Class) resultClass, queryClassLoader);
				}
			}

			results = newArrayList(Iterables.filter(results, (Predicate<Object>) havingPredicate));

			results = applyLimitAndOffset(results);

			List<Record> resultRecords = new ArrayList<>(results.size());
			for (Object result : results) {
				Record record = Record.create(recordScheme);
				recordFunction.copyAttributes(result, record, fieldConverters);
				recordFunction.copyMeasures(result, record, fieldConverters);
				resultRecords.add(record);
			}

			Record totalRecord = Record.create(recordScheme);
			recordFunction.copyMeasures(totals, totalRecord, fieldConverters);

			Map<String, Object> filterAttributes = resolveFilterAttributes();

			QueryResult result = QueryResult.create(recordScheme,
					resultRecords, totalRecord, totalCount,
					newArrayList(resultAttributes), newArrayList(queryMeasures), resultOrderings,
					drillDownsAndChains.drilldowns, drillDownsAndChains.chains, filterAttributes);
			callback.setResult(result);
		}

		Map<String, Object> resolveFilterAttributes() {
			Map<String, Object> result = newLinkedHashMap();
			for (AttributeResolverInfo resolverInfo : attributeResolvers) {
				if (fullySpecifiedDimensions.keySet().containsAll(resolverInfo.dimensions)) {
					Object[] key = new Object[resolverInfo.dimensions.size()];
					for (int i = 0; i < resolverInfo.dimensions.size(); i++) {
						String dimension = resolverInfo.dimensions.get(i);
						key[i] = fullySpecifiedDimensions.get(dimension);
					}
					Object[] attributes = resolverInfo.resolver.resolveAttributes(key);
					for (int i = 0; i < resolverInfo.attributes.size(); i++) {
						String attribute = resolverInfo.attributes.get(i);
						result.put(attribute, attributes[i]);
					}
				}
			}
			return result;
		}

		List applyLimitAndOffset(List results) {
			Integer offset = query.getOffset();
			Integer limit = query.getLimit();
			int start;
			int end;

			if (offset == null) {
				start = 0;
				offset = 0;
			} else if (offset >= results.size()) {
				return newArrayList();
			} else {
				start = offset;
			}

			if (limit == null) {
				end = results.size();
				limit = results.size();
			} else if (start + limit > results.size()) {
				end = results.size();
			} else {
				end = start + limit;
			}

			if (comparator != null) {
				int upperBound = offset + limit > results.size() ? results.size() : offset + limit;
				return Ordering.from(comparator).leastOf(results, upperBound).subList(offset, upperBound);
			}

			return results.subList(start, end);
		}

		TotalsFunction createTotalsFunction() {
			ClassBuilder<TotalsFunction> builder = ClassBuilder.create(queryClassLoader, TotalsFunction.class);

			ExpressionSequence zeroSequence = ExpressionSequence.create();
			ExpressionSequence initSequence = ExpressionSequence.create();
			ExpressionSequence accumulateSequence = ExpressionSequence.create();
			for (String field : resultStoredMeasures) {
				AggregateFunction measure = measures.get(field);
				zeroSequence.add(measure.zeroAccumulator(
						field(cast(arg(0), resultClass), field)));
				initSequence.add(measure.initAccumulatorWithAccumulator(
						field(cast(arg(0), resultClass), field),
						field(cast(arg(1), resultClass), field)));
				accumulateSequence.add(measure.reduce(
						field(cast(arg(0), resultClass), field),
						field(cast(arg(1), resultClass), field)));
			}
			builder = builder
					.withMethod("zero", zeroSequence)
					.withMethod("init", initSequence)
					.withMethod("accumulate", accumulateSequence);

			List<Expression> computeSequence = new ArrayList<>();
			for (String computedMeasure : resultComputedMeasures) {
				Expression result = cast(arg(0), resultClass);
				computeSequence.add(set(field(result, computedMeasure),
						computedMeasures.get(computedMeasure).getExpression(result, storedMeasures)));
			}

			return builder
					.withMethod("computeMeasures", sequence(computeSequence))
					.buildClassAndCreateNewInstance();
		}

	}
	// endregion

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("aggregations", aggregations)
				.add("lastRevisionId", lastRevisionId)
				.toString();
	}

	// jmx
	@JmxOperation
	public void flushBuffers() {
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			aggregationInfo.aggregation.flushBuffers();
		}
	}

	@JmxOperation
	public void flushBuffers(final String aggregationId) {
		AggregationInfo aggregationInfo = aggregations.get(aggregationId);
		if (aggregationInfo != null)
			aggregationInfo.aggregation.flushBuffers();
	}

	@JmxOperation
	public void setChunkSize(String aggregationId, int chunkSize) {
		AggregationInfo aggregationInfo = aggregations.get(aggregationId);
		if (aggregationInfo.aggregation != null)
			aggregationInfo.aggregation.setAggregationChunkSize(chunkSize);
	}

	@JmxAttribute
	public int getBuffersSize() {
		int size = 0;
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			size += aggregationInfo.aggregation.getBuffersSize();
		}
		return size;
	}

	@JmxAttribute
	public Map<String, Integer> getBuffersSizeByAggregation() {
		Map<String, Integer> map = new HashMap<>();
		for (Map.Entry<String, AggregationInfo> entry : aggregations.entrySet()) {
			map.put(entry.getKey(), entry.getValue().aggregation.getBuffersSize());
		}
		return map;
	}

	@JmxAttribute
	public void setMaxIncrementalReloadPeriodMillis(int maxIncrementalReloadPeriodMillis) {
		this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			aggregationInfo.aggregation.setMaxIncrementalReloadPeriodMillis(maxIncrementalReloadPeriodMillis);
		}
	}

	@JmxAttribute
	public int getMaxIncrementalReloadPeriodMillis() {
		return maxIncrementalReloadPeriodMillis;
	}

	@JmxAttribute
	public void setIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
		this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
		for (AggregationInfo aggregation : aggregations.values()) {
			aggregation.aggregation.setIgnoreChunkReadingExceptions(ignoreChunkReadingExceptions);
		}
	}

	@JmxAttribute
	public boolean getIgnoreChunkReadingExceptions() {
		return ignoreChunkReadingExceptions;
	}

	@JmxAttribute
	public void setChunkSize(int chunkSize) {
		this.aggregationChunkSize = chunkSize;
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			aggregationInfo.aggregation.setAggregationChunkSize(chunkSize);
		}
	}

	@JmxAttribute
	public int getChunkSize() {
		return aggregationChunkSize;
	}

	@JmxAttribute
	public void setSorterItemsInMemory(int sorterItemsInMemory) {
		this.sorterItemsInMemory = sorterItemsInMemory;
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			aggregationInfo.aggregation.setSorterItemsInMemory(sorterItemsInMemory);
		}
	}

	@JmxAttribute
	public int getSorterItemsInMemory() {
		return sorterItemsInMemory;
	}

	@JmxAttribute
	public void setSorterBlockSize(int sorterBlockSize) {
		this.sorterBlockSize = sorterBlockSize;
		for (AggregationInfo aggregationInfo : aggregations.values()) {
			aggregationInfo.aggregation.setSorterBlockSize(sorterBlockSize);
		}
	}

	@JmxAttribute
	public int getSorterBlockSize() {
		return sorterBlockSize;
	}

	@JmxAttribute
	public int getOverlappingChunksThreshold() {
		return overlappingChunksThreshold;
	}

	@JmxAttribute
	public void setOverlappingChunksThreshold(int overlappingChunksThreshold) {
		this.overlappingChunksThreshold = overlappingChunksThreshold;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

}