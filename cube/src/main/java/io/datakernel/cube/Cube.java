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

import io.datakernel.aggregation.*;
import io.datakernel.aggregation.AggregationState.ConsolidationDebugInfo;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.async.*;
import io.datakernel.codegen.*;
import io.datakernel.cube.asm.MeasuresFunction;
import io.datakernel.cube.asm.RecordFunction;
import io.datakernel.cube.asm.TotalsFunction;
import io.datakernel.cube.attributes.AttributeResolver;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.ValueStats;
import io.datakernel.logfs.ot.LogDataConsumer;
import io.datakernel.ot.OTState;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.*;
import io.datakernel.util.Initializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.aggregation.AggregationUtils.*;
import static io.datakernel.codegen.ExpressionComparator.leftField;
import static io.datakernel.codegen.ExpressionComparator.rightField;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Primitives.isWrapperType;
import static io.datakernel.cube.Utils.createResultClass;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.toList;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
@SuppressWarnings("unchecked")
public final class Cube implements ICube, OTState<CubeDiff>, Initializable<Cube>, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(Cube.class);

	public static final int DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD = 300;

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final AggregationChunkStorage aggregationChunkStorage;
	private Path temporarySortDir;

	private final Map<String, FieldType> fieldTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> dimensionTypes = new LinkedHashMap<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();
	private final Map<String, ComputedMeasure> computedMeasures = new LinkedHashMap<>();

	private static final class AttributeResolverContainer {
		private final List<String> attributes = new ArrayList<>();
		private final List<String> dimensions;
		private final AttributeResolver resolver;

		private AttributeResolverContainer(List<String> dimensions, AttributeResolver resolver) {
			this.dimensions = dimensions;
			this.resolver = resolver;
		}
	}

	private final List<AttributeResolverContainer> attributeResolvers = new ArrayList<>();
	private final Map<String, Class<?>> attributeTypes = new LinkedHashMap<>();
	private final Map<String, AttributeResolverContainer> attributes = new LinkedHashMap<>();

	private final Map<String, String> childParentRelations = new LinkedHashMap<>();

	// settings
	private int aggregationsChunkSize = Aggregation.DEFAULT_CHUNK_SIZE;
	private int aggregationsReducerBufferSize = Aggregation.DEFAULT_REDUCER_BUFFER_SIZE;
	private int aggregationsSorterItemsInMemory = Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private int aggregationsMaxChunksToConsolidate = Aggregation.DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE;
	private boolean aggregationsIgnoreChunkReadingExceptions = false;

	private int maxOverlappingChunksToProcessLogs = Cube.DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD;
	private Duration maxIncrementalReloadPeriod = Aggregation.DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD;

	static final class AggregationContainer {
		private final Aggregation aggregation;
		private final List<String> measures;
		private final AggregationPredicate predicate;

		private AggregationContainer(Aggregation aggregation, List<String> measures, AggregationPredicate predicate) {
			this.aggregation = aggregation;
			this.measures = measures;
			this.predicate = predicate;
		}

		@Override
		public String toString() {
			return aggregation.toString();
		}
	}

	// state
	private Map<String, AggregationContainer> aggregations = new LinkedHashMap<>();

	private CubeClassLoaderCache classLoaderCache;

	// JMX
	private final AggregationStats aggregationStats = new AggregationStats();
	private final ValueStats queryTimes = ValueStats.create(Duration.ofMinutes(10));
	private long queryErrors;
	private Throwable queryLastError;

	private Cube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	             AggregationChunkStorage aggregationChunkStorage) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.aggregationChunkStorage = aggregationChunkStorage;
	}

	public static Cube create(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                          AggregationChunkStorage aggregationChunkStorage) {
		return new Cube(eventloop, executorService, classLoader, aggregationChunkStorage);
	}

	// VisibleForTesting
	static Cube createUninitialized() {
		return new Cube(null, null, null, null);
	}

	public Cube withAttribute(String attribute, AttributeResolver resolver) {
		checkArgument(!this.attributes.containsKey(attribute), "Attribute %s has already been defined", attribute);
		int pos = attribute.indexOf('.');
		if (pos == -1)
			throw new IllegalArgumentException();
		String dimension = attribute.substring(0, pos);
		String attributeName = attribute.substring(pos + 1);
		checkArgument(resolver.getAttributeTypes().containsKey(attributeName), "Resolver does not support %s", attribute);
		checkArgument(!isWrapperType(resolver.getAttributeTypes().get(attributeName)), "Unsupported attribute type for %s", attribute);
		List<String> dimensions = getAllParents(dimension);
		checkArgument(dimensions.size() == resolver.getKeyTypes().length, "Parent dimensions: %s, key types: %s", dimensions, asList(resolver.getKeyTypes()));
		for (int i = 0; i < dimensions.size(); i++) {
			String d = dimensions.get(i);
			checkArgument(((Class<?>) dimensionTypes.get(d).getInternalDataType()).equals(resolver.getKeyTypes()[i]), "Dimension type mismatch for %s", d);
		}
		AttributeResolverContainer resolverContainer = null;
		for (AttributeResolverContainer r : attributeResolvers) {
			if (r.resolver == resolver) {
				resolverContainer = r;
				break;
			}
		}
		if (resolverContainer == null) {
			resolverContainer = new AttributeResolverContainer(dimensions, resolver);
			attributeResolvers.add(resolverContainer);
		}
		resolverContainer.attributes.add(attribute);
		attributes.put(attribute, resolverContainer);
		attributeTypes.put(attribute, resolver.getAttributeTypes().get(attributeName));
		return this;
	}

	public Cube withClassLoaderCache(CubeClassLoaderCache classLoaderCache) {
		this.classLoaderCache = classLoaderCache;
		return this;
	}

	public Cube withDimension(String dimensionId, FieldType type) {
		addDimension(dimensionId, type);
		return this;
	}

	public Cube withMeasure(String measureId, Measure measure) {
		addMeasure(measureId, measure);
		return this;
	}

	public Cube withComputedMeasure(String measureId, ComputedMeasure computedMeasure) {
		addComputedMeasure(measureId, computedMeasure);
		return this;
	}

	public Cube withRelation(String child, String parent) {
		addRelation(child, parent);
		return this;
	}

	public Cube withTemporarySortDir(Path temporarySortDir) {
		this.temporarySortDir = temporarySortDir;
		return this;
	}

	public static final class AggregationConfig implements Initializable<AggregationConfig> {
		private final String id;
		private List<String> dimensions = new ArrayList<>();
		private List<String> measures = new ArrayList<>();
		private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();
		private List<String> partitioningKey = new ArrayList<>();
		private int chunkSize;
		private int reducerBufferSize;
		private int sorterItemsInMemory;
		private int maxChunksToConsolidate;

		public AggregationConfig(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public static AggregationConfig id(String id) {
			return new AggregationConfig(id);
		}

		public AggregationConfig withDimensions(Collection<String> dimensions) {
			this.dimensions.addAll(dimensions);
			return this;
		}

		public AggregationConfig withDimensions(String... dimensions) {
			return withDimensions(asList(dimensions));
		}

		public AggregationConfig withMeasures(Collection<String> measures) {
			this.measures.addAll(measures);
			return this;
		}

		public AggregationConfig withMeasures(String... measures) {
			return withMeasures(asList(measures));
		}

		public AggregationConfig withPredicate(AggregationPredicate predicate) {
			this.predicate = predicate;
			return this;
		}

		public AggregationConfig withPartitioningKey(List<String> partitioningKey) {
			this.partitioningKey.addAll(partitioningKey);
			return this;
		}

		public AggregationConfig withPartitioningKey(String... partitioningKey) {
			this.partitioningKey.addAll(asList(partitioningKey));
			return this;
		}

		public AggregationConfig withChunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public AggregationConfig withReducerBufferSize(int reducerBufferSize) {
			this.reducerBufferSize = reducerBufferSize;
			return this;
		}

		public AggregationConfig withSorterItemsInMemory(int sorterItemsInMemory) {
			this.sorterItemsInMemory = sorterItemsInMemory;
			return this;
		}

		public AggregationConfig withMaxChunksToConsolidate(int maxChunksToConsolidate) {
			this.maxChunksToConsolidate = maxChunksToConsolidate;
			return this;
		}
	}

	public Cube withAggregation(AggregationConfig aggregationConfig) {
		addAggregation(aggregationConfig);
		return this;
	}

	private static <K, V> Stream<Entry<K, V>> filterKeys(Stream<Entry<K, V>> stream, Predicate<K> predicate) {
		return stream.filter(kvEntry -> predicate.test(kvEntry.getKey()));
	}

	public void addMeasure(String measureId, Measure measure) {
		checkState(aggregations.isEmpty());
		measures.put(measureId, measure);
		fieldTypes.put(measureId, measure.getFieldType());
	}

	public void addComputedMeasure(String measureId, ComputedMeasure computedMeasure) {
		checkState(aggregations.isEmpty());
		this.computedMeasures.put(measureId, computedMeasure);
	}

	public void addRelation(String child, String parent) {
		this.childParentRelations.put(child, parent);
	}

	public void addDimension(String dimensionId, FieldType type) {
		checkState(aggregations.isEmpty());
		dimensionTypes.put(dimensionId, type);
		fieldTypes.put(dimensionId, type);
	}

	public Cube addAggregation(AggregationConfig config) {
		checkArgument(!aggregations.containsKey(config.id), "Aggregation '%s' is already defined", config.id);

		Stream<Entry<String, FieldType>> measuresAsFields = measuresAsFields(Cube.this.measures).entrySet().stream();
		AggregationStructure structure = new AggregationStructure()
				.withKeys(streamToLinkedMap(config.dimensions.stream(), this.dimensionTypes::get))
				.withMeasures(projectMeasures(Cube.this.measures, config.measures))
				.withIgnoredMeasures(valuesToLinkedMap(filterKeys(measuresAsFields, s -> !config.measures.contains(s))))
				.withPartitioningKey(config.partitioningKey);

		Aggregation aggregation = Aggregation.create(eventloop, executorService, classLoader, aggregationChunkStorage, structure)
				.withTemporarySortDir(temporarySortDir)
				.withChunkSize(config.chunkSize != 0 ? config.chunkSize : aggregationsChunkSize)
				.withReducerBufferSize(config.reducerBufferSize != 0 ? config.reducerBufferSize : aggregationsReducerBufferSize)
				.withSorterItemsInMemory(config.sorterItemsInMemory != 0 ? config.sorterItemsInMemory : aggregationsSorterItemsInMemory)
				.withMaxChunksToConsolidate(config.maxChunksToConsolidate != 0 ? config.maxChunksToConsolidate : aggregationsMaxChunksToConsolidate)
				.withIgnoreChunkReadingExceptions(aggregationsIgnoreChunkReadingExceptions)
				.withStats(aggregationStats);

		aggregations.put(config.id, new AggregationContainer(aggregation, config.measures, config.predicate));
		logger.info("Added aggregation {} for id '{}'", aggregation, config.id);
		return this;
	}

	public Class<?> getAttributeInternalType(String attribute) {
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
			return computedMeasures.get(field).getType(measures);
		return null;
	}

	public Type getAttributeType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		return null;
	}

	public Type getMeasureType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(measures);
		return null;
	}

	@Override
	public Map<String, Type> getAttributeTypes() {
		Map<String, Type> result = new LinkedHashMap<>();
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
		Map<String, Type> result = new LinkedHashMap<>();
		for (String measure : measures.keySet()) {
			result.put(measure, measures.get(measure).getFieldType().getDataType());
		}
		for (String measure : computedMeasures.keySet()) {
			result.put(measure, computedMeasures.get(measure).getType(measures));
		}
		return result;
	}

	public Aggregation getAggregation(String aggregationId) {
		AggregationContainer aggregationContainer = aggregations.get(aggregationId);
		return (aggregationContainer == null) ? null : aggregationContainer.aggregation;
	}

	public Set<String> getAggregationIds() {
		return aggregations.keySet();
	}

	@Override
	public void init() {
		for (AggregationContainer container : aggregations.values()) {
			container.aggregation.getState().init();
		}
	}

	@Override
	public void apply(CubeDiff op) {
		for (String aggregationId : op.keySet()) {
			AggregationDiff aggregationDiff = op.get(aggregationId);
			aggregations.get(aggregationId).aggregation.getState().apply(aggregationDiff);
		}
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass) {
		return logStreamConsumer(inputClass, AggregationPredicates.alwaysTrue());
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass,
	                                                          AggregationPredicate predicate) {
		return logStreamConsumer(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), predicate);
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields) {
		return logStreamConsumer(inputClass, dimensionFields, measureFields, AggregationPredicates.alwaysTrue());
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields,
	                                                          AggregationPredicate predicate) {
		return () -> {
			StreamConsumerWithResult<T, CubeDiff> consumer = Cube.this.consume(inputClass, dimensionFields, measureFields, predicate);
			return consumer.withResult(consumer.getResult().thenApply(Collections::singletonList));
		};
	}

	public <T> StreamConsumerWithResult<T, CubeDiff> consume(Class<T> inputClass) {
		return consume(inputClass, AggregationPredicates.alwaysTrue());
	}

	public <T> StreamConsumerWithResult<T, CubeDiff> consume(Class<T> inputClass, AggregationPredicate predicate) {
		return consume(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), predicate);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this cube.
	 * The returned {@link StreamConsumer} writes to {@link Aggregation}'s chosen using the specified dimensions, measures and input class.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to cube
	 */
	public <T> StreamConsumerWithResult<T, CubeDiff> consume(Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields,
	                                                         AggregationPredicate dataPredicate) {
		logger.info("Started consuming data. Dimensions: {}. Measures: {}", dimensionFields.keySet(), measureFields.keySet());

		StreamSplitter<T> streamSplitter = StreamSplitter.create();

		StagesAccumulator<Map<String, AggregationDiff>> tracker = StagesAccumulator.create(new HashMap<>());
		Map<String, AggregationPredicate> compatibleAggregations = getCompatibleAggregationsForDataInput(dimensionFields, measureFields, dataPredicate);
		if (compatibleAggregations.size() == 0) {
			throw new IllegalArgumentException(format("No compatible aggregation for " +
					"dimensions fields: %s, measureFields: %s", dimensionFields, measureFields));
		}

		for (Entry<String, AggregationPredicate> aggregationToDataInputFilterPredicate : compatibleAggregations.entrySet()) {
			String aggregationId = aggregationToDataInputFilterPredicate.getKey();
			AggregationContainer aggregationContainer = aggregations.get(aggregationToDataInputFilterPredicate.getKey());
			Aggregation aggregation = aggregationContainer.aggregation;

			List<String> keys = aggregation.getKeys();
			Map<String, String> aggregationKeyFields = valuesToLinkedMap(filterKeys(dimensionFields.entrySet().stream(), keys::contains));
			Map<String, String> aggregationMeasureFields = valuesToLinkedMap(filterKeys(measureFields.entrySet().stream(), aggregationContainer.measures::contains));

			AggregationPredicate dataInputFilterPredicate = aggregationToDataInputFilterPredicate.getValue();
			StreamProducer<T> output = streamSplitter.newOutput();
			if (!dataInputFilterPredicate.equals(AggregationPredicates.alwaysTrue())) {
				Predicate<T> filterPredicate = createFilterPredicate(inputClass, dataInputFilterPredicate, getClassLoader(), fieldTypes);
				output = output.with(StreamFilter.create(filterPredicate));
			}
			Stage<AggregationDiff> consume = aggregation.consume(output, inputClass, aggregationKeyFields, aggregationMeasureFields);
			tracker.addStage(consume, (accumulator, diff) -> accumulator.put(aggregationId, diff));
		}
		return streamSplitter.getInput().withResult(tracker.get().thenApply(CubeDiff::of));
	}

	Map<String, AggregationPredicate> getCompatibleAggregationsForDataInput(Map<String, String> dimensionFields,
	                                                                        Map<String, String> measureFields,
	                                                                        AggregationPredicate predicate) {
		AggregationPredicate dataPredicate = predicate.simplify();
		Map<String, AggregationPredicate> aggregationToDataInputFilterPredicate = new HashMap<>();
		for (Entry<String, AggregationContainer> aggregationContainer : aggregations.entrySet()) {
			AggregationContainer container = aggregationContainer.getValue();
			Aggregation aggregation = container.aggregation;

			Set<String> dimensions = dimensionFields.keySet();
			if (!aggregation.getKeys().stream().allMatch(dimensions::contains)) continue;

			Map<String, String> aggregationMeasureFields = valuesToLinkedMap(filterKeys(measureFields.entrySet().stream(), container.measures::contains));
			if (aggregationMeasureFields.isEmpty()) continue;

			AggregationPredicate containerPredicate = container.predicate.simplify();

			AggregationPredicate intersection = AggregationPredicates.and(containerPredicate, dataPredicate).simplify();
			if (AggregationPredicates.alwaysFalse().equals(intersection)) continue;

			if (intersection.equals(containerPredicate)) {
				aggregationToDataInputFilterPredicate.put(aggregationContainer.getKey(), AggregationPredicates.alwaysTrue());
				continue;
			}

			aggregationToDataInputFilterPredicate.put(aggregationContainer.getKey(), containerPredicate);
		}
		return aggregationToDataInputFilterPredicate;
	}

	static Predicate createFilterPredicate(Class<?> inputClass,
	                                       AggregationPredicate predicate,
	                                       DefiningClassLoader classLoader,
	                                       Map<String, FieldType> keyTypes) {
		return ClassBuilder.create(classLoader, Predicate.class)
				.withMethod("test", boolean.class, singletonList(Object.class),
						predicate.createPredicateDef(cast(arg(0), inputClass), keyTypes))
				.buildClassAndCreateNewInstance();
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from cube for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param resultClass class of output records
	 * @return producer that streams query results
	 */
	public <T> StreamProducer<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
	                                            Class<T> resultClass) throws QueryException {
		return queryRawStream(dimensions, storedMeasures, where, resultClass, classLoader);
	}

	public <T> StreamProducer<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
	                                            Class<T> resultClass, DefiningClassLoader queryClassLoader) throws QueryException {

		List<AggregationContainer> compatibleAggregations = getCompatibleAggregationsForQuery(dimensions, storedMeasures, where);

		return queryRawStream(dimensions, storedMeasures, where, resultClass, queryClassLoader, compatibleAggregations);
	}

	private <T> StreamProducer<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
	                                             Class<T> resultClass, DefiningClassLoader queryClassLoader,
	                                             List<AggregationContainer> compatibleAggregations) throws QueryException {
		List<AggregationContainerWithScore> containerWithScores = new ArrayList<>();
		for (AggregationContainer compatibleAggregation : compatibleAggregations) {
			AggregationQuery aggregationQuery = AggregationQuery.create(dimensions, storedMeasures, where);
			double score = compatibleAggregation.aggregation.estimateCost(aggregationQuery);
			containerWithScores.add(new AggregationContainerWithScore(compatibleAggregation, score));
		}
		sort(containerWithScores);

		Class resultKeyClass = createKeyClass(projectKeys(dimensionTypes, dimensions), queryClassLoader);

		StreamReducer<Comparable, T, Object> streamReducer = StreamReducer.create(Comparable::compareTo);
		StreamProducer<T> queryResultProducer = streamReducer.getOutput();

		storedMeasures = new ArrayList<>(storedMeasures);
		for (AggregationContainerWithScore aggregationContainerWithScore : containerWithScores) {
			AggregationContainer aggregationContainer = aggregationContainerWithScore.aggregationContainer;
			List<String> compatibleMeasures = storedMeasures.stream().filter(aggregationContainer.measures::contains).collect(toList());
			if (compatibleMeasures.isEmpty())
				continue;
			storedMeasures.removeAll(compatibleMeasures);

			Class aggregationClass = createRecordClass(projectKeys(dimensionTypes, dimensions),
					measuresAsFields(projectMeasures(measures, compatibleMeasures)), queryClassLoader);

			AggregationQuery aggregationQuery = AggregationQuery.create(dimensions, compatibleMeasures, where);
			StreamProducer aggregationProducer = aggregationContainer.aggregation.query(aggregationQuery, aggregationClass, queryClassLoader);

			if (storedMeasures.isEmpty() && streamReducer.getInputs().isEmpty()) {
				/*
				If query is fulfilled from the single aggregation,
				just use mapper instead of reducer to copy requested fields.
				 */
				StreamMap.MapperProjection mapper = AggregationUtils.createMapper(aggregationClass, resultClass, dimensions,
						compatibleMeasures, queryClassLoader);
				queryResultProducer = aggregationProducer.with(StreamMap.create(mapper));
				break;
			}

			Function keyFunction = AggregationUtils.createKeyFunction(aggregationClass, resultKeyClass, dimensions, queryClassLoader);

			StreamReducers.Reducer reducer = aggregationContainer.aggregation.aggregationReducer(aggregationClass, resultClass,
					dimensions, compatibleMeasures, queryClassLoader);

			StreamConsumer streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			aggregationProducer.streamTo(streamReducerInput);
		}

		return queryResultProducer;
	}

	List<AggregationContainer> getCompatibleAggregationsForQuery(Collection<String> dimensions,
	                                                             Collection<String> storedMeasures,
	                                                             AggregationPredicate where) {
		where = where.simplify();
		List<String> allDimensions = Stream.concat(dimensions.stream(), where.getDimensions().stream()).collect(toList());

		List<AggregationContainer> compatibleAggregations = new ArrayList<>();
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			List<String> keys = aggregationContainer.aggregation.getKeys();
			if (!allDimensions.stream().allMatch(keys::contains)) continue;

			List<String> compatibleMeasures = storedMeasures.stream().filter(aggregationContainer.measures::contains).collect(toList());
			if (compatibleMeasures.isEmpty()) continue;
			AggregationPredicate intersection = AggregationPredicates.and(where, aggregationContainer.predicate).simplify();

			if (!intersection.equals(where)) continue;
			compatibleAggregations.add(aggregationContainer);
		}
		return compatibleAggregations;
	}

	static class AggregationContainerWithScore implements Comparable<AggregationContainerWithScore> {
		final AggregationContainer aggregationContainer;
		final double score;

		private AggregationContainerWithScore(AggregationContainer aggregationContainer, double score) {
			this.score = score;
			this.aggregationContainer = aggregationContainer;
		}

		@Override
		public int compareTo(AggregationContainerWithScore o) {
			int result;
			result = -Integer.compare(aggregationContainer.measures.size(), o.aggregationContainer.measures.size());
			if (result != 0) return result;
			result = Double.compare(score, o.score);
			if (result != 0) return result;
			result = Integer.compare(aggregationContainer.aggregation.getChunks(), o.aggregationContainer.aggregation.getChunks());
			if (result != 0) return result;
			result = Integer.compare(aggregationContainer.aggregation.getKeys().size(), o.aggregationContainer.aggregation.getKeys().size());
			return result;
		}
	}

	public boolean containsExcessiveNumberOfOverlappingChunks() {
		boolean excessive = false;

		for (AggregationContainer aggregationContainer : aggregations.values()) {
			int numberOfOverlappingChunks = aggregationContainer.aggregation.getNumberOfOverlappingChunks();
			if (numberOfOverlappingChunks > maxOverlappingChunksToProcessLogs) {
				logger.info("Aggregation {} contains {} overlapping chunks", aggregationContainer.aggregation, numberOfOverlappingChunks);
				excessive = true;
			}
		}

		return excessive;
	}

	public Stage<CubeDiff> consolidate(AsyncFunction<Aggregation, AggregationDiff> strategy) {
		logger.info("Launching consolidation");

		Map<String, AggregationDiff> map = new HashMap<>();
		List<AsyncCallable<?>> runnables = new ArrayList<>();

		aggregations.forEach((aggregationId, aggregationContainer) -> {
			Aggregation aggregation = aggregationContainer.aggregation;

			runnables.add((AsyncCallable<AggregationDiff>) () ->
					strategy.apply(aggregation)
							.whenResult(aggregationDiff -> {
								if (!aggregationDiff.isEmpty()) {
									map.put(aggregationId, aggregationDiff);
								}
							}));
		});

		return Stages.runSequence(runnables).thenApply($ -> CubeDiff.of(map));
	}

	private List<String> getAllParents(String dimension) {
		ArrayList<String> chain = new ArrayList<>();
		chain.add(dimension);
		String child = dimension;
		String parent;
		while ((parent = childParentRelations.get(child)) != null) {
			chain.add(0, parent);
			child = parent;
		}
		return chain;
	}

	public Set<Long> getAllChunks() {
		Set<Long> chunks = new HashSet<>();
		for (AggregationContainer container : aggregations.values()) {
			chunks.addAll(container.aggregation.getState().getChunks().keySet());
		}
		return chunks;
	}

	public Map<String, List<ConsolidationDebugInfo>> getConsolidationDebugInfo() {
		Map<String, List<ConsolidationDebugInfo>> m = new HashMap<>();
		for (Entry<String, AggregationContainer> aggregationEntry : aggregations.entrySet()) {
			m.put(aggregationEntry.getKey(), aggregationEntry.getValue().aggregation.getState().getConsolidationDebugInfo());
		}
		return m;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	// region temp query() method
	@Override
	public Stage<QueryResult> query(CubeQuery cubeQuery) throws QueryException {
		DefiningClassLoader queryClassLoader = getQueryClassLoader(new CubeClassLoaderCache.Key(
				new LinkedHashSet<>(cubeQuery.getAttributes()),
				new LinkedHashSet<>(cubeQuery.getMeasures()),
				cubeQuery.getWhere().getDimensions()));
		long queryStarted = eventloop.currentTimeMillis();
		return new RequestContext().execute(queryClassLoader, cubeQuery)
				.whenComplete((queryResult, throwable) -> {
					if (throwable == null) {
						queryTimes.recordValue((int) (eventloop.currentTimeMillis() - queryStarted));
					} else {
						queryErrors++;
						queryLastError = throwable;

						if (throwable instanceof NoSuchFileException) {
							logger.warn("Query failed because of NoSuchFileException. " + cubeQuery.toString(), throwable);
						}
					}
				});
	}
	// endregion

	private DefiningClassLoader getQueryClassLoader(CubeClassLoaderCache.Key key) {
		if (classLoaderCache == null)
			return this.classLoader;
		return classLoaderCache.getOrCreate(key);
	}

	private class RequestContext {
		DefiningClassLoader queryClassLoader;
		CubeQuery query;

		AggregationPredicate queryPredicate;
		AggregationPredicate queryHaving;

		List<AggregationContainer> compatibleAggregations = new ArrayList<>();
		Map<String, Object> fullySpecifiedDimensions;

		Set<String> resultDimensions = new LinkedHashSet<>();
		Set<String> resultAttributes = new LinkedHashSet<>();

		Set<String> resultMeasures = new LinkedHashSet<>();
		Set<String> resultStoredMeasures = new LinkedHashSet<>();
		Set<String> resultComputedMeasures = new LinkedHashSet<>();

		Class<?> resultClass;
		Predicate<?> havingPredicate;
		List<String> resultOrderings = new ArrayList<>();
		Comparator<?> comparator;
		MeasuresFunction measuresFunction;
		TotalsFunction totalsFunction;

		List<String> recordAttributes = new ArrayList<>();
		List<String> recordMeasures = new ArrayList<>();
		RecordScheme recordScheme;
		RecordFunction recordFunction;

		Stage<QueryResult> execute(DefiningClassLoader queryClassLoader, CubeQuery query) throws QueryException {
			this.queryClassLoader = queryClassLoader;
			this.query = query;

			queryPredicate = query.getWhere().simplify();
			queryHaving = query.getHaving().simplify();
			fullySpecifiedDimensions = queryPredicate.getFullySpecifiedDimensions();

			prepareDimensions();
			prepareMeasures();

			resultClass = createResultClass(resultAttributes, resultMeasures, Cube.this, queryClassLoader);
			recordScheme = createRecordScheme();
			if (query.getReportType() == ReportType.METADATA) {
				return Stage.of(QueryResult.createForMetadata(recordScheme, recordAttributes, recordMeasures));
			}
			measuresFunction = createMeasuresFunction();
			totalsFunction = createTotalsFunction();
			comparator = createComparator();
			havingPredicate = createHavingPredicate();
			recordFunction = createRecordFunction();

			return queryRawStream(new ArrayList<>(resultDimensions), new ArrayList<>(resultStoredMeasures),
					queryPredicate, (Class<Object>) resultClass, queryClassLoader, compatibleAggregations)
					.toList()
					.thenCompose(this::processResults);
		}

		void prepareDimensions() throws QueryException {
			for (String attribute : query.getAttributes()) {
//				if (resultAttributes.contains(attribute))
//					continue;
				recordAttributes.add(attribute);
				List<String> dimensions = new ArrayList<>();
				if (dimensionTypes.containsKey(attribute)) {
					dimensions = getAllParents(attribute);
				} else if (attributes.containsKey(attribute)) {
					AttributeResolverContainer resolverContainer = attributes.get(attribute);
					for (String dimension : resolverContainer.dimensions) {
						dimensions.addAll(getAllParents(dimension));
					}
				} else
					throw new QueryException("Attribute not found: " + attribute);
				resultDimensions.addAll(dimensions);
				resultAttributes.addAll(dimensions);
				resultAttributes.add(attribute);
			}
		}

		void prepareMeasures() throws QueryException {
			Set<String> queryStoredMeasures = new HashSet<>();
			for (String measure : query.getMeasures()) {
				if (computedMeasures.containsKey(measure)) {
					queryStoredMeasures.addAll(computedMeasures.get(measure).getMeasureDependencies());
				} else if (measures.containsKey(measure)) {
					queryStoredMeasures.add(measure);
				}
			}
			compatibleAggregations = getCompatibleAggregationsForQuery(resultDimensions, queryStoredMeasures, queryPredicate);

			Set<String> compatibleMeasures = new LinkedHashSet<>();
			for (AggregationContainer aggregationContainer : compatibleAggregations) {
				compatibleMeasures.addAll(aggregationContainer.measures);
			}
			for (String computedMeasure : computedMeasures.keySet()) {
				if (compatibleMeasures.containsAll(computedMeasures.get(computedMeasure).getMeasureDependencies())) {
					compatibleMeasures.add(computedMeasure);
				}
			}

			for (String queryMeasure : query.getMeasures()) {
				if (!compatibleMeasures.contains(queryMeasure) || recordMeasures.contains(queryMeasure))
					continue;
				recordMeasures.add(queryMeasure);
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
			for (String attribute : recordAttributes) {
				recordScheme.addField(attribute, getAttributeType(attribute));
			}
			for (String measure : recordMeasures) {
				recordScheme.addField(measure, getMeasureType(measure));
			}
			return recordScheme;
		}

		RecordFunction createRecordFunction() {
			ExpressionSequence copyAttributes = ExpressionSequence.create();
			ExpressionSequence copyMeasures = ExpressionSequence.create();

			for (String field : recordScheme.getFields()) {
				int fieldIndex = recordScheme.getFieldIndex(field);
				if (dimensionTypes.containsKey(field)) {
					copyAttributes.add(call(arg(1), "put", value(fieldIndex),
							cast(dimensionTypes.get(field).toValue(
									field(cast(arg(0), resultClass), field)), Object.class)));
				} else if (measures.containsKey(field)) {
					VarField fieldValue = field(cast(arg(0), resultClass), field);
					copyMeasures.add(call(arg(1), "put", value(fieldIndex),
							cast(measures.get(field).getFieldType().toValue(
									measures.get(field).valueOfAccumulator(fieldValue)), Object.class)));
				} else {
					copyMeasures.add(call(arg(1), "put", value(fieldIndex),
							cast(field(cast(arg(0), resultClass), field.replace('.', '$')), Object.class)));
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
				builder = builder.withField(computedMeasure, computedMeasures.get(computedMeasure).getType(measures));
				Expression record = cast(arg(0), resultClass);
				computeSequence.add(set(field(record, computedMeasure),
						computedMeasures.get(computedMeasure).getExpression(record, measures)));
			}
			return builder.withMethod("computeMeasures", sequence(computeSequence))
					.buildClassAndCreateNewInstance();
		}

		private Predicate createHavingPredicate() {
			if (queryHaving == AggregationPredicates.alwaysTrue()) return o -> true;
			if (queryHaving == AggregationPredicates.alwaysFalse()) return o -> false;

			return ClassBuilder.create(queryClassLoader, Predicate.class)
					.withMethod("test", boolean.class, singletonList(Object.class),
							queryHaving.createPredicateDef(cast(arg(0), resultClass), fieldTypes))
					.buildClassAndCreateNewInstance();
		}

		@SuppressWarnings("unchecked")
		Comparator<Object> createComparator() {
			if (query.getOrderings().isEmpty())
				return (o1, o2) -> 0;

			ExpressionComparator comparator = ExpressionComparator.create();

			for (CubeQuery.Ordering ordering : query.getOrderings()) {
				String field = ordering.getField();

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
		Stage<QueryResult> processResults(List<Object> results) {
			Object totals;
			try {
				totals = resultClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}

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
				totalsFunction.computeMeasures(totals);
			}

			Record totalRecord = Record.create(recordScheme);
			recordFunction.copyMeasures(totals, totalRecord);

			List<Stage<Void>> tasks = new ArrayList<>();
			Map<String, Object> filterAttributes = new LinkedHashMap<>();
			for (AttributeResolverContainer resolverContainer : attributeResolvers) {
				List<String> attributes = new ArrayList<>(resolverContainer.attributes);
				attributes.retainAll(resultAttributes);
				if (!attributes.isEmpty()) {
					tasks.add(Utils.resolveAttributes(results, resolverContainer.resolver,
							resolverContainer.dimensions, attributes,
							fullySpecifiedDimensions, (Class) resultClass, queryClassLoader));
				}
			}

			for (AttributeResolverContainer resolverContainer : attributeResolvers) {
				if (fullySpecifiedDimensions.keySet().containsAll(resolverContainer.dimensions)) {
					tasks.add(resolveSpecifiedDimensions(resolverContainer, filterAttributes));
				}
			}
			return Stages.all(tasks)
					.thenApply($ -> processResults2(results, totals, filterAttributes));
		}

		QueryResult processResults2(List<Object> results, Object totals, Map<String, Object> filterAttributes) {
			results = results.stream().filter(((Predicate<Object>) havingPredicate)).collect(toList());

			int totalCount = results.size();

			results = applyLimitAndOffset(results);

			List<Record> resultRecords = new ArrayList<>(results.size());
			for (Object result : results) {
				Record record = Record.create(recordScheme);
				recordFunction.copyAttributes(result, record);
				recordFunction.copyMeasures(result, record);
				resultRecords.add(record);
			}

			if (query.getReportType() == ReportType.DATA) {
				return QueryResult.createForData(recordScheme,
						resultRecords,
						recordAttributes,
						recordMeasures,
						resultOrderings,
						filterAttributes);
			}

			if (query.getReportType() == ReportType.DATA_WITH_TOTALS) {
				Record totalRecord = Record.create(recordScheme);
				recordFunction.copyMeasures(totals, totalRecord);
				return QueryResult.createForDataWithTotals(recordScheme,
						resultRecords,
						totalRecord,
						totalCount,
						recordAttributes,
						recordMeasures,
						resultOrderings,
						filterAttributes);
			}

			throw new AssertionError();
		}

		private Stage<Void> resolveSpecifiedDimensions(AttributeResolverContainer resolverContainer,
		                                               Map<String, Object> result) {
			Object[] key = new Object[resolverContainer.dimensions.size()];
			for (int i = 0; i < resolverContainer.dimensions.size(); i++) {
				String dimension = resolverContainer.dimensions.get(i);
				key[i] = fullySpecifiedDimensions.get(dimension);
			}
			Object[] attributesPlaceholder = new Object[1];

			return resolverContainer.resolver.resolveAttributes(singletonList(key),
					result1 -> (Object[]) result1,
					(result12, attributes) -> attributesPlaceholder[0] = attributes)
					.thenRun(() -> {
						for (int i = 0; i < resolverContainer.attributes.size(); i++) {
							String attribute = resolverContainer.attributes.get(i);
							result.put(attribute, attributesPlaceholder[0] != null ? ((Object[]) attributesPlaceholder[0])[i] : null);
						}
					});
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
				return new ArrayList();
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
				return ((List<Object>) results).stream()
						.sorted(((Comparator<Object>) comparator))
						.skip(offset)
						.limit(limit)
						.collect(Collectors.toList());
			}

			return results.subList(start, end);
		}

		TotalsFunction createTotalsFunction() {
			ExpressionSequence zeroSequence = ExpressionSequence.create();
			ExpressionSequence initSequence = ExpressionSequence.create();
			ExpressionSequence accumulateSequence = ExpressionSequence.create();
			for (String field : resultStoredMeasures) {
				Measure measure = measures.get(field);
				zeroSequence.add(measure.zeroAccumulator(
						field(cast(arg(0), resultClass), field)));
				initSequence.add(measure.initAccumulatorWithAccumulator(
						field(cast(arg(0), resultClass), field),
						field(cast(arg(1), resultClass), field)));
				accumulateSequence.add(measure.reduce(
						field(cast(arg(0), resultClass), field),
						field(cast(arg(1), resultClass), field)));
			}

			ExpressionSequence computeSequence = ExpressionSequence.create();
			for (String computedMeasure : resultComputedMeasures) {
				Expression result = cast(arg(0), resultClass);
				computeSequence.add(set(field(result, computedMeasure),
						computedMeasures.get(computedMeasure).getExpression(result, measures)));
			}
			return ClassBuilder.create(queryClassLoader, TotalsFunction.class)
					.withMethod("zero", zeroSequence)
					.withMethod("init", initSequence)
					.withMethod("accumulate", accumulateSequence)
					.withMethod("computeMeasures", computeSequence)
					.buildClassAndCreateNewInstance();
		}

	}
	// endregion

	@Override
	public String toString() {
		return "Cube{" +
				"aggregations=" + aggregations +
				'}';
	}

	// jmx
	@JmxAttribute
	public int getAggregationsChunkSize() {
		return aggregationsChunkSize;
	}

	@JmxAttribute
	public void setAggregationsChunkSize(int aggregationsChunkSize) {
		this.aggregationsChunkSize = aggregationsChunkSize;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setChunkSize(aggregationsChunkSize);
		}
	}

	public Cube withAggregationsChunkSize(int aggregationsChunkSize) {
		this.aggregationsChunkSize = aggregationsChunkSize;
		return this;
	}

	public Cube withAggregationsReducerBufferSize(int aggregationsReducerBufferSize) {
		this.aggregationsReducerBufferSize = aggregationsReducerBufferSize;
		return this;
	}

	@JmxAttribute
	public int getAggregationsSorterItemsInMemory() {
		return aggregationsSorterItemsInMemory;
	}

	@JmxAttribute
	public void setAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
		this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setSorterItemsInMemory(aggregationsSorterItemsInMemory);
		}
	}

	public Cube withAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
		this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
		return this;
	}

	@JmxAttribute
	public int getAggregationsMaxChunksToConsolidate() {
		return aggregationsMaxChunksToConsolidate;
	}

	@JmxAttribute
	public void setAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
		this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setMaxChunksToConsolidate(aggregationsMaxChunksToConsolidate);
		}
	}

	public Cube withAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
		this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
		return this;
	}

	@JmxAttribute
	public boolean getAggregationsIgnoreChunkReadingExceptions() {
		return aggregationsIgnoreChunkReadingExceptions;
	}

	@JmxAttribute
	public void setAggregationsIgnoreChunkReadingExceptions(boolean aggregationsIgnoreChunkReadingExceptions) {
		this.aggregationsIgnoreChunkReadingExceptions = aggregationsIgnoreChunkReadingExceptions;
		for (AggregationContainer aggregation : aggregations.values()) {
			aggregation.aggregation.setIgnoreChunkReadingExceptions(aggregationsIgnoreChunkReadingExceptions);
		}
	}

	public Cube withAggregationsIgnoreChunkReadingExceptions(boolean aggregationsIgnoreChunkReadingExceptions) {
		this.aggregationsIgnoreChunkReadingExceptions = aggregationsIgnoreChunkReadingExceptions;
		return this;
	}

	@JmxAttribute
	public int getMaxOverlappingChunksToProcessLogs() {
		return maxOverlappingChunksToProcessLogs;
	}

	@JmxAttribute
	public void setMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
		this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
	}

	public Cube withMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
		this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
		return this;
	}

	@JmxAttribute
	public long getMaxIncrementalReloadPeriod() {
		return maxIncrementalReloadPeriod.toMillis();
	}

	@JmxAttribute
	public void setMaxIncrementalReloadPeriod(long maxIncrementalReloadPeriod) {
		this.maxIncrementalReloadPeriod = Duration.ofMillis(maxIncrementalReloadPeriod);
	}

	public Cube withMaxIncrementalReloadPeriod(Duration maxIncrementalReloadPeriod) {
		this.maxIncrementalReloadPeriod = maxIncrementalReloadPeriod;
		return this;
	}

	@JmxAttribute
	public ValueStats getQueryTimes() {
		return queryTimes;
	}

	@JmxAttribute
	public long getQueryErrors() {
		return queryErrors;
	}

	@JmxAttribute
	public Throwable getQueryLastError() {
		return queryLastError;
	}

	@JmxAttribute
	public AggregationStats getAggregationStats() {
		return aggregationStats;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

}