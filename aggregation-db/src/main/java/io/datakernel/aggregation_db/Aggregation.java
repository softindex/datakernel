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

package io.datakernel.aggregation_db;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.aggregation_db.AggregationMetadataStorage.LoadedChunks;
import io.datakernel.aggregation_db.processor.ProcessorFactory;
import io.datakernel.aggregation_db.util.BiPredicate;
import io.datakernel.aggregation_db.util.Predicates;
import io.datakernel.async.*;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.PredicateDefAnd;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.ErrorIgnoringTransformer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.*;
import static io.datakernel.aggregation_db.AggregationStructure.createKeyComparator;
import static io.datakernel.aggregation_db.AggregationStructure.createKeyFunction;
import static io.datakernel.aggregation_db.AggregationStructure.createMapper;
import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.singletonList;

/**
 * Represents an aggregation, which aggregates data using custom reducer and preaggregator.
 * Provides methods for loading and querying data.
 */
@SuppressWarnings("unchecked")
public class Aggregation implements AggregationOperationTracker {
	private static final Logger logger = LoggerFactory.getLogger(Aggregation.class);
	private static final Joiner JOINER = Joiner.on(", ");

	public static final int DEFAULT_AGGREGATION_CHUNK_SIZE = 1_000_000;
	public static final int DEFAULT_SORTER_ITEMS_IN_MEMORY = 1_000_000;
	public static final int DEFAULT_SORTER_BLOCK_SIZE = 256 * 1024;
	public static final int DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS = 10 * 60 * 1000; // 10 minutes

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final AggregationMetadataStorage metadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final AggregationMetadata aggregationMetadata;
	private final List<String> partitioningKey;
	private final AggregationStructure structure;
	private final ProcessorFactory processorFactory;

	// settings
	private int aggregationChunkSize;
	private int sorterItemsInMemory;
	private int sorterBlockSize;
	private int maxIncrementalReloadPeriodMillis;
	private boolean ignoreChunkReadingExceptions;

	// state
	private final Map<Long, AggregationChunk> chunks = new LinkedHashMap<>();
	private int lastRevisionId;
	private long lastReloadTimestamp;
	private ListenableCompletionCallback loadChunksCallback;

	// jmx
	private final List<AggregationGroupReducer> activeGroupReducers = new ArrayList<>();
	private final List<AggregationChunker> activeChunkers = new ArrayList<>();

	/**
	 * Instantiates an aggregation with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages,
	 * and uses the specified parameters.
	 *
	 * @param eventloop               event loop, in which the aggregation is to run
	 * @param classLoader             class loader for defining dynamic classes
	 * @param metadataStorage         storage for aggregations metadata
	 * @param aggregationChunkStorage storage for data chunks
	 * @param aggregationMetadata     metadata of the aggregation
	 * @param structure               structure of an aggregation
	 * @param aggregationChunkSize    maximum size of aggregation chunk
	 * @param sorterItemsInMemory     maximum number of records that can stay in memory while sorting
	 */
	public Aggregation(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                   AggregationMetadataStorage metadataStorage, AggregationChunkStorage aggregationChunkStorage,
	                   AggregationMetadata aggregationMetadata, AggregationStructure structure,
	                   int aggregationChunkSize, int sorterItemsInMemory, int sorterBlockSize, int maxIncrementalReloadPeriodMillis,
	                   List<String> partitioningKey) {
		checkArgument(partitioningKey == null || (aggregationMetadata.getKeys().containsAll(partitioningKey) &&
				isPrefix(partitioningKey, aggregationMetadata.getKeys())));
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.metadataStorage = metadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.aggregationMetadata = aggregationMetadata;
		this.partitioningKey = partitioningKey;
		this.aggregationChunkSize = aggregationChunkSize;
		this.sorterItemsInMemory = sorterItemsInMemory;
		this.sorterBlockSize = sorterBlockSize;
		this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
		this.structure = structure;
		this.processorFactory = new ProcessorFactory(structure);
	}

	/**
	 * Instantiates an aggregation with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages.
	 * Maximum size of chunk is 1,000,000 bytes.
	 * No more than 1,000,000 records stay in memory while sorting.
	 * Maximum duration of consolidation attempt is 30 minutes.
	 * Consolidated chunks become available for removal in 10 minutes from consolidation.
	 *
	 * @param eventloop               event loop, in which the aggregation is to run
	 * @param classLoader             class loader for defining dynamic classes
	 * @param metadataStorage         storage for persisting aggregation metadata
	 * @param aggregationChunkStorage storage for data chunks
	 * @param aggregationMetadata     metadata of the aggregation
	 * @param structure               structure of an aggregation
	 */
	public Aggregation(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                   AggregationMetadataStorage metadataStorage, AggregationChunkStorage aggregationChunkStorage,
	                   AggregationMetadata aggregationMetadata, AggregationStructure structure) {
		this(eventloop, executorService, classLoader, metadataStorage, aggregationChunkStorage, aggregationMetadata,
				structure, DEFAULT_AGGREGATION_CHUNK_SIZE, DEFAULT_SORTER_ITEMS_IN_MEMORY, DEFAULT_SORTER_BLOCK_SIZE,
				DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS, null);
	}

	public List<String> getAggregationFieldsForConsumer(List<String> fields) {
		return newArrayList(filter(getFields(), in(fields)));
	}

	public List<String> getAggregationFieldsForQuery(List<String> queryFields) {
		return newArrayList(filter(queryFields, in(getFields())));
	}

	public boolean allKeysIn(List<String> requestedKeys) {
		return aggregationMetadata.allKeysIn(requestedKeys);
	}

	public boolean containsKeys(List<String> requestedKeys) {
		return aggregationMetadata.containsKeys(requestedKeys);
	}

	public Map<Long, AggregationChunk> getChunks() {
		return Collections.unmodifiableMap(chunks);
	}

	public AggregationMetadata getAggregationMetadata() {
		return aggregationMetadata;
	}

	public void addToIndex(AggregationChunk chunk) {
		aggregationMetadata.addToIndex(chunk);
		chunks.put(chunk.getChunkId(), chunk);
	}

	public void removeFromIndex(AggregationChunk chunk) {
		aggregationMetadata.removeFromIndex(chunk);
		chunks.remove(chunk.getChunkId());
	}

	public void clearIndex() {
		aggregationMetadata.clearIndex();
		chunks.clear();
	}

	public boolean hasPredicates() {
		return aggregationMetadata.hasPredicates();
	}

	public boolean matchQueryPredicates(AggregationQuery.Predicates predicates) {
		return aggregationMetadata.matchQueryPredicates(predicates);
	}

	public AggregationFilteringResult applyQueryPredicates(AggregationQuery query, AggregationStructure structure) {
		return aggregationMetadata.applyQueryPredicates(query, structure);
	}

	public List<String> getKeys() {
		return aggregationMetadata.getKeys();
	}

	public int getNumberOfPredicates() {
		return aggregationMetadata.getNumberOfPredicates();
	}

	public List<String> getFields() {
		return aggregationMetadata.getFields();
	}

	public double getCost(AggregationQuery query) {
		return aggregationMetadata.getCost(query);
	}

	public AggregationQuery.Predicates getAggregationPredicates() {
		return aggregationMetadata.getAggregationPredicates();
	}

	public AggregationStructure getStructure() {
		return structure;
	}

	public int getLastRevisionId() {
		return lastRevisionId;
	}

	public void setLastRevisionId(int lastRevisionId) {
		this.lastRevisionId = lastRevisionId;
	}

	public void incrementLastRevisionId() {
		++lastRevisionId;
	}

	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                                 List<String> fields, DefiningClassLoader classLoader) {
		return processorFactory.aggregationReducer(inputClass, outputClass, keys, fields, classLoader);
	}

	private BiPredicate createPartitionPredicate(Class recordClass) {
		if (partitioningKey == null)
			return Predicates.alwaysTrue();

		AsmBuilder<BiPredicate> builder = new AsmBuilder<>(classLoader, BiPredicate.class);
		PredicateDefAnd predicate = and();
		for (String keyComponent : partitioningKey) {
			predicate.add(cmpEq(
					getter(cast(arg(0), recordClass), keyComponent),
					getter(cast(arg(1), recordClass), keyComponent)));
		}
		builder.method("test", predicate);
		return builder.newInstance();
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to aggregation
	 */
	public <T> StreamConsumer<T> consumer(Class<T> inputClass) {
		return consumer(inputClass, (List) null, null, new AggregationCommitCallback(this));
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> fields,
	                                      Map<String, String> outputToInputFields) {
		return consumer(inputClass, fields, outputToInputFields, new AggregationCommitCallback(this));
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass          class of input records
	 * @param fields              list of output field names
	 * @param outputToInputFields mapping from output to input fields
	 * @param chunksCallback      callback which is called when chunks are created
	 * @param <T>                 data records type
	 * @return consumer for streaming data to aggregation
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> fields, Map<String, String> outputToInputFields,
	                                      ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		logger.info("Started consuming data in aggregation {}. Fields: {}. Output to input fields mapping: {}",
				this, fields, outputToInputFields);

		List<String> outputFields = fields == null ? getFields() : fields;

		Class<?> keyClass = structure.createKeyClass(getKeys(), classLoader);
		Class<?> aggregationClass = structure.createRecordClass(getKeys(), outputFields, classLoader);

		Function<T, Comparable<?>> keyFunction = createKeyFunction(inputClass, keyClass, getKeys(), classLoader);

		Aggregate aggregate = processorFactory.createPreaggregator(inputClass, aggregationClass, getKeys(), fields,
				outputToInputFields, classLoader);

		return new AggregationGroupReducer<>(eventloop, aggregationChunkStorage, this, metadataStorage, getKeys(),
				outputFields, aggregationClass, createPartitionPredicate(aggregationClass), keyFunction, aggregate,
				aggregationChunkSize, classLoader, chunksCallback);
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from aggregation for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param query       query
	 * @param outputClass class of output records
	 * @return producer that streams query results
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamProducer<T> query(AggregationQuery query, List<String> fields, Class<T> outputClass,
	                                   DefiningClassLoader classLoader) {
		List<String> resultKeys = query.getResultKeys();

		List<String> aggregationFields = getAggregationFieldsForQuery(fields);

		List<AggregationChunk> allChunks = aggregationMetadata.findChunks(structure, query.getPredicates(), aggregationFields);

		AggregationQueryPlan queryPlan = new AggregationQueryPlan();

		StreamProducer streamProducer = consolidatedProducer(query.getResultKeys(), query.getRequestedKeys(),
				aggregationFields, outputClass, query.getPredicates(), allChunks, queryPlan, null, classLoader);

		StreamProducer queryResultProducer = streamProducer;

		List<AggregationQuery.PredicateNotEquals> notEqualsPredicates = getNotEqualsPredicates(query.getPredicates());

		for (String key : resultKeys) {
			Object restrictedValue = structure.getKeyType(key).getRestrictedValue();
			if (restrictedValue != null)
				notEqualsPredicates.add(new AggregationQuery.PredicateNotEquals(key, restrictedValue));
		}

		if (!notEqualsPredicates.isEmpty()) {
			StreamFilter streamFilter = new StreamFilter<>(eventloop,
					createNotEqualsPredicate(outputClass, notEqualsPredicates, classLoader));
			streamProducer.streamTo(streamFilter.getInput());
			queryResultProducer = streamFilter.getOutput();
			queryPlan.setPostFiltering(true);
		}

		logger.info("Query plan for {} in aggregation {}: {}", query, this, queryPlan);

		return queryResultProducer;
	}

	public <T> StreamProducer<T> query(AggregationQuery query, Class<T> outputClass) {
		return query(query, query.getResultFields(), outputClass, classLoader);
	}

	private <T> StreamProducer<T> getOrderedStream(StreamProducer<T> rawStream, Class<T> resultClass,
	                                               List<String> keys, List<String> fields, DefiningClassLoader classLoader) {
		Comparator keyComparator = createKeyComparator(resultClass, keys, classLoader);
		Path path = Paths.get("sorterStorage", "%d.part");
		BufferSerializer bufferSerializer = structure.createBufferSerializer(resultClass, getKeys(), fields, classLoader);
		StreamMergeSorterStorage sorterStorage = new StreamMergeSorterStorageImpl(eventloop, executorService,
				bufferSerializer, path, sorterBlockSize);
		StreamSorter sorter = new StreamSorter(eventloop, sorterStorage, Functions.identity(), keyComparator, false,
				sorterItemsInMemory);
		rawStream.streamTo(sorter.getInput());
		return sorter.getOutput();
	}

	private List<AggregationQuery.PredicateNotEquals> getNotEqualsPredicates(AggregationQuery.Predicates queryPredicates) {
		List<AggregationQuery.PredicateNotEquals> notEqualsPredicates = newArrayList();

		for (AggregationQuery.Predicate queryPredicate : queryPredicates.asCollection()) {
			if (queryPredicate instanceof AggregationQuery.PredicateNotEquals) {
				notEqualsPredicates.add((AggregationQuery.PredicateNotEquals) queryPredicate);
			}
		}

		return notEqualsPredicates;
	}

	private static boolean sortingRequired(List<String> keys, List<String> aggregationKeys) {
		boolean resultKeysAreSubset = !all(aggregationKeys, in(keys));
		return resultKeysAreSubset && !isPrefix(keys, aggregationKeys);
	}

	private static boolean isPrefix(List<String> l1, List<String> l2) {
		checkArgument(l1.size() <= l2.size());
		for (int i = 0; i < l1.size(); ++i) {
			if (!l1.get(i).equals(l2.get(i)))
				return false;
		}
		return true;
	}

	private void doConsolidation(final List<AggregationChunk> chunksToConsolidate,
	                             final ResultCallback<List<AggregationChunk.NewChunk>> callback) {
		Set<String> aggregationFields = newHashSet(getFields());
		Set<String> chunkFields = newHashSet();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getFields()) {
				if (aggregationFields.contains(field))
					chunkFields.add(field);
			}
		}

		List<String> fields = Ordering.explicit(getFields()).sortedCopy(chunkFields);

		Class resultClass = structure.createRecordClass(getKeys(), fields, classLoader);

		ConsolidationPlan consolidationPlan = new ConsolidationPlan();
		consolidatedProducer(getKeys(), getKeys(), fields, resultClass, null, chunksToConsolidate, null, consolidationPlan, classLoader)
				.streamTo(new AggregationChunker(eventloop, this, getKeys(), fields, resultClass,
						createPartitionPredicate(resultClass), aggregationChunkStorage, metadataStorage,
						aggregationChunkSize, classLoader, callback));
		logger.info("Consolidation plan: {}", consolidationPlan);
	}

	private <T> StreamProducer<T> consolidatedProducer(List<String> queryKeys, List<String> requestedKeys,
	                                                   List<String> fields, Class<T> resultClass,
	                                                   AggregationQuery.Predicates predicates,
	                                                   List<AggregationChunk> individualChunks,
	                                                   AggregationQueryPlan queryPlan,
	                                                   ConsolidationPlan consolidationPlan,
	                                                   DefiningClassLoader classLoader) {
		Set<String> fieldsSet = newHashSet(fields);
		individualChunks = newArrayList(individualChunks);
		Collections.sort(individualChunks, new Comparator<AggregationChunk>() {
			@Override
			public int compare(AggregationChunk chunk1, AggregationChunk chunk2) {
				return chunk1.getMinPrimaryKey().compareTo(chunk2.getMinPrimaryKey());
			}
		});

		List<StreamProducer> producers = new ArrayList<>();
		List<List<String>> producersFields = new ArrayList<>();
		List<Class<?>> producersClasses = new ArrayList<>();

		List<AggregationChunk> chunks = new ArrayList<>();

		/*
		Build producers. Chunks can be read sequentially (using StreamProducers.concat) when the ranges of their keys do not overlap.
		 */
		for (int i = 0; i <= individualChunks.size(); i++) {
			AggregationChunk chunk = (i != individualChunks.size()) ? individualChunks.get(i) : null;

			boolean nextSequence = chunks.isEmpty() || chunk == null ||
					getLast(chunks).getMaxPrimaryKey().compareTo(chunk.getMinPrimaryKey()) >= 0 ||
					!newHashSet(getLast(chunks).getFields()).equals(newHashSet(chunk.getFields()));

			if (nextSequence && !chunks.isEmpty()) {
				List<String> sequenceFields = chunks.get(0).getFields();
				Set<String> requestedFieldsInSequence = intersection(fieldsSet, newLinkedHashSet(sequenceFields));

				Class<?> chunksClass = structure.createRecordClass(getKeys(), sequenceFields, this.classLoader);

				producersFields.add(sequenceFields);
				producersClasses.add(chunksClass);

				List<AggregationChunk> sequentialChunkGroup = newArrayList(chunks);

				boolean sorted = false;
				StreamProducer producer = sequentialProducer(predicates, sequentialChunkGroup, chunksClass, classLoader);
				if (sortingRequired(requestedKeys, getKeys())) {
					producer = getOrderedStream(producer, chunksClass, requestedKeys, sequenceFields, this.classLoader);
					sorted = true;
				}
				producers.add(producer);

				chunks.clear();

				if (queryPlan != null)
					queryPlan.addChunkGroup(newArrayList(requestedFieldsInSequence), sequentialChunkGroup, sorted);

				if (consolidationPlan != null)
					consolidationPlan.addChunkGroup(newArrayList(requestedFieldsInSequence), sequentialChunkGroup);
			}

			if (chunk != null) {
				chunks.add(chunk);
			}
		}

		return mergeProducers(queryKeys, requestedKeys, fields, resultClass, producers, producersFields,
				producersClasses, queryPlan, classLoader);
	}

	private <T> StreamProducer<T> mergeProducers(List<String> queryKeys, List<String> requestedKeys, List<String> fields,
	                                             Class<?> resultClass, List<StreamProducer> producers,
	                                             List<List<String>> producersFields, List<Class<?>> producerClasses,
	                                             AggregationQueryPlan queryPlan, DefiningClassLoader classLoader) {
		if (newHashSet(requestedKeys).equals(newHashSet(getKeys())) && producers.size() == 1) {
			/*
			If there is only one sequential producer and all aggregation keys are requested, then there is no need for
			using StreamReducer, because all records have unique keys and all we need to do is copy requested fields
			from record class to result class.
			 */
			StreamMap.MapperProjection mapper = createMapper(producerClasses.get(0), resultClass, queryKeys,
					newArrayList(filter(fields, in(producersFields.get(0)))), classLoader);
			StreamMap<Object, T> streamMap = new StreamMap<>(eventloop, mapper);
			producers.get(0).streamTo(streamMap.getInput());
			if (queryPlan != null)
				queryPlan.setOptimizedAwayReducer(true);
			return streamMap.getOutput();
		}

		StreamReducer<Comparable, T, Object> streamReducer = new StreamReducer<>(eventloop, Ordering.natural());

		Class<?> keyClass = structure.createKeyClass(queryKeys, this.classLoader);

		for (int i = 0; i < producers.size(); i++) {
			StreamProducer producer = producers.get(i);

			Function extractKeyFunction = createKeyFunction(producerClasses.get(i), keyClass, queryKeys, this.classLoader);

			StreamReducers.Reducer reducer = processorFactory.aggregationReducer(producerClasses.get(i), resultClass,
					queryKeys, newArrayList(filter(fields, in(producersFields.get(i)))), classLoader);

			producer.streamTo(streamReducer.newInput(extractKeyFunction, reducer));
		}

		return streamReducer.getOutput();
	}

	private StreamProducer sequentialProducer(final AggregationQuery.Predicates predicates,
	                                          List<AggregationChunk> individualChunks, final Class<?> sequenceClass,
	                                          final DefiningClassLoader classLoader) {
		checkArgument(!individualChunks.isEmpty());
		AsyncIterator<StreamProducer<Object>> producerAsyncIterator = AsyncIterators.transform(individualChunks.iterator(),
				new AsyncFunction<AggregationChunk, StreamProducer<Object>>() {
					@Override
					public void apply(AggregationChunk chunk, ResultCallback<StreamProducer<Object>> producerCallback) {
						producerCallback.onResult(chunkReaderWithFilter(predicates, chunk, sequenceClass, classLoader));
					}
				});
		return StreamProducers.concat(eventloop, producerAsyncIterator);
	}

	private StreamProducer chunkReaderWithFilter(AggregationQuery.Predicates predicates, AggregationChunk chunk,
	                                             Class<?> chunkRecordClass, DefiningClassLoader classLoader) {
		StreamProducer chunkReader = aggregationChunkStorage.chunkReader(getKeys(),
				chunk.getFields(), chunkRecordClass, chunk.getChunkId(), this.classLoader);
		StreamProducer chunkProducer = chunkReader;
		if (ignoreChunkReadingExceptions) {
			ErrorIgnoringTransformer errorIgnoringTransformer = new ErrorIgnoringTransformer<>(eventloop);
			chunkReader.streamTo(errorIgnoringTransformer.getInput());
			chunkProducer = errorIgnoringTransformer.getOutput();
		}
		if (predicates == null)
			return chunkProducer;
		StreamFilter streamFilter = new StreamFilter<>(eventloop,
				createPredicate(chunk, chunkRecordClass, predicates, classLoader));
		chunkProducer.streamTo(streamFilter.getInput());
		return streamFilter.getOutput();
	}

	private static Predicate createNotEqualsPredicate(Class<?> recordClass,
	                                                  List<AggregationQuery.PredicateNotEquals> notEqualsPredicates,
	                                                  DefiningClassLoader classLoader) {
		AsmBuilder<Predicate> builder = new AsmBuilder<>(classLoader, Predicate.class);
		PredicateDefAnd predicateDefAnd = and();
		for (AggregationQuery.PredicateNotEquals notEqualsPredicate : notEqualsPredicates) {
			predicateDefAnd.add(cmpNe(
					getter(cast(arg(0), recordClass), notEqualsPredicate.key),
					value(notEqualsPredicate.value)
			));
		}
		builder.method("apply", boolean.class, singletonList(Object.class), predicateDefAnd);
		return builder.newInstance();
	}

	private Predicate createPredicate(AggregationChunk chunk, Class<?> chunkRecordClass,
	                                  AggregationQuery.Predicates predicates, DefiningClassLoader classLoader) {
		List<String> keysInChunk = new ArrayList<>();
		List<Object> objectsInChunk = new ArrayList<>();
		for (int i = 0; i < getKeys().size(); i++) {
			String key = getKeys().get(i);
			Object min = chunk.getMinPrimaryKey().get(i);
			Object max = chunk.getMaxPrimaryKey().get(i);
			if (!min.equals(max)) {
				break;
			}
			keysInChunk.add(key);
			objectsInChunk.add(min);
		}

		AsmBuilder<Predicate> builder = new AsmBuilder<>(classLoader, Predicate.class);
		PredicateDefAnd predicateDefAnd = and();

		for (AggregationQuery.Predicate predicate : predicates.asCollection()) {
			if (predicate instanceof AggregationQuery.PredicateEq) {
				Object value = ((AggregationQuery.PredicateEq) predicate).value;
				int indexInChunk = keysInChunk.indexOf(predicate.key);
				if (indexInChunk != -1 && objectsInChunk.get(indexInChunk).equals(value))
					continue;

				predicateDefAnd.add(cmpEq(
						getter(cast(arg(0), chunkRecordClass), predicate.key),
						value(value)));
			} else if (predicate instanceof AggregationQuery.PredicateBetween) {
				Object from = ((AggregationQuery.PredicateBetween) predicate).from;
				Object to = ((AggregationQuery.PredicateBetween) predicate).to;

				predicateDefAnd.add(cmpGe(
						getter(cast(arg(0), chunkRecordClass), predicate.key),
						value(from)));

				predicateDefAnd.add(cmpLe(
						getter(cast(arg(0), chunkRecordClass), predicate.key),
						value(to)));
			}
		}
		builder.method("apply", boolean.class, singletonList(Object.class), predicateDefAnd);
		return builder.newInstance();
	}

	public int getNumberOfOverlappingChunks() {
		return aggregationMetadata.findOverlappingChunks().size();
	}

	public void consolidateMinKey(final int maxChunksToConsolidate, final ResultCallback<Boolean> callback) {
		loadChunks(new CompletionCallback() {
			@Override
			public void onComplete() {
				List<AggregationChunk> chunks = aggregationMetadata.findChunksForConsolidationMinKey(maxChunksToConsolidate,
						aggregationChunkSize, partitioningKey == null ? 0 : partitioningKey.size());
				consolidate(chunks, callback);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Loading chunks for aggregation '{}' before starting min key consolidation failed",
						Aggregation.this, e);
				callback.onException(e);
			}
		});
	}

	public void consolidateHotSegment(final int maxChunksToConsolidate, final ResultCallback<Boolean> callback) {
		loadChunks(new CompletionCallback() {
			@Override
			public void onComplete() {
				List<AggregationChunk> chunks = aggregationMetadata.findChunksForConsolidationHotSegment(maxChunksToConsolidate);
				consolidate(chunks, callback);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Loading chunks for aggregation '{}' before starting consolidation of hot segment failed",
						Aggregation.this, e);
				callback.onException(e);
			}
		});
	}

	private void consolidate(final List<AggregationChunk> chunks, final ResultCallback<Boolean> callback) {
		if (chunks.isEmpty()) {
			logger.info("Nothing to consolidate in aggregation '{}", this);
			callback.onResult(false);
			return;
		}

		logger.info("Starting consolidation of aggregation '{}'", this);
		metadataStorage.startConsolidation(chunks, new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				logger.info("Completed writing consolidation start metadata in aggregation '{}'", Aggregation.this);
				doConsolidation(chunks, new ForwardingResultCallback<List<AggregationChunk.NewChunk>>(callback) {
					@Override
					public void onResult(final List<AggregationChunk.NewChunk> consolidatedChunks) {
						logger.info("Saving consolidation results to metadata storage in aggregation '{}'", Aggregation.this);
						metadataStorage.saveConsolidatedChunks(chunks, consolidatedChunks, new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Completed consolidation of the following chunks ({}) " +
												"in aggregation '{}': [{}]. Created chunks ({}): [{}]",
										chunks.size(), aggregationMetadata, getChunkIds(chunks),
										consolidatedChunks.size(), getNewChunkIds(consolidatedChunks));
								callback.onResult(true);
							}

							@Override
							public void onException(Exception e) {
								callback.onException(e);
							}
						});
					}
				});
			}
		});
	}

	public static String getNewChunkIds(Iterable<AggregationChunk.NewChunk> chunks) {
		List<Long> ids = new ArrayList<>();
		for (AggregationChunk.NewChunk chunk : chunks) {
			ids.add(chunk.chunkId);
		}
		return JOINER.join(ids);
	}

	public static String getChunkIds(Iterable<AggregationChunk> chunks) {
		List<Long> ids = new ArrayList<>();
		for (AggregationChunk chunk : chunks) {
			ids.add(chunk.getChunkId());
		}
		return JOINER.join(ids);
	}

	public void loadChunks(final CompletionCallback callback) {
		if (loadChunksCallback != null) {
			logger.info("Loading chunks for aggregation {} is already started. Added callback", this);
			loadChunksCallback.addListener(callback);
			return;
		}

		loadChunksCallback = new ListenableCompletionCallback();
		loadChunksCallback.addListener(callback);

		final boolean incremental = eventloop.currentTimeMillis() - lastReloadTimestamp <= maxIncrementalReloadPeriodMillis;
		logger.info("Loading chunks for aggregation {} (incremental={})", this, incremental);
		int revisionId = incremental ? lastRevisionId : 0;

		metadataStorage.loadChunks(revisionId, new ResultCallback<LoadedChunks>() {
			@Override
			public void onResult(LoadedChunks loadedChunks) {
				loadChunks(loadedChunks, incremental);
				CompletionCallback currentCallback = loadChunksCallback;
				loadChunksCallback = null;
				currentCallback.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Loading chunks for aggregation {} failed", this, exception);
				CompletionCallback currentCallback = loadChunksCallback;
				loadChunksCallback = null;
				currentCallback.onException(exception);
			}
		});
	}

	public void loadChunks(LoadedChunks loadedChunks, boolean incremental) {
		if (!incremental) {
			clearIndex();
		}

		for (AggregationChunk newChunk : loadedChunks.newChunks) {
			addToIndex(newChunk);
			logger.trace("Added chunk {} to index", newChunk);
		}

		for (Long consolidatedChunkId : loadedChunks.consolidatedChunkIds) {
			AggregationChunk chunk = chunks.get(consolidatedChunkId);
			if (chunk != null) {
				removeFromIndex(chunk);
				logger.trace("Removed chunk {} from index", chunk);
			}
		}

		this.lastRevisionId = loadedChunks.lastRevisionId;
		this.lastReloadTimestamp = eventloop.currentTimeMillis();

		logger.info("Loading chunks for aggregation {} completed. " +
						"Loaded {} new chunks and {} consolidated chunks. Revision id: {}",
				this, loadedChunks.newChunks.size(), loadedChunks.consolidatedChunkIds.size(),
				loadedChunks.lastRevisionId);
	}

	public List<AggregationMetadata.ConsolidationDebugInfo> getConsolidationDebugInfo() {
		return aggregationMetadata.getConsolidationDebugInfo();
	}

	// visible for testing
	public void setLastReloadTimestamp(long lastReloadTimestamp) {
		this.lastReloadTimestamp = lastReloadTimestamp;
	}

	// jmx

	public int getMaxIncrementalReloadPeriodMillis() {
		return maxIncrementalReloadPeriodMillis;
	}

	public void setMaxIncrementalReloadPeriodMillis(int maxIncrementalReloadPeriodMillis) {
		this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
	}

	public int getAggregationChunkSize() {
		return aggregationChunkSize;
	}

	public void setAggregationChunkSize(int chunkSize) {
		this.aggregationChunkSize = chunkSize;
		for (AggregationChunker chunker : activeChunkers) {
			chunker.setChunkSize(chunkSize);
		}
		for (AggregationGroupReducer groupReducer : activeGroupReducers) {
			groupReducer.setChunkSize(chunkSize);
		}
	}

	public int getSorterItemsInMemory() {
		return sorterItemsInMemory;
	}

	public void setSorterItemsInMemory(int sorterItemsInMemory) {
		this.sorterItemsInMemory = sorterItemsInMemory;
	}

	public int getSorterBlockSize() {
		return sorterBlockSize;
	}

	public void setSorterBlockSize(int sorterBlockSize) {
		this.sorterBlockSize = sorterBlockSize;
	}

	public boolean isIgnoreChunkReadingExceptions() {
		return ignoreChunkReadingExceptions;
	}

	public void setIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
		this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
	}

	public void flushBuffers() {
		for (AggregationGroupReducer groupReducer : activeGroupReducers) {
			groupReducer.flush();
		}
	}

	public int getBuffersSize() {
		int size = 0;
		for (AggregationGroupReducer groupReducer : activeGroupReducers) {
			size += groupReducer.getBufferSize();
		}
		return size;
	}

	@Override
	public void reportStart(AggregationChunker chunker) {
		activeChunkers.add(chunker);
	}

	@Override
	public void reportCompletion(AggregationChunker chunker) {
		activeChunkers.remove(chunker);
	}

	@Override
	public void reportStart(AggregationGroupReducer groupReducer) {
		activeGroupReducers.add(groupReducer);
	}

	@Override
	public void reportCompletion(AggregationGroupReducer groupReducer) {
		activeGroupReducers.remove(groupReducer);
	}

	@Override
	public String toString() {
		return getKeys().toString();
	}
}
