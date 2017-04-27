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

package io.datakernel.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.aggregation.AggregationMetadataStorage.LoadedChunks;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.async.*;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
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

import static com.google.common.base.Functions.identity;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.*;
import static io.datakernel.aggregation.AggregationUtils.*;
import static io.datakernel.codegen.Expressions.arg;
import static io.datakernel.codegen.Expressions.cast;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;

/**
 * Represents an aggregation, which aggregates data using custom reducer and preaggregator.
 * Provides methods for loading and querying data.
 */
@SuppressWarnings("unchecked")
public class Aggregation implements IAggregation, AggregationOperationTracker, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private static final Joiner JOINER = Joiner.on(", ");

	public static final int DEFAULT_CHUNK_SIZE = 1_000_000;
	public static final int DEFAULT_SORTER_ITEMS_IN_MEMORY = 1_000_000;
	public static final int DEFAULT_SORTER_BLOCK_SIZE = 256 * 1024;
	public static final long DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS = 10 * 60 * 1000; // 10 minutes
	public static final int DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE = 1000;

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final AggregationMetadataStorage metadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final AggregationMetadata metadata;

	private final Map<String, FieldType> keyTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> measureTypes = new LinkedHashMap<>();
	private final List<String> partitioningKey = new ArrayList<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();

	// settings
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int sorterItemsInMemory = DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private int sorterBlockSize = DEFAULT_SORTER_BLOCK_SIZE;
	private long maxIncrementalReloadPeriodMillis = DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS;
	private boolean ignoreChunkReadingExceptions = false;
	private int maxChunksToConsolidate = DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE;

	// state
	private int lastRevisionId;
	private long lastReloadTimestamp;
	private ListenableCompletionCallback loadChunksCallback;

	// jmx
	private final List<AggregationGroupReducer> activeGroupReducers = new ArrayList<>();
	private final List<AggregationChunker> activeChunkers = new ArrayList<>();
	private long consolidationStarted;
	private long consolidationLastTimeMillis;
	private int consolidations;
	private Exception consolidationLastError;

	private Aggregation(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                    AggregationMetadataStorage metadataStorage, AggregationChunkStorage aggregationChunkStorage) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.metadataStorage = metadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.metadata = new AggregationMetadata(this);
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
	 */
	public static Aggregation create(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                                 AggregationMetadataStorage metadataStorage, AggregationChunkStorage aggregationChunkStorage) {
		return new Aggregation(eventloop, executorService, classLoader, metadataStorage, aggregationChunkStorage);
	}

	@VisibleForTesting
	static Aggregation createUninitialized() {
		return new Aggregation(null, null, null, null, null);
	}

	public Aggregation withChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
		return this;
	}

	public Aggregation withSorterItemsInMemory(int sorterItemsInMemory) {
		this.sorterItemsInMemory = sorterItemsInMemory;
		return this;
	}

	public Aggregation withSorterBlockSize(int sorterBlockSize) {
		this.sorterBlockSize = sorterBlockSize;
		return this;
	}

	public Aggregation withMaxIncrementalReloadPeriodMillis(int maxIncrementalReloadPeriodMillis) {
		this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
		return this;
	}

	public Aggregation withIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
		this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
		return this;
	}

	public Aggregation withMaxChunksToConsolidate(int maxChunksToConsolidate) {
		this.maxChunksToConsolidate = maxChunksToConsolidate;
		return this;
	}

	public Aggregation withKey(String keyId, FieldType type) {
		checkArgument(!keyTypes.containsKey(keyId));
		keyTypes.put(keyId, type);
		metadata.initIndex();
		return this;
	}

	public Aggregation withKeys(Map<String, FieldType> keyTypes) {
		this.keyTypes.putAll(keyTypes);
		metadata.initIndex();
		return this;
	}

	public Aggregation withMeasure(String measureId, Measure aggregateFunction) {
		checkArgument(!measureTypes.containsKey(measureId));
		measureTypes.put(measureId, aggregateFunction.getFieldType());
		measures.put(measureId, aggregateFunction);
		return this;
	}

	public Aggregation withMeasures(Map<String, Measure> measures) {
		for (String measureId : measures.keySet()) {
			withMeasure(measureId, measures.get(measureId));
		}
		return this;
	}

	public Aggregation withIgnoredMeasure(String measureId, FieldType measureType) {
		checkArgument(!measureTypes.containsKey(measureId));
		measureTypes.put(measureId, measureType);
		return this;
	}

	public Aggregation withIgnoredMeasures(Map<String, FieldType> measureTypes) {
		checkArgument(intersection(this.measureTypes.keySet(), measureTypes.keySet()).isEmpty());
		this.measureTypes.putAll(measureTypes);
		return this;
	}

	public Aggregation withPartitioningKey(List<String> partitioningKey) {
		this.partitioningKey.addAll(partitioningKey);
		return this;
	}

	public Aggregation withPartitioningKey(String... partitioningKey) {
		this.partitioningKey.addAll(Arrays.asList(partitioningKey));
		return this;
	}

	public List<String> getKeys() {
		return newArrayList(keyTypes.keySet());
	}

	public List<String> getMeasures() {
		return newArrayList(measures.keySet());
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

	@VisibleForTesting
	public AggregationMetadata getMetadata() {
		return metadata;
	}

	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass,
	                                                 List<String> keys, List<String> measures,
	                                                 DefiningClassLoader classLoader) {
		return AggregationUtils.aggregationReducer(this, inputClass, outputClass,
				keys, measures, classLoader);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass class of input records
	 * @param callback   callback which is called when chunks are created
	 * @param <T>        data records type
	 * @return consumer for streaming data to aggregation
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, Map<String, String> keyFields, Map<String, String> measureFields,
	                                      ResultCallback<List<AggregationChunk.NewChunk>> callback) {
		checkArgument(newHashSet(getKeys()).equals(keyFields.keySet()), "Expected keys: %s, actual keyFields: %s", getKeys(), keyFields);
		checkArgument(measures.keySet().containsAll(measureFields.keySet()), "Unknown measures: %s", difference(measureFields.keySet(), measures.keySet()));

		logger.info("Started consuming data in aggregation {}. Keys: {} Measures: {}", this, keyFields.keySet(), measureFields.keySet());

		Class<?> keyClass = createKeyClass(this, getKeys(), classLoader);
		List<String> measures = newArrayList(filter(this.measures.keySet(), in(measureFields.keySet())));
		Class<?> accumulatorClass = createRecordClass(this, getKeys(),
				measures, classLoader);

		Function<T, Comparable<?>> keyFunction = createKeyFunction(inputClass, keyClass, getKeys(), classLoader);

		Aggregate aggregate = createPreaggregator(this, inputClass, accumulatorClass,
				keyFields, measureFields,
				classLoader);

		return new AggregationGroupReducer<>(eventloop, aggregationChunkStorage, this, metadataStorage,
				this, getKeys(), measures,
				accumulatorClass,
				createPartitionPredicate(accumulatorClass, getPartitioningKey(), classLoader),
				keyFunction, aggregate, chunkSize, classLoader, callback);
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, ResultCallback<List<AggregationChunk.NewChunk>> callback) {
		return consumer(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), callback);
	}

	public double estimateCost(AggregationQuery query) {
		List<String> aggregationFields = newArrayList(filter(query.getMeasures(), in(getMeasures())));
		return metadata.findChunks(query.getPredicate(), aggregationFields).size();
	}

	public <T> StreamProducer<T> query(AggregationQuery query, Class<T> outputClass) {
		return query(query, outputClass, classLoader);
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
	@Override
	public <T> StreamProducer<T> query(AggregationQuery query, Class<T> outputClass, DefiningClassLoader queryClassLoader) {
		ClassLoader cl;
		for (cl = queryClassLoader; cl != null; cl = cl.getParent()) {
			if (cl == this.classLoader)
				break;
		}
		checkArgument(cl != null, "Unrelated queryClassLoader");
		List<String> aggregationFields = newArrayList(filter(query.getMeasures(), in(getMeasures())));

		List<AggregationChunk> allChunks = metadata.findChunks(query.getPredicate(), aggregationFields);

		AggregationQueryPlan queryPlan = AggregationQueryPlan.create();

		StreamProducer streamProducer = consolidatedProducer(query.getKeys(),
				aggregationFields, outputClass, query.getPredicate(), allChunks, queryPlan, null, queryClassLoader);

		logger.info("Query plan for {} in aggregation {}: {}", query, this, queryPlan);

		return streamProducer;
	}

	private <T> StreamProducer<T> getOrderedStream(StreamProducer<T> rawStream, Class<T> resultClass,
	                                               List<String> allKeys, List<String> measures, DefiningClassLoader classLoader) {
		Comparator keyComparator = createKeyComparator(resultClass, allKeys, classLoader);
		Path path = Paths.get("sorterStorage", "%d.part");
		BufferSerializer bufferSerializer = createBufferSerializer(this, resultClass,
				getKeys(), measures, classLoader);
		StreamMergeSorterStorage sorterStorage = StreamMergeSorterStorageImpl.create(eventloop, executorService,
				bufferSerializer, path, sorterBlockSize);
		StreamSorter sorter = StreamSorter.create(eventloop, sorterStorage, identity(), keyComparator, false,
				sorterItemsInMemory);
		rawStream.streamTo(sorter.getInput());
		return sorter.getOutput();
	}

	private boolean alreadySorted(List<String> keys) {
		return getKeys().subList(0, min(getKeys().size(), keys.size())).equals(keys);
	}

	private void doConsolidation(final List<AggregationChunk> chunksToConsolidate,
	                             final ResultCallback<List<AggregationChunk.NewChunk>> callback) {
		Set<String> aggregationFields = newHashSet(getMeasures());
		Set<String> chunkFields = newHashSet();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String measure : chunk.getMeasures()) {
				if (aggregationFields.contains(measure))
					chunkFields.add(measure);
			}
		}

		List<String> measures = Ordering.explicit(getMeasures()).sortedCopy(chunkFields);

		Class resultClass = createRecordClass(this, getKeys(), measures, classLoader);

		ConsolidationPlan consolidationPlan = ConsolidationPlan.create();
		consolidatedProducer(getKeys(), measures, resultClass, null, chunksToConsolidate, null, consolidationPlan, classLoader)
				.streamTo(new AggregationChunker<>(eventloop, this,
						this, getKeys(), measures, resultClass,
						createPartitionPredicate(resultClass, getPartitioningKey(), classLoader),
						aggregationChunkStorage, metadataStorage, chunkSize, classLoader, callback));
		logger.info("Consolidation plan: {}", consolidationPlan);
	}

	private <T> StreamProducer<T> consolidatedProducer(List<String> queryKeys,
	                                                   List<String> measures, Class<T> resultClass,
	                                                   AggregationPredicate where,
	                                                   List<AggregationChunk> individualChunks,
	                                                   AggregationQueryPlan queryPlan,
	                                                   ConsolidationPlan consolidationPlan,
	                                                   DefiningClassLoader queryClassLoader) {
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
					!newHashSet(getLast(chunks).getMeasures()).equals(newHashSet(chunk.getMeasures()));

			if (nextSequence && !chunks.isEmpty()) {
				List<String> sequenceMeasures = chunks.get(0).getMeasures();
				Set<String> requestedMeasuresInSequence = intersection(newHashSet(measures), newLinkedHashSet(sequenceMeasures));

				Class<?> chunksClass = createRecordClass(this, getKeys(), sequenceMeasures, classLoader);

				producersFields.add(sequenceMeasures);
				producersClasses.add(chunksClass);

				List<AggregationChunk> sequentialChunkGroup = newArrayList(chunks);

				boolean sorted = false;
				StreamProducer producer = sequentialProducer(where, sequentialChunkGroup, chunksClass, queryClassLoader);
				if (!getKeys().subList(0, min(getKeys().size(), queryKeys.size())).equals(queryKeys)) {
					producer = getOrderedStream(producer, chunksClass, queryKeys, sequenceMeasures, classLoader);
					sorted = true;
				}
				producers.add(producer);

				chunks.clear();

				if (queryPlan != null)
					queryPlan.addChunkGroup(newArrayList(requestedMeasuresInSequence), sequentialChunkGroup, sorted);

				if (consolidationPlan != null)
					consolidationPlan.addChunkGroup(newArrayList(requestedMeasuresInSequence), sequentialChunkGroup);
			}

			if (chunk != null) {
				chunks.add(chunk);
			}
		}

		return mergeProducers(queryKeys, measures, resultClass, producers, producersFields,
				producersClasses, queryPlan, queryClassLoader);
	}

	private <T> StreamProducer<T> mergeProducers(List<String> queryKeys, List<String> measures,
	                                             Class<?> resultClass, List<StreamProducer> producers,
	                                             List<List<String>> producersFields, List<Class<?>> producerClasses,
	                                             AggregationQueryPlan queryPlan, DefiningClassLoader classLoader) {
		if (newHashSet(queryKeys).equals(newHashSet(getKeys())) && producers.size() == 1) {
			/*
			If there is only one sequential producer and all aggregation keys are requested, then there is no need for
			using StreamReducer, because all records have unique keys and all we need to do is copy requested measures
			from record class to result class.
			 */
			StreamMap.MapperProjection mapper = createMapper(producerClasses.get(0), resultClass,
					queryKeys, newArrayList(filter(measures, in(producersFields.get(0)))),
					classLoader);
			StreamMap<Object, T> streamMap = StreamMap.create(eventloop, mapper);
			producers.get(0).streamTo(streamMap.getInput());
			if (queryPlan != null)
				queryPlan.setOptimizedAwayReducer(true);
			return streamMap.getOutput();
		}

		StreamReducer<Comparable, T, Object> streamReducer = StreamReducer.create(eventloop, Ordering.natural());

		Class<?> keyClass = createKeyClass(this, queryKeys, this.classLoader);

		for (int i = 0; i < producers.size(); i++) {
			StreamProducer producer = producers.get(i);

			Function extractKeyFunction = createKeyFunction(producerClasses.get(i), keyClass, queryKeys, this.classLoader);

			StreamReducers.Reducer reducer = AggregationUtils.aggregationReducer(this, producerClasses.get(i), resultClass,
					queryKeys, newArrayList(filter(measures, in(producersFields.get(i)))), classLoader);

			producer.streamTo(streamReducer.newInput(extractKeyFunction, reducer));
		}

		return streamReducer.getOutput();
	}

	private StreamProducer sequentialProducer(final AggregationPredicate where,
	                                          List<AggregationChunk> individualChunks, final Class<?> sequenceClass,
	                                          final DefiningClassLoader queryClassLoader) {
		checkArgument(!individualChunks.isEmpty());
		AsyncIterator<StreamProducer<Object>> producerAsyncIterator = AsyncIterators.transform(individualChunks.iterator(),
				new AsyncFunction<AggregationChunk, StreamProducer<Object>>() {
					@Override
					public void apply(AggregationChunk chunk, ResultCallback<StreamProducer<Object>> producerCallback) {
						producerCallback.setResult(chunkReaderWithFilter(where, chunk, sequenceClass, queryClassLoader));
					}
				});
		return StreamProducers.concat(eventloop, producerAsyncIterator);
	}

	private StreamProducer chunkReaderWithFilter(AggregationPredicate where, AggregationChunk chunk,
	                                             Class<?> chunkRecordClass, DefiningClassLoader queryClassLoader) {
		StreamProducer chunkReader = aggregationChunkStorage.chunkReader(this, getKeys(), chunk.getMeasures(),
				chunkRecordClass, chunk.getChunkId(), this.classLoader);
		StreamProducer chunkProducer = chunkReader;
		if (ignoreChunkReadingExceptions) {
			ErrorIgnoringTransformer errorIgnoringTransformer = ErrorIgnoringTransformer.create(eventloop);
			chunkReader.streamTo(errorIgnoringTransformer.getInput());
			chunkProducer = errorIgnoringTransformer.getOutput();
		}
		if (where == null || where == AggregationPredicates.alwaysTrue())
			return chunkProducer;
		StreamFilter streamFilter = StreamFilter.create(eventloop,
				createPredicate(chunkRecordClass, where, queryClassLoader));
		chunkProducer.streamTo(streamFilter.getInput());
		return streamFilter.getOutput();
	}

	private Predicate createPredicate(Class<?> chunkRecordClass,
	                                  AggregationPredicate where, DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Predicate.class)
				.withMethod("apply", boolean.class, singletonList(Object.class),
						where.createPredicateDef(cast(arg(0), chunkRecordClass), keyTypes))
				.buildClassAndCreateNewInstance();
	}

	@JmxAttribute
	public int getNumberOfOverlappingChunks() {
		return metadata.findOverlappingChunks().size();
	}

	public void consolidateMinKey(final ResultCallback<Boolean> callback) {
		consolidate(false, callback);
	}

	public void consolidateHotSegment(final ResultCallback<Boolean> callback) {
		consolidate(true, callback);
	}

	private void consolidate(final boolean hotSegment, final ResultCallback<Boolean> callback) {
		if (consolidationStarted != 0) {
			logger.warn("Consolidation has already been started {} seconds ago", (eventloop.currentTimeMillis() - consolidationStarted) / 1000);
			callback.setResult(false);
			return;
		}
		consolidationStarted = eventloop.currentTimeMillis();
		doConsolidate(hotSegment, new ResultCallback<Boolean>() {
			@Override
			protected void onResult(Boolean result) {
				if (result) {
					consolidationLastTimeMillis = eventloop.currentTimeMillis() - consolidationStarted;
					consolidations++;
				}
				consolidationStarted = 0;
				callback.setResult(result);
			}

			@Override
			protected void onException(Exception e) {
				consolidationStarted = 0;
				consolidationLastError = e;
				callback.setException(e);
			}
		});
	}

	private void doConsolidate(final boolean hotSegment, final ResultCallback<Boolean> callback) {
		loadChunks(new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				List<AggregationChunk> chunks = hotSegment ?
						metadata.findChunksForConsolidationHotSegment(maxChunksToConsolidate) :
						metadata.findChunksForConsolidationMinKey(maxChunksToConsolidate, chunkSize);
				doConsolidate(chunks, callback);
			}
		});
	}

	private void doConsolidate(final List<AggregationChunk> chunks, final ResultCallback<Boolean> callback) {
		if (chunks.isEmpty()) {
			logger.info("Nothing to consolidate in aggregation '{}", this);
			callback.setResult(false);
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
						metadataStorage.saveConsolidatedChunks(chunks, consolidatedChunks, new ForwardingCompletionCallback(callback) {
							@Override
							public void onComplete() {
								logger.info("Completed consolidation of the following chunks ({}) " +
												"in aggregation '{}': [{}]. Created chunks ({}): [{}]",
										chunks.size(), metadata, getChunkIds(chunks),
										consolidatedChunks.size(), getNewChunkIds(consolidatedChunks));
								callback.setResult(true);
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
			loadChunksCallback.addListener(callback);
			return;
		}
		loadChunksCallback = ListenableCompletionCallback.create();
		loadChunksCallback.addListener(callback);

		final boolean incremental = eventloop.currentTimeMillis() - lastReloadTimestamp <= maxIncrementalReloadPeriodMillis;
		logger.info("Loading chunks for aggregation {} (incremental={})", this, incremental);
		int revisionId = incremental ? lastRevisionId : 0;

		metadataStorage.loadChunks(this, revisionId, new CompletionCallback() {
			@Override
			protected void onComplete() {
				CompletionCallback currentCallback = loadChunksCallback;
				loadChunksCallback = null;
				currentCallback.setComplete();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Loading chunks for aggregation {} failed", this, exception);
				CompletionCallback currentCallback = loadChunksCallback;
				loadChunksCallback = null;
				currentCallback.setException(exception);
			}
		});
	}

	public void loadChunks(LoadedChunks loadedChunks, boolean incremental) {
		if (!incremental) {
			metadata.clearIndex();
		}

		for (AggregationChunk newChunk : loadedChunks.newChunks) {
			metadata.addToIndex(newChunk);
			logger.trace("Added chunk {} to index", newChunk);
		}

		for (Long consolidatedChunkId : loadedChunks.consolidatedChunkIds) {
			AggregationChunk chunk = metadata.getChunks().get(consolidatedChunkId);
			if (chunk != null) {
				metadata.removeFromIndex(chunk);
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
		return metadata.getConsolidationDebugInfo();
	}

	@VisibleForTesting
	public void setLastReloadTimestamp(long lastReloadTimestamp) {
		this.lastReloadTimestamp = lastReloadTimestamp;
	}

	// jmx

	@JmxAttribute
	public long getMaxIncrementalReloadPeriodMillis() {
		return maxIncrementalReloadPeriodMillis;
	}

	@JmxAttribute
	public void setMaxIncrementalReloadPeriodMillis(long maxIncrementalReloadPeriodMillis) {
		this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
	}

	@JmxAttribute
	public int getChunkSize() {
		return chunkSize;
	}

	@JmxAttribute
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
		for (AggregationChunker chunker : activeChunkers) {
			chunker.setChunkSize(chunkSize);
		}
		for (AggregationGroupReducer groupReducer : activeGroupReducers) {
			groupReducer.setChunkSize(chunkSize);
		}
	}

	@JmxAttribute
	public int getSorterItemsInMemory() {
		return sorterItemsInMemory;
	}

	@JmxAttribute
	public void setSorterItemsInMemory(int sorterItemsInMemory) {
		this.sorterItemsInMemory = sorterItemsInMemory;
	}

	@JmxAttribute
	public int getSorterBlockSize() {
		return sorterBlockSize;
	}

	@JmxAttribute
	public void setSorterBlockSize(int sorterBlockSize) {
		this.sorterBlockSize = sorterBlockSize;
	}

	@JmxAttribute
	public boolean isIgnoreChunkReadingExceptions() {
		return ignoreChunkReadingExceptions;
	}

	@JmxAttribute
	public void setIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
		this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
	}

	@JmxAttribute
	public int getMaxChunksToConsolidate() {
		return maxChunksToConsolidate;
	}

	@JmxAttribute
	public void setMaxChunksToConsolidate(int maxChunksToConsolidate) {
		this.maxChunksToConsolidate = maxChunksToConsolidate;
	}

	@JmxOperation
	public void flushActiveReducersBuffers() {
		for (AggregationGroupReducer groupReducer : activeGroupReducers) {
			groupReducer.flush();
		}
	}

	@JmxAttribute
	public int getActiveReducersBuffersSize() {
		int size = 0;
		for (AggregationGroupReducer groupReducer : activeGroupReducers) {
			size += groupReducer.getBufferSize();
		}
		return size;
	}

	@JmxOperation
	public void reloadChunks() {
		loadChunks(IgnoreCompletionCallback.create());
	}

	@JmxOperation
	public void consolidateHotSegment() {
		consolidateHotSegment(IgnoreResultCallback.<Boolean>create());
	}

	@JmxOperation
	public void consolidateMinKey() {
		consolidateMinKey(IgnoreResultCallback.<Boolean>create());
	}

	@JmxAttribute
	public Integer getConsolidationSeconds() {
		return consolidationStarted == 0 ? null : (int) ((eventloop.currentTimeMillis() - consolidationStarted) / 1000);
	}

	@JmxAttribute
	public Integer getConsolidationLastTimeSeconds() {
		return consolidationLastTimeMillis == 0 ? null : (int) ((consolidationLastTimeMillis) / 1000);
	}

	@JmxAttribute
	public int getConsolidations() {
		return consolidations;
	}

	@JmxAttribute
	public Exception getConsolidationLastError() {
		return consolidationLastError;
	}

	@JmxAttribute
	public int getChunks() {
		return metadata.getChunks().size();
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
		return "{" + keyTypes.keySet() + " " + measures.keySet() + '}';
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
