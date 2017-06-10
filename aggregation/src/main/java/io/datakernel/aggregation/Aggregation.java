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
public class Aggregation implements IAggregation, EventloopJmxMBean {
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
	                                      ResultCallback<List<AggregationChunk>> callback) {
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

		return new AggregationGroupReducer<>(eventloop, aggregationChunkStorage, metadataStorage,
				this, getKeys(), measures,
				accumulatorClass,
				createPartitionPredicate(accumulatorClass, getPartitioningKey(), classLoader),
				keyFunction, aggregate, chunkSize, classLoader, callback);
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, ResultCallback<List<AggregationChunk>> callback) {
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

		return consolidatedProducer(query.getKeys(),
				aggregationFields, outputClass, query.getPredicate(), allChunks, queryClassLoader);
	}

	private <T> StreamProducer<T> sortStream(StreamProducer<T> unsortedStream, Class<T> resultClass,
	                                         List<String> allKeys, List<String> measures, DefiningClassLoader classLoader) {
		Comparator keyComparator = createKeyComparator(resultClass, allKeys, classLoader);
		Path path = Paths.get("sorterStorage", "%d.part");
		BufferSerializer bufferSerializer = createBufferSerializer(this, resultClass,
				getKeys(), measures, classLoader);
		StreamMergeSorterStorage sorterStorage = StreamMergeSorterStorageImpl.create(eventloop, executorService,
				bufferSerializer, path, sorterBlockSize);
		StreamSorter sorter = StreamSorter.create(eventloop, sorterStorage, identity(), keyComparator, false,
				sorterItemsInMemory);
		unsortedStream.streamTo(sorter.getInput());
		return sorter.getOutput();
	}

	private boolean alreadySorted(List<String> keys) {
		return getKeys().subList(0, min(getKeys().size(), keys.size())).equals(keys);
	}

	private void doConsolidation(final List<AggregationChunk> chunksToConsolidate,
	                             final ResultCallback<List<AggregationChunk>> callback) {
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

		StreamProducer<Object> consolidatedProducer = consolidatedProducer(getKeys(), measures, resultClass, null, chunksToConsolidate, classLoader);
		AggregationChunker<Object> chunker = new AggregationChunker<>(eventloop,
				this, getKeys(), measures, resultClass,
				createPartitionPredicate(resultClass, getPartitioningKey(), classLoader),
				aggregationChunkStorage, metadataStorage, chunkSize, classLoader, callback);
		consolidatedProducer.streamTo(chunker);
	}

	private static void addChunkToPlan(Map<Set<String>, TreeMap<PrimaryKey, List<QueryPlan.Sequence>>> planIndex,
	                                   AggregationChunk chunk) {
		Set<String> fields = new LinkedHashSet<>(chunk.getMeasures());
		TreeMap<PrimaryKey, List<QueryPlan.Sequence>> map = planIndex.get(fields);
		if (map == null) {
			map = new TreeMap<>();
			planIndex.put(fields, map);
		}

		Map.Entry<PrimaryKey, List<QueryPlan.Sequence>> entry = map.lowerEntry(chunk.getMinPrimaryKey());
		QueryPlan.Sequence sequence;
		if (entry == null) {
			sequence = new QueryPlan.Sequence(chunk.getMeasures());
		} else {
			List<QueryPlan.Sequence> list = entry.getValue();
			sequence = list.remove(list.size() - 1);
			if (list.isEmpty()) {
				map.remove(entry.getKey());
			}
		}
		sequence.add(chunk);
		List<QueryPlan.Sequence> list = map.get(chunk.getMaxPrimaryKey());
		if (list == null) {
			list = new ArrayList<>();
			map.put(chunk.getMaxPrimaryKey(), list);
		}
		list.add(sequence);
	}

	static QueryPlan createPlan(List<AggregationChunk> chunks) {
		Map<Set<String>, TreeMap<PrimaryKey, List<QueryPlan.Sequence>>> index = new HashMap<>();
		chunks = newArrayList(chunks);
		Collections.sort(chunks, new Comparator<AggregationChunk>() {
			@Override
			public int compare(AggregationChunk chunk1, AggregationChunk chunk2) {
				return chunk1.getMinPrimaryKey().compareTo(chunk2.getMinPrimaryKey());
			}
		});
		for (AggregationChunk chunk : chunks) {
			addChunkToPlan(index, chunk);
		}
		List<QueryPlan.Sequence> sequences = new ArrayList<>();
		for (TreeMap<PrimaryKey, List<QueryPlan.Sequence>> map : index.values()) {
			for (List<QueryPlan.Sequence> list : map.values()) {
				sequences.addAll(list);
			}
		}
		return new QueryPlan(sequences);
	}

	private <T> StreamProducer<T> consolidatedProducer(List<String> queryKeys,
	                                                   List<String> measures, Class<T> resultClass,
	                                                   AggregationPredicate where,
	                                                   List<AggregationChunk> individualChunks,
	                                                   DefiningClassLoader queryClassLoader) {
		QueryPlan plan = createPlan(individualChunks);

		logger.info("Query plan for {} in aggregation {}: {}", queryKeys, this, plan);

		boolean alreadySorted = this.getKeys().subList(0, min(this.getKeys().size(), queryKeys.size())).equals(queryKeys);

		List<SequenceStream> sequenceStreams = new ArrayList<>();

		for (QueryPlan.Sequence sequence : plan.getSequences()) {
			Class<?> sequenceClass = createRecordClass(this, this.getKeys(), sequence.getFields(), classLoader);

			StreamProducer stream = sequenceStream(where, sequence.getChunks(), sequenceClass, queryClassLoader);
			if (!alreadySorted) {
				stream = sortStream(stream, sequenceClass, queryKeys, sequence.getFields(), classLoader);
			}

			sequenceStreams.add(new SequenceStream(stream, sequence.getFields(), sequenceClass));
		}

		return mergeSequences(queryKeys, measures, resultClass, sequenceStreams, queryClassLoader);
	}

	static final class SequenceStream<T> {
		final StreamProducer<T> stream;
		final List<String> fields;
		final Class<T> type;

		private SequenceStream(StreamProducer<T> stream, List<String> fields, Class<T> type) {
			this.stream = stream;
			this.fields = fields;
			this.type = type;
		}
	}

	private <T> StreamProducer<T> mergeSequences(List<String> queryKeys, List<String> measures,
	                                             Class<?> resultClass, List<SequenceStream> sequences,
	                                             DefiningClassLoader classLoader) {
		if (sequences.size() == 1 && newHashSet(queryKeys).equals(newHashSet(getKeys()))) {
			/*
			If there is only one sequential producer and all aggregation keys are requested, then there is no need for
			using StreamReducer, because all records have unique keys and all we need to do is copy requested measures
			from record class to result class.
			 */
			SequenceStream sequence = sequences.get(0);
			StreamMap.MapperProjection mapper = createMapper(sequence.type, resultClass,
					queryKeys, newArrayList(filter(measures, in(sequence.fields))), classLoader);
			StreamMap<Object, T> streamMap = StreamMap.create(eventloop, mapper);
			sequence.stream.streamTo(streamMap.getInput());
			return streamMap.getOutput();
		}

		StreamReducer<Comparable, T, Object> streamReducer = StreamReducer.create(eventloop, Ordering.natural());

		Class<?> keyClass = createKeyClass(this, queryKeys, this.classLoader);

		for (SequenceStream sequence : sequences) {
			Function extractKeyFunction = createKeyFunction(sequence.type, keyClass, queryKeys, this.classLoader);

			StreamReducers.Reducer reducer = AggregationUtils.aggregationReducer(this, sequence.type, resultClass,
					queryKeys, newArrayList(filter(measures, in(sequence.fields))), classLoader);

			sequence.stream.streamTo(streamReducer.newInput(extractKeyFunction, reducer));
		}

		return streamReducer.getOutput();
	}

	private <T> StreamProducer sequenceStream(final AggregationPredicate where,
	                                          List<AggregationChunk> individualChunks, final Class<T> sequenceClass,
	                                          final DefiningClassLoader queryClassLoader) {
		checkArgument(!individualChunks.isEmpty());
		AsyncIterator<StreamProducer<T>> producerAsyncIterator = AsyncIterators.transform(individualChunks.iterator(),
				new AsyncFunction<AggregationChunk, StreamProducer<T>>() {
					@Override
					public void apply(AggregationChunk chunk, ResultCallback<StreamProducer<T>> callback) {
						chunkReaderWithFilter(where, chunk, sequenceClass, queryClassLoader, callback);
					}
				});
		return StreamProducers.concat(eventloop, producerAsyncIterator);
	}

	private <T> void chunkReaderWithFilter(final AggregationPredicate where, AggregationChunk chunk,
	                                       final Class<T> chunkRecordClass, final DefiningClassLoader queryClassLoader,
	                                       final ResultCallback<StreamProducer<T>> callback) {
		aggregationChunkStorage.read(this, getKeys(), chunk.getMeasures(), chunkRecordClass, chunk.getChunkId(), classLoader,
				new ForwardingResultCallback<StreamProducer<T>>(callback) {
					@Override
					protected void onResult(StreamProducer<T> chunkProducer) {
						if (ignoreChunkReadingExceptions) {
							ErrorIgnoringTransformer errorIgnoringTransformer = ErrorIgnoringTransformer.create(eventloop);
							chunkProducer.streamTo(errorIgnoringTransformer.getInput());
							chunkProducer = errorIgnoringTransformer.getOutput();
						}
						if (where != null && where != AggregationPredicates.alwaysTrue()) {
							StreamFilter<T> streamFilter = StreamFilter.create(eventloop,
									createPredicate(chunkRecordClass, where, queryClassLoader));
							chunkProducer.streamTo(streamFilter.getInput());
							chunkProducer = streamFilter.getOutput();
						}
						callback.setResult(chunkProducer);
					}
				});
	}

	private <T> Predicate<T> createPredicate(Class<T> chunkRecordClass,
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
				doConsolidation(chunks, new ForwardingResultCallback<List<AggregationChunk>>(callback) {
					@Override
					public void onResult(final List<AggregationChunk> consolidatedChunks) {
						logger.info("Saving consolidation results to metadata storage in aggregation '{}'", Aggregation.this);
						metadataStorage.saveConsolidatedChunks(chunks, consolidatedChunks, new ForwardingCompletionCallback(callback) {
							@Override
							public void onComplete() {
								logger.info("Completed consolidation of the following chunks ({}) " +
												"in aggregation '{}': [{}]. Created chunks ({}): [{}]",
										chunks.size(), metadata, getChunkIds(chunks),
										consolidatedChunks.size(), getChunkIds(consolidatedChunks));
								callback.setResult(true);
							}
						});
					}
				});
			}
		});
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
	public String toString() {
		return "{" + keyTypes.keySet() + " " + measures.keySet() + '}';
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
