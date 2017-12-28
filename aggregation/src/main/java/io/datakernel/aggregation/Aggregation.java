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

import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.async.Stages;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.datakernel.aggregation.AggregationUtils.*;
import static io.datakernel.codegen.Expressions.arg;
import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Represents an aggregation, which aggregates data using custom reducer and preaggregator.
 * Provides methods for loading and querying data.
 */
@SuppressWarnings("unchecked")
public class Aggregation implements IAggregation, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final int DEFAULT_CHUNK_SIZE = 1_000_000;
	public static final int DEFAULT_SORTER_ITEMS_IN_MEMORY = 1_000_000;
	public static final int DEFAULT_SORTER_BLOCK_SIZE = 256 * 1024;
	public static final long DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS = 10 * 60 * 1000; // 10 minutes
	public static final int DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE = 1000;

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final AggregationChunkStorage aggregationChunkStorage;
	private Path temporarySortDir;

	private final AggregationStructure structure;
	private AggregationState state;

	// settings
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int sorterItemsInMemory = DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private int sorterBlockSize = DEFAULT_SORTER_BLOCK_SIZE;
	private long maxIncrementalReloadPeriodMillis = DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS;
	private boolean ignoreChunkReadingExceptions = false;
	private int maxChunksToConsolidate = DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE;

	// jmx
	private final List<AggregationGroupReducer> activeGroupReducers = new ArrayList<>();
	private final List<AggregationChunker> activeChunkers = new ArrayList<>();
	private long consolidationStarted;
	private long consolidationLastTimeMillis;
	private int consolidations;
	private Throwable consolidationLastError;

	private Aggregation(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                    AggregationChunkStorage aggregationChunkStorage, AggregationStructure structure,
	                    AggregationState state) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.temporarySortDir = temporarySortDir;
		this.structure = structure;
		this.state = state;
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
	 * @param aggregationChunkStorage storage for data chunks
	 */
	public static Aggregation create(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                                 AggregationChunkStorage aggregationChunkStorage, AggregationStructure structure) {
		return new Aggregation(eventloop, executorService, classLoader, aggregationChunkStorage, structure, new AggregationState(structure));
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

	public Aggregation withTemporarySortDir(Path temporarySortDir) {
		this.temporarySortDir = temporarySortDir;
		return this;
	}

	public AggregationStructure getStructure() {
		return structure;
	}

	public AggregationState getState() {
		return state;
	}

	public void setState(AggregationState state) {
		this.state = state;
	}

	public AggregationState detachState() {
		AggregationState state = this.state;
		this.state = null;
		return state;
	}

	public List<String> getKeys() {
		return structure.getKeys();
	}

	public List<String> getMeasures() {
		return structure.getMeasures();
	}

	public Map<String, FieldType> getKeyTypes() {
		return structure.getKeyTypes();
	}

	public Map<String, FieldType> getMeasureTypes() {
		return structure.getMeasureTypes();
	}

	public List<String> getPartitioningKey() {
		return structure.getPartitioningKey();
	}

	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass,
	                                                 List<String> keys, List<String> measures,
	                                                 DefiningClassLoader classLoader) {
		return AggregationUtils.aggregationReducer(structure, inputClass, outputClass,
				keys, measures, classLoader);
	}

	private static <T> Set<T> difference(Set<T> a, Set<T> b) {
		Set<T> set = new HashSet<>(a);
		set.removeAll(b);
		return set;
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to aggregation
	 */
	@SuppressWarnings("unchecked")
	public <T> CompletionStage<AggregationDiff> consume(StreamProducer<T> producer,
	                                                    Class<T> inputClass, Map<String, String> keyFields, Map<String, String> measureFields) {
		checkArgument(new HashSet<>(getKeys()).equals(keyFields.keySet()), "Expected keys: %s, actual keyFields: %s", getKeys(), keyFields);
		checkArgument(getMeasureTypes().keySet().containsAll(measureFields.keySet()), "Unknown measures: %s", difference(measureFields.keySet(), getMeasureTypes().keySet()));

		logger.info("Started consuming data in aggregation {}. Keys: {} Measures: {}", this, keyFields.keySet(), measureFields.keySet());

		Class<?> keyClass = createKeyClass(structure, getKeys(), classLoader);
		final Set<String> measureFieldKeys = measureFields.keySet();
		List<String> measures = this.getMeasureTypes().keySet().stream().filter(measureFieldKeys::contains).collect(toList());

		Class<?> accumulatorClass = createRecordClass(structure, getKeys(), measures, classLoader);

		Aggregate aggregate = createPreaggregator(structure, inputClass, accumulatorClass,
				keyFields, measureFields,
				classLoader);

		AggregationGroupReducer<T> groupReducer = new AggregationGroupReducer<>(eventloop, aggregationChunkStorage,
				structure, measures,
				accumulatorClass,
				createPartitionPredicate(accumulatorClass, getPartitioningKey(), classLoader),
				createKeyFunction(inputClass, keyClass, getKeys(), classLoader),
				aggregate, chunkSize, classLoader);

		producer.streamTo(groupReducer);

		return groupReducer.getResult().thenApply(chunks -> AggregationDiff.of(new HashSet<>(chunks)));
	}

	public <T> CompletionStage<AggregationDiff> consume(StreamProducer<T> producer, Class<T> inputClass) {
		return consume(producer, inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass));
	}

	public double estimateCost(AggregationQuery query) {
		final List<String> measures = getMeasures();
		List<String> aggregationFields = query.getMeasures().stream().filter(measures::contains).collect(toList());
		return state.findChunks(query.getPredicate(), aggregationFields).size();
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
		final List<String> measures = query.getMeasures();
		List<String> fields = getMeasures().stream().filter(measures::contains).collect(toList());

		List<AggregationChunk> allChunks = state.findChunks(query.getPredicate(), fields);

		return consolidatedProducer(query.getKeys(),
				fields, outputClass, query.getPredicate(), allChunks, queryClassLoader);
	}

	private <T> StreamProducer<T> sortStream(StreamProducer<T> unsortedStream, Class<T> resultClass,
	                                         List<String> allKeys, List<String> measures, DefiningClassLoader classLoader) {
		Comparator keyComparator = createKeyComparator(resultClass, allKeys, classLoader);
		BufferSerializer bufferSerializer = createBufferSerializer(structure, resultClass,
				getKeys(), measures, classLoader);
		if (temporarySortDir == null) {
			try {
				temporarySortDir = Files.createTempDirectory("aggregation_sort_dir");
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
		StreamSorterStorage sorterStorage = StreamSorterStorageImpl.create(eventloop, executorService,
				bufferSerializer, temporarySortDir);
		StreamSorter sorter = StreamSorter.create(eventloop, sorterStorage, o -> o, keyComparator, false,
				sorterItemsInMemory);
		unsortedStream.streamTo(sorter.getInput());
		return sorter.getOutput();
	}

	private CompletionStage<List<AggregationChunk>> doConsolidation(List<AggregationChunk> chunksToConsolidate) {
		Set<String> aggregationFields = new HashSet<>(getMeasures());
		Set<String> chunkFields = new HashSet<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String measure : chunk.getMeasures()) {
				if (aggregationFields.contains(measure))
					chunkFields.add(measure);
			}
		}

		final List<String> measures = getMeasures().stream().filter(chunkFields::contains).collect(toList());
		final Class resultClass = createRecordClass(structure, getKeys(), measures, classLoader);

		StreamProducer<Object> consolidatedProducer = consolidatedProducer(getKeys(), measures, resultClass, AggregationPredicates.alwaysTrue(), chunksToConsolidate, classLoader);
		AggregationChunker<Object> chunker = AggregationChunker.create(eventloop,
				structure, measures, resultClass,
				createPartitionPredicate(resultClass, getPartitioningKey(), classLoader),
				aggregationChunkStorage, classLoader);
		consolidatedProducer.streamTo(chunker);

		return chunker.getResult();
	}

	private static void addChunkToPlan(Map<List<String>, TreeMap<PrimaryKey, List<QueryPlan.Sequence>>> planIndex,
	                                   AggregationChunk chunk, List<String> queryFields) {
		queryFields = new ArrayList<>(queryFields);
		queryFields.retainAll(chunk.getMeasures());
		checkArgument(!queryFields.isEmpty());
		TreeMap<PrimaryKey, List<QueryPlan.Sequence>> map = planIndex.computeIfAbsent(queryFields, k -> new TreeMap<>());

		Map.Entry<PrimaryKey, List<QueryPlan.Sequence>> entry = map.lowerEntry(chunk.getMinPrimaryKey());
		QueryPlan.Sequence sequence;
		if (entry == null) {
			sequence = new QueryPlan.Sequence(queryFields);
		} else {
			List<QueryPlan.Sequence> list = entry.getValue();
			sequence = list.remove(list.size() - 1);
			if (list.isEmpty()) {
				map.remove(entry.getKey());
			}
		}
		sequence.add(chunk);
		List<QueryPlan.Sequence> list = map.computeIfAbsent(chunk.getMaxPrimaryKey(), k -> new ArrayList<>());
		list.add(sequence);
	}

	private static QueryPlan createPlan(List<AggregationChunk> chunks, List<String> queryFields) {
		Map<List<String>, TreeMap<PrimaryKey, List<QueryPlan.Sequence>>> index = new HashMap<>();
		chunks = new ArrayList<>(chunks);
		chunks.sort(comparing(AggregationChunk::getMinPrimaryKey));
		for (AggregationChunk chunk : chunks) {
			addChunkToPlan(index, chunk, queryFields);
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
		QueryPlan plan = createPlan(individualChunks, measures);

		logger.info("Query plan for {} in aggregation {}: {}", queryKeys, this, plan);

		boolean alreadySorted = this.getKeys().subList(0, min(this.getKeys().size(), queryKeys.size())).equals(queryKeys);

		List<SequenceStream> sequenceStreams = new ArrayList<>();

		for (QueryPlan.Sequence sequence : plan.getSequences()) {
			Class<?> sequenceClass = createRecordClass(structure, this.getKeys(), sequence.getChunksFields(), classLoader);

			StreamProducer stream = sequenceStream(where, sequence.getChunks(), sequenceClass, queryClassLoader);
			if (!alreadySorted) {
				stream = sortStream(stream, sequenceClass, queryKeys, sequence.getQueryFields(), classLoader);
			}

			sequenceStreams.add(new SequenceStream(stream, sequence.getQueryFields(), sequenceClass));
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
		if (sequences.size() == 1 && new HashSet<>(queryKeys).equals(new HashSet<>(getKeys()))) {
			/*
			If there is only one sequential producer and all aggregation keys are requested, then there is no need for
			using StreamReducer, because all records have unique keys and all we need to do is copy requested measures
			from record class to result class.
			 */
			SequenceStream sequence = sequences.get(0);
			final List<String> collect = measures.stream().filter(sequence.fields::contains).collect(toList());
			StreamMap.MapperProjection mapper = createMapper(sequence.type, resultClass, queryKeys, collect, classLoader);
			StreamMap<Object, T> streamMap = StreamMap.create(eventloop, mapper);
			sequence.stream.streamTo(streamMap.getInput());
			return streamMap.getOutput();
		}

		StreamReducer<Comparable, T, Object> streamReducer = StreamReducer.create(eventloop, Comparable::compareTo);

		Class<?> keyClass = createKeyClass(structure, queryKeys, this.classLoader);

		for (SequenceStream sequence : sequences) {
			Function extractKeyFunction = createKeyFunction(sequence.type, keyClass, queryKeys, this.classLoader);

			final List<String> collect = measures.stream().filter(sequence.fields::contains).collect(toList());
			StreamReducers.Reducer reducer = AggregationUtils.aggregationReducer(structure, sequence.type, resultClass,
					queryKeys, collect, classLoader);

			sequence.stream.streamTo(streamReducer.newInput(extractKeyFunction, reducer));
		}

		return streamReducer.getOutput();
	}

	private <T> StreamProducer<T> sequenceStream(AggregationPredicate where,
	                                             List<AggregationChunk> individualChunks, Class<T> sequenceClass,
	                                             DefiningClassLoader queryClassLoader) {
		Iterator<AggregationChunk> chunkIterator = individualChunks.iterator();
		return StreamProducers.concat(eventloop, new Iterator<StreamProducer<T>>() {
			@Override
			public boolean hasNext() {
				return chunkIterator.hasNext();
			}

			@Override
			public StreamProducer<T> next() {
				AggregationChunk chunk = chunkIterator.next();
				return chunkReaderWithFilter(where, chunk, sequenceClass, queryClassLoader);
			}
		});
	}

	private <T> StreamProducer<T> chunkReaderWithFilter(final AggregationPredicate where, AggregationChunk chunk,
	                                                    final Class<T> chunkRecordClass, final DefiningClassLoader queryClassLoader) {
		StreamProducer<T> producer = aggregationChunkStorage.readStream(structure, chunk.getMeasures(), chunkRecordClass, chunk.getChunkId(), classLoader);
		if (where != AggregationPredicates.alwaysTrue()) {
			StreamFilter<T> streamFilter = StreamFilter.create(eventloop,
					createPredicate(chunkRecordClass, where, queryClassLoader));
			producer.streamTo(streamFilter.getInput());
			return streamFilter.getOutput();
		}
		return producer;
	}

	private <T> Predicate<T> createPredicate(Class<T> chunkRecordClass,
	                                         AggregationPredicate where, DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Predicate.class)
				.withMethod("test", boolean.class, singletonList(Object.class),
						where.createPredicateDef(cast(arg(0), chunkRecordClass), getKeyTypes()))
				.buildClassAndCreateNewInstance();
	}

	@JmxAttribute
	public int getNumberOfOverlappingChunks() {
		return state.findOverlappingChunks().size();
	}

	public CompletionStage<AggregationDiff> consolidateMinKey() {
		return doConsolidate(false);
	}

	public CompletionStage<AggregationDiff> consolidateHotSegment() {
		return doConsolidate(true);
	}

	private CompletionStage<AggregationDiff> doConsolidate(boolean hotSegment) {
		final List<AggregationChunk> chunks = hotSegment ?
				state.findChunksForConsolidationHotSegment(maxChunksToConsolidate) :
				state.findChunksForConsolidationMinKey(maxChunksToConsolidate, chunkSize);

		if (chunks.isEmpty()) {
			logger.info("Nothing to consolidate in aggregation '{}", this);
			return Stages.of(AggregationDiff.empty());
		}

		logger.info("Starting consolidation of aggregation '{}'", this);
		consolidationStarted = eventloop.currentTimeMillis();

		return doConsolidation(chunks)
				.whenComplete(($, throwable) -> {
					if (throwable == null) {
						consolidationLastTimeMillis = eventloop.currentTimeMillis() - consolidationStarted;
						consolidations++;
					} else {
						consolidationStarted = 0;
						consolidationLastError = throwable;
					}
				})
				.thenApply(removedChunks -> AggregationDiff.of(new LinkedHashSet<>(removedChunks), new LinkedHashSet<>(chunks)));
	}

	public static String getChunkIds(Iterable<AggregationChunk> chunks) {
		List<Long> ids = new ArrayList<>();
		for (AggregationChunk chunk : chunks) {
			ids.add(chunk.getChunkId());
		}
		return ids.stream().map(Object::toString).collect(Collectors.joining(", "));
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
	public Throwable getConsolidationLastError() {
		return consolidationLastError;
	}

	@JmxAttribute
	public int getChunks() {
		return state.getChunks().size();
	}

	@Override
	public String toString() {
		return "{" + getKeyTypes().keySet() + " " + getMeasureTypes().keySet() + '}';
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
