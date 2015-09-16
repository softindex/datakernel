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
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.codegen.AsmFunctionFactory;
import io.datakernel.codegen.PredicateDefAnd;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static java.util.Arrays.asList;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for defining aggregations and managing chunks (consolidation, for example).
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class Cube {
	private static final Logger logger = LoggerFactory.getLogger(Cube.class);

	private final Eventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final CubeMetadataStorage metadataStorage;
	private final AggregationStorage aggregationStorage;
	private final int aggregationChunkSize;
	private final int sorterItemsInMemory;
	private final int consolidationTimeoutMillis;
	private final int removeChunksAfterConsolidationMillis;

	private final CubeStructure structure;

	private final Map<String, Aggregation> aggregations = new LinkedHashMap<>();
	private final Map<Long, AggregationChunk> chunks = new LinkedHashMap<>();

	private int lastRevisionId;

	/**
	 * Instantiates a cube with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages,
	 * and uses the specified parameters.
	 *
	 * @param eventloop                            event loop, in which the cube is to run
	 * @param classLoader                          class loader for defining dynamic classes
	 * @param metadataStorage                      storage for persisting metadata
	 * @param aggregationStorage                   storage for aggregations (data chunks)
	 * @param structure                            structure of a cube
	 * @param aggregationChunkSize                 maximum size of aggregation chunk
	 * @param sorterItemsInMemory                  maximum number of records that can stay in memory while sorting
	 * @param consolidationTimeoutMillis           maximum duration of consolidation attempt (in milliseconds)
	 * @param removeChunksAfterConsolidationMillis period of time (in milliseconds) after consolidation after which consolidated chunks can be removed
	 */
	public Cube(Eventloop eventloop, DefiningClassLoader classLoader, CubeMetadataStorage metadataStorage,
	            AggregationStorage aggregationStorage, CubeStructure structure, int aggregationChunkSize,
	            int sorterItemsInMemory, int consolidationTimeoutMillis, int removeChunksAfterConsolidationMillis) {
		this.eventloop = eventloop;
		this.classLoader = classLoader;
		this.metadataStorage = metadataStorage;
		this.aggregationStorage = aggregationStorage;
		this.structure = structure;
		this.aggregationChunkSize = aggregationChunkSize;
		this.sorterItemsInMemory = sorterItemsInMemory;
		this.consolidationTimeoutMillis = consolidationTimeoutMillis;
		this.removeChunksAfterConsolidationMillis = removeChunksAfterConsolidationMillis;
	}

	/**
	 * Instantiates a cube with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages.
	 * Maximum size of chunk is 1,000,000 bytes.
	 * No more than 1,000,000 records stay in memory while sorting.
	 * Maximum duration of consolidation attempt is 30 minutes.
	 * Consolidated chunks become available for removal in 10 minutes from consolidation.
	 *
	 * @param eventloop          event loop, in which the cube is to run
	 * @param classLoader        class loader for defining dynamic classes
	 * @param metadataStorage    storage for persisting metadata
	 * @param aggregationStorage storage for aggregations (data chunks)
	 * @param structure          structure of a cube
	 */
	public Cube(Eventloop eventloop, DefiningClassLoader classLoader,
	            CubeMetadataStorage metadataStorage, AggregationStorage aggregationStorage, CubeStructure structure) {
		this(eventloop, classLoader, metadataStorage, aggregationStorage, structure, 1_000_000, 1_000_000,
				30 * 60 * 1000, 10 * 60 * 1000);
	}

	public Map<String, Aggregation> getAggregations() {
		return aggregations;
	}

	public CubeStructure getStructure() {
		return structure;
	}

	public Map<Long, AggregationChunk> getChunks() {
		return Collections.unmodifiableMap(chunks);
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

	public void addChunk(AggregationChunk chunk) {
		chunks.put(chunk.getChunkId(), chunk);
	}

	public void removeChunk(long chunkId) {
		chunks.remove(chunkId);
	}

	/**
	 * Adds the given aggregation.
	 *
	 * @param aggregation aggregation to add
	 */
	public void addAggregation(Aggregation aggregation) {
		aggregations.put(aggregation.getId(), aggregation);
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> dimensions, List<String> measures, final CommitCallback callback) {
		logger.trace("Started building StreamConsumer for populating cube {}.", this);

		final StreamSplitter<T> streamSplitter = new StreamSplitter<>(eventloop);
		final Multimap<Aggregation, AggregationChunk.NewChunk> resultChunks = LinkedHashMultimap.create();
		final int[] aggregationsDone = {0};

		for (final Aggregation aggregation : aggregations.values()) {
			if (!all(aggregation.getDimensions(), in(dimensions)))
				continue;

			List<String> aggregationMeasures = newArrayList(filter(aggregation.getMeasures(), in(measures)));
			if (aggregationMeasures.isEmpty())
				continue;

			Class<?> keyClass = structure.createKeyClass(aggregation.getDimensions());
			Class<?> aggregationClass = structure.createRecordClass(aggregation.getDimensions(), aggregationMeasures);

			Function<T, Comparable<?>> keyFunction = structure.createKeyFunction(inputClass, keyClass, aggregation.getDimensions());

			Aggregate aggregate = structure.createAggregate(inputClass, aggregationClass, aggregation.getDimensions(), aggregationMeasures);

			AggregationGroupReducer<T> aggregationGroupReducer = new AggregationGroupReducer<>(eventloop, aggregationStorage, metadataStorage, aggregation,
					aggregation.getDimensions(), aggregationMeasures,
					aggregationClass, keyFunction, aggregate, new ForwardingResultCallback<List<AggregationChunk.NewChunk>>(callback) {
				@Override
				public void onResult(List<AggregationChunk.NewChunk> chunks) {
					resultChunks.putAll(aggregation, chunks);
					++aggregationsDone[0];
					if (aggregationsDone[0] == streamSplitter.getOutputsCount()) {
						callback.onCommit(resultChunks);
					}
				}
			}, aggregationChunkSize);

			streamSplitter.newOutput().streamTo(aggregationGroupReducer);
		}

		logger.trace("Finished building StreamConsumer for populating cube {}. Aggregation chunk size: {}", this, aggregationChunkSize);

		streamSplitter.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.trace("Populating cube {} completed.", Cube.this);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Populating cube {} failed.", Cube.this, e);
			}
		});

		return streamSplitter;
	}

	public void consolidate(final CompletionCallback callback) {
		consolidate(new ForwardingConsolidateCallback(callback) {
			@Override
			public void onConsolidate(Aggregation aggregation,
			                          List<AggregationChunk> originalChunks,
			                          List<AggregationChunk.NewChunk> consolidatedChunks) {
				metadataStorage.saveConsolidatedChunks(Cube.this, aggregation, originalChunks, consolidatedChunks, new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						callback.onComplete();
					}
				});
			}

			@Override
			public void onNothingToConsolidate() {
				callback.onComplete();
			}
		});
	}

	/**
	 * Asynchronously saves metadata on this cube's aggregations.
	 *
	 * @param callback callback which saving to metadata storage is completed
	 */
	public void saveAggregations(CompletionCallback callback) {
		metadataStorage.saveAggregations(this, callback);
	}

	public void loadAggregations(CompletionCallback callback) {
		metadataStorage.loadAggregations(this, callback);
	}

	public void loadChunks(int maxRevisionId, final CompletionCallback callback) {
		metadataStorage.loadChunks(this, lastRevisionId, maxRevisionId, new ForwardingResultCallback<Integer>(callback) {
			@Override
			public void onResult(Integer result) {
				Cube.this.lastRevisionId = result;
				callback.onComplete();
			}
		});
	}

	public void reloadAllChunksConsolidations(CompletionCallback callback) {
		metadataStorage.reloadAllChunkConsolidations(this, callback);
	}

	public void loadChunks(CompletionCallback callback) {
		loadChunks(Integer.MAX_VALUE, callback);
	}

	public void refreshChunkConsolidations(CompletionCallback callback) {
		metadataStorage.refreshNotConsolidatedChunks(this, callback);
	}

	@SuppressWarnings("rawtypes")
	private Predicate createPredicate(Aggregation aggregation, AggregationChunk chunk,
	                                  Class<?> chunkRecordClass, CubeQuery.CubePredicates predicates) {
		List<String> dimensionsAlreadyInChunk = new ArrayList<>();
		for (int i = 0; i < aggregation.getDimensions().size(); i++) {
			String dimension = aggregation.getDimensions().get(i);
			Object min = chunk.getMinPrimaryKey().get(i);
			Object max = chunk.getMaxPrimaryKey().get(i);
			if (!min.equals(max)) {
				break;
			}
			dimensionsAlreadyInChunk.add(dimension);
		}

		AsmFunctionFactory functionFactory = new AsmFunctionFactory(classLoader, Predicate.class);
		PredicateDefAnd predicateDefAnd = and();

		for (CubeQuery.CubePredicate predicate : predicates.predicates()) {
			if (dimensionsAlreadyInChunk.contains(predicate.dimension))
				continue;
			if (predicate instanceof CubeQuery.CubePredicateEq) {
				Object value = ((CubeQuery.CubePredicateEq) predicate).value;

				predicateDefAnd.add(cmpEq(
						field(cast(arg(0), chunkRecordClass), predicate.dimension),
						value(value)));
			} else if (predicate instanceof CubeQuery.CubePredicateBetween) {
				Object from = ((CubeQuery.CubePredicateBetween) predicate).from;
				Object to = ((CubeQuery.CubePredicateBetween) predicate).to;

				predicateDefAnd.add(cmpGe(
						field(cast(arg(0), chunkRecordClass), predicate.dimension),
						value(from)));

				predicateDefAnd.add(cmpLe(
						field(cast(arg(0), chunkRecordClass), predicate.dimension),
						value(to)));
			} else {
				throw new IllegalArgumentException("Unsupported predicate " + predicate);
			}
		}
//		if (predicateDefFieldsList.isEmpty())
//			return alwaysTrue();
		functionFactory.method("apply", boolean.class, asList(Object.class), predicateDefAnd);
		return (Predicate) functionFactory.newInstance();
	}

	private StreamProducer chunkReaderWithFilter(Aggregation aggregation, CubeQuery.CubePredicates predicates, AggregationChunk chunk, Class<?> chunkRecordClass) {
		StreamProducer chunkReader = aggregationStorage.chunkReader(aggregation.getId(), aggregation.getDimensions(), chunk.getMeasures(), chunkRecordClass, chunk.getChunkId());
		if (predicates == null)
			return chunkReader;
		StreamFilter streamFilter = new StreamFilter<>(eventloop,
				createPredicate(aggregation, chunk, chunkRecordClass, predicates));
		chunkReader.streamTo(streamFilter);
		return streamFilter;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private <T> StreamProducer<T> mergeProducers(List<String> dimensions,
	                                             List<String> measures,
	                                             Class<?> resultClass,
	                                             List<StreamProducer> producers,
	                                             List<List<String>> producersMeasures,
	                                             List<Class<?>> producerClasses) {
		StreamReducer<Comparable, T, Object> streamReducer = new StreamReducer<>(eventloop, Ordering.natural());

		Class<?> keyClass = structure.createKeyClass(dimensions);

		for (int i = 0; i < producers.size(); i++) {
			StreamProducer producer = producers.get(i);

			Function extractKeyFunction = structure.createKeyFunction(producerClasses.get(i), keyClass, dimensions);

			StreamReducers.Reducer reducer = structure.aggregationReducer(producerClasses.get(i), resultClass,
					dimensions,
					newArrayList(filter(measures, in(producersMeasures.get(i))))
					// producersMetrics.get(i) // TODO ?
			);

			producer.streamTo(streamReducer.newInput(extractKeyFunction, reducer));
		}
		return streamReducer;
	}

	private StreamProducer sequentialProducer(Aggregation aggregation,
	                                          CubeQuery.CubePredicates predicates,
	                                          List<AggregationChunk> individualChunks,
	                                          Class<?> sequenceClass) {
		checkArgument(!individualChunks.isEmpty());
		List<StreamProducer<Object>> producers = new ArrayList<>();
		for (AggregationChunk chunk : individualChunks) {
			producers.add(chunkReaderWithFilter(aggregation, predicates, chunk, sequenceClass));
		}
		return StreamProducers.concat(eventloop, producers);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private <T> StreamProducer<T> consolidatedProducer(Aggregation aggregation,
	                                                   List<String> dimensions,
	                                                   List<String> measures,
	                                                   Class<T> resultClass,
	                                                   CubeQuery.CubePredicates predicates,
	                                                   List<AggregationChunk> individualChunks) {
		individualChunks = newArrayList(individualChunks);
		Collections.sort(individualChunks, new Comparator<AggregationChunk>() {
			@Override
			public int compare(AggregationChunk chunk1, AggregationChunk chunk2) {
				return chunk1.getMinPrimaryKey().compareTo(chunk2.getMinPrimaryKey());
			}
		});

		List<StreamProducer> producers = new ArrayList<>();
		List<List<String>> producersMeasures = new ArrayList<>();
		List<Class<?>> producersClasses = new ArrayList<>();

		List<AggregationChunk> chunks = new ArrayList<>();

		for (int i = 0; i <= individualChunks.size(); i++) {
			AggregationChunk chunk = (i != individualChunks.size()) ? individualChunks.get(i) : null;

			boolean nextSequence = chunks.isEmpty() || chunk == null ||
					getLast(chunks).getMaxPrimaryKey().compareTo(chunk.getMinPrimaryKey()) >= 0 ||
					!getLast(chunks).getMeasures().equals(chunk.getMeasures());

			if (nextSequence && !chunks.isEmpty()) {
				Class<?> chunksClass = structure.createRecordClass(aggregation.getDimensions(),
						chunks.get(0).getMeasures());

				producersMeasures.add(chunks.get(0).getMeasures());
				producersClasses.add(chunksClass);

				StreamProducer producer = sequentialProducer(aggregation, predicates, chunks, chunksClass);
				producers.add(producer);

				chunks.clear();
			}

			if (chunk != null) {
				chunks.add(chunk);
			}
		}

		return mergeProducers(dimensions, measures, resultClass, producers, producersMeasures, producersClasses);
	}

	public AvailableDrillDowns getAvailableDrillDowns(Set<String> dimensions, Set<CubeQuery.CubePredicate> predicates,
	                                                  Set<String> measures) {
		Set<String> queryDimensions = newHashSet();
		Set<String> availableMeasures = newHashSet();
		Set<String> availableDimensions = newHashSet();

		queryDimensions.addAll(dimensions);
		for (CubeQuery.CubePredicate predicate : predicates) {
			if (predicate instanceof CubeQuery.CubePredicateEq) {
				queryDimensions.add(predicate.dimension);
			}
		}

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getMeasures());

			if (!all(queryDimensions, in(aggregation.getDimensions())))
				continue;

			if (!any(measures, in(aggregationMeasures)))
				continue;

			Sets.intersection(aggregationMeasures, measures).copyInto(availableMeasures);

			availableDimensions.addAll(newArrayList(filter(aggregation.getDimensions(), not(in(queryDimensions)))));
		}

		return new AvailableDrillDowns(availableDimensions, availableMeasures);
	}

	public Set<String> getAvailableMeasures(List<String> dimensions, List<String> allMeasures) {
		Set<String> availableMeasures = newHashSet();
		Set<String> allMeasuresSet = newHashSet();
		allMeasuresSet.addAll(allMeasures);

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getMeasures());

			if (!all(dimensions, in(aggregation.getDimensions()))) {
				continue;
			}

			if (!any(allMeasures, in(aggregation.getMeasures()))) {
				continue;
			}

			Sets.intersection(aggregationMeasures, allMeasuresSet).copyInto(availableMeasures);
		}

		return availableMeasures;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> StreamProducer<T> query(int revisionId, Class<T> resultClass, CubeQuery query) {
		logger.trace("Started building StreamProducer for query.");

		structure.checkThatDimensionsExist(query.getAllDimensions());
		structure.checkThatMeasuresExist(query.getAllMeasures());

		StreamReducer<Comparable, T, Object> streamReducer = new StreamReducer<>(eventloop, Ordering.natural());
		List<String> queryMeasures = newArrayList(query.getResultMeasures());
		List<String> resultDimensions = query.getResultDimensions();
		Class<?> resultKeyClass = structure.createKeyClass(resultDimensions);

		for (Aggregation aggregation : aggregations.values()) {
			if (queryMeasures.isEmpty())
				break;
			if (!all(query.getAllDimensions(), in(aggregation.getDimensions())))
				continue;
			List<String> aggregationMeasures = newArrayList(filter(queryMeasures, in(aggregation.getMeasures())));

			if (aggregationMeasures.isEmpty())
				continue;

			List<AggregationChunk> allChunks = aggregation.queryByPredicates(structure, chunks, revisionId, query.getPredicates());

			Class<?> aggregationClass = structure.createRecordClass(aggregation.getDimensions(), aggregationMeasures);

			Function keyFunction = structure.createKeyFunction(aggregationClass, resultKeyClass, resultDimensions);

			StreamProducer streamProducer = consolidatedProducer(aggregation, query.getAllDimensions(),
					aggregationMeasures, aggregationClass, query.getPredicates(), allChunks);

			StreamReducers.Reducer reducer = structure.mergeMeasuresReducer(aggregationClass, resultClass, resultDimensions, aggregationMeasures);
			StreamConsumer streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			if (aggregationSortingRequired(resultDimensions, aggregation.getDimensions())) {
				StreamMergeSorterStorage sorterStorage = getSorterStorage(aggregationClass, aggregation.getDimensions(), aggregationMeasures);
				StreamSorter sorter = new StreamSorter(eventloop, sorterStorage, keyFunction, Ordering.natural(), false, sorterItemsInMemory);
				streamProducer.streamTo(sorter);
				sorter.getSortedStream().streamTo(streamReducerInput);
			} else {
				streamProducer.streamTo(streamReducerInput);
			}

			queryMeasures = newArrayList(filter(queryMeasures, not(in(aggregation.getMeasures()))));
		}

		checkArgument(queryMeasures.isEmpty());

		final StreamProducer<T> orderedResultStream = getOrderedResultStream(query, resultClass, streamReducer, query.getResultDimensions(), query.getResultMeasures());

		logger.trace("Finished building StreamProducer for query.");

		orderedResultStream.addCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.trace("Streaming query result from stream {} completed.", orderedResultStream);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Streaming query result from stream {} failed.", orderedResultStream, e);
			}
		});

		return orderedResultStream;
	}

	private <T> StreamMergeSorterStorage getSorterStorage(Class<T> recordClass, List<String> dimensions, List<String> measures) {
		String sorterStorageDirectory = "sorterStorage";
		Path path = Paths.get(sorterStorageDirectory, "%d.part");
		int blockSize = 64;
		ExecutorService executorService = Executors.newCachedThreadPool();

		BufferSerializer bufferSerializer = structure.createBufferSerializer(recordClass, dimensions, measures);

		return new StreamMergeSorterStorageImpl(eventloop, executorService, bufferSerializer, path, blockSize);
	}

	private <T> StreamProducer<T> getOrderedResultStream(CubeQuery query, Class<T> resultClass,
	                                                     StreamReducer<Comparable, T, Object> rawResultStream,
	                                                     List<String> dimensions, List<String> measures) {
		if (queryRequiresSortings(query)) {
			ArrayList<String> orderingFields = new ArrayList<>(query.getOrderingFields());
			Class<?> fieldClass = structure.createFieldClass(orderingFields);
			Function sortingMeasureFunction = structure.createFieldFunction(resultClass, fieldClass, orderingFields);
			Comparator fieldComparator = structure.createFieldComparator(query, fieldClass);

			StreamMergeSorterStorage sorterStorage = getSorterStorage(resultClass, dimensions, measures);
			StreamSorter sorter = new StreamSorter(eventloop, sorterStorage, sortingMeasureFunction,
					fieldComparator, false, sorterItemsInMemory);
			rawResultStream.streamTo(sorter);
			StreamForwarder<T> sortedStream = (StreamForwarder<T>) sorter.getSortedStream();
			sortedStream.setTag(query);
			return sortedStream;
		} else {
			rawResultStream.setTag(query);
			return rawResultStream;
		}
	}

	private boolean queryRequiresSortings(CubeQuery query) {
		int orderings = query.getOrderings().size();
		return orderings != 0;
	}

	private boolean aggregationSortingRequired(List<String> resultDimensions, List<String> aggregationDimensions) {
		boolean resultDimensionsAreSubset = !all(aggregationDimensions, in(resultDimensions));
		return resultDimensionsAreSubset && !isPrefix(resultDimensions, aggregationDimensions);
	}

	private boolean isPrefix(List<String> fields1, List<String> fields2) {
		checkArgument(fields1.size() <= fields2.size());
		for (int i = 0; i < fields1.size(); ++i) {
			String resultDimension = fields1.get(i);
			String aggregationDimension = fields2.get(i);
			if (!resultDimension.equals(aggregationDimension)) {
				// not prefix
				return false;
			}
		}
		return true;
	}

	public List<Long> getIdsOfNotConsolidatedChunks() {
		List<Long> ids = newArrayList();

		for (Map.Entry<Long, AggregationChunk> chunkEntry : chunks.entrySet()) {
			if (!chunkEntry.getValue().isConsolidated()) {
				ids.add(chunkEntry.getKey());
			}
		}

		return ids;
	}

	public List<Long> getIdsOfChunksAvailableForConsolidation() {
		List<Long> ids = newArrayList();

		for (Map.Entry<Long, AggregationChunk> chunkEntry : chunks.entrySet()) {
			AggregationChunk chunk = chunkEntry.getValue();
			Long chunkId = chunkEntry.getKey();
			boolean chunkIsConsolidated = chunk.isConsolidated();

			if (chunkIsConsolidated) {
				continue;
			}

			Timestamp consolidationStarted = chunk.getConsolidationStarted();

			if (consolidationStarted == null) {
				ids.add(chunkId);
				continue;
			}

			long consolidationTimeoutTimestamp = consolidationStarted.getTime() + consolidationTimeoutMillis;
			long currentTimestamp = eventloop.currentTimeMillis();

			if (currentTimestamp > consolidationTimeoutTimestamp) {
				ids.add(chunkId);
			}
		}

		return ids;
	}

	public void removeOldChunks() {
		for (Map.Entry<Long, AggregationChunk> chunkEntry : chunks.entrySet()) {
			final AggregationChunk chunk = chunkEntry.getValue();
			if (chunk.isConsolidated()) {
				long currentTimestamp = eventloop.currentTimeMillis();
				long chunkRemovalTimestamp = chunk.getConsolidationCompleted().getTime() + removeChunksAfterConsolidationMillis;

				if (currentTimestamp > chunkRemovalTimestamp) {
					final long chunkId = chunk.getChunkId();
					final String aggregationId = chunk.getAggregationId();
					final Aggregation chunkAggregation = aggregations.get(aggregationId);
					metadataStorage.removeChunk(chunkId, new CompletionCallback() {
						@Override
						public void onComplete() {
							chunkAggregation.removeFromIndex(chunk);
							aggregationStorage.removeChunk(aggregationId, chunkId);
							logger.info("Removed chunk #{}", chunkId);
						}

						@Override
						public void onException(Exception exception) {
							logger.error("Removal of chunk #{} failed.", chunkId, exception);
						}
					});
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void consolidate(final ConsolidateCallback callback) {
		Function<List<Long>, List<AggregationChunk>> chunkConsolidator = new Function<List<Long>, List<AggregationChunk>>() {
			@Override
			public List<AggregationChunk> apply(List<Long> consolidationCandidateChunkIds) {
				Aggregation foundAggregation = null;
				List<AggregationChunk> foundChunksToConsolidate = Collections.emptyList();

				for (Aggregation partialAggregation : Cube.this.aggregations.values()) {
					List<AggregationChunk> chunksToConsolidate = partialAggregation.findChunksToConsolidate(consolidationCandidateChunkIds);
					if (chunksToConsolidate.size() > foundChunksToConsolidate.size()) {
						foundChunksToConsolidate = chunksToConsolidate;
						foundAggregation = partialAggregation;
					}
				}

				if (foundAggregation == null) {
					callback.onNothingToConsolidate();
					return new ArrayList<>();
				}

				final List<String> measures = new ArrayList<>();
				for (AggregationChunk chunk : foundChunksToConsolidate) {
					for (String measure : chunk.getMeasures()) {
						if (!measures.contains(measure)) {
							measures.add(measure);
						}
					}
				}

				final Class<?> resultClass = structure.createRecordClass(foundAggregation.getDimensions(), measures);

				final Aggregation finalAggregation = foundAggregation;
				final List<AggregationChunk> finalChunksToConsolidate = foundChunksToConsolidate;

				final Aggregation finalFoundAggregation = foundAggregation;
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						consolidatedProducer(finalFoundAggregation, finalFoundAggregation.getDimensions(), measures, resultClass,
								null, finalChunksToConsolidate)
								.streamTo(new AggregationChunker(eventloop, finalFoundAggregation.getId(), finalFoundAggregation.getDimensions(), measures, resultClass, aggregationStorage, metadataStorage,
										new ForwardingResultCallback<List<AggregationChunk.NewChunk>>(callback) {
											@Override
											public void onResult(List<AggregationChunk.NewChunk> consolidatedChunks) {
												callback.onConsolidate(finalAggregation,
														finalChunksToConsolidate, consolidatedChunks);
											}
										}, aggregationChunkSize));

					}
				});

				return finalChunksToConsolidate;
			}
		};

		metadataStorage.performConsolidation(this, chunkConsolidator, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.trace("Cube {} consolidation successfully started.", this);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Cube {} consolidation failed to start.", this, exception);
			}
		});
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("structure", structure)
				.add("aggregations", aggregations)
				.add("chunks", chunks)
				.add("lastRevisionId", lastRevisionId)
				.toString();
	}
}
