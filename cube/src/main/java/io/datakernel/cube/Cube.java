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
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.datakernel.aggregation_db.*;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.sort;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
@SuppressWarnings("unchecked")
public final class Cube {
	private static final Logger logger = LoggerFactory.getLogger(Cube.class);

	private final Eventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final CubeMetadataStorage cubeMetadataStorage;
	private final AggregationMetadataStorage aggregationMetadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final int aggregationChunkSize;
	private final int sorterItemsInMemory;
	private final int consolidationTimeoutMillis;
	private final int removeChunksAfterConsolidationMillis;

	private final AggregationStructure structure;

	private Map<String, Aggregation> aggregations = new LinkedHashMap<>();

	private int lastRevisionId;

	/**
	 * Instantiates a cube with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages,
	 * and uses the specified parameters.
	 *
	 * @param eventloop                            event loop, in which the cube is to run
	 * @param classLoader                          class loader for defining dynamic classes
	 * @param cubeMetadataStorage                  storage for persisting cube metadata
	 * @param aggregationMetadataStorage           storage for aggregations metadata
	 * @param aggregationChunkStorage              storage for data chunks
	 * @param structure                            structure of a cube
	 * @param aggregationChunkSize                 maximum size of aggregation chunk
	 * @param sorterItemsInMemory                  maximum number of records that can stay in memory while sorting
	 * @param consolidationTimeoutMillis           maximum duration of consolidation attempt (in milliseconds)
	 * @param removeChunksAfterConsolidationMillis period of time (in milliseconds) after consolidation after which consolidated chunks can be removed
	 */
	public Cube(Eventloop eventloop, DefiningClassLoader classLoader, CubeMetadataStorage cubeMetadataStorage,
	            AggregationMetadataStorage aggregationMetadataStorage, AggregationChunkStorage aggregationChunkStorage,
	            AggregationStructure structure, int aggregationChunkSize, int sorterItemsInMemory, int consolidationTimeoutMillis,
	            int removeChunksAfterConsolidationMillis) {
		this.eventloop = eventloop;
		this.classLoader = classLoader;
		this.cubeMetadataStorage = cubeMetadataStorage;
		this.aggregationMetadataStorage = aggregationMetadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
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
	 * @param eventloop                  event loop, in which the cube is to run
	 * @param classLoader                class loader for defining dynamic classes
	 * @param cubeMetadataStorage        storage for persisting cube metadata
	 * @param aggregationMetadataStorage storage for aggregations metadata
	 * @param aggregationChunkStorage    storage for data chunks
	 * @param structure                  structure of a cube
	 */
	public Cube(Eventloop eventloop, DefiningClassLoader classLoader,
	            CubeMetadataStorage cubeMetadataStorage, AggregationMetadataStorage aggregationMetadataStorage,
	            AggregationChunkStorage aggregationChunkStorage, AggregationStructure structure) {
		this(eventloop, classLoader, cubeMetadataStorage, aggregationMetadataStorage, aggregationChunkStorage, structure,
				1_000_000, 1_000_000, 30 * 60 * 1000, 10 * 60 * 1000);
	}

	public Map<String, Aggregation> getAggregations() {
		return aggregations;
	}

	public AggregationStructure getStructure() {
		return structure;
	}

	/**
	 * Adds the given aggregation.
	 *
	 * @param aggregation aggregation to add
	 */
	public void addAggregation(Aggregation aggregation) {
		aggregations.put(aggregation.getId(), aggregation);
	}

	/**
	 * Creates an {@link Aggregation} with the specified metadata and adds it to the collection of aggregations managed by this cube.
	 *
	 * @param aggregationMetadata metadata of aggregation
	 */
	public void addAggregation(AggregationMetadata aggregationMetadata) {
		Aggregation aggregation = new Aggregation(eventloop, classLoader, aggregationMetadataStorage, aggregationChunkStorage, aggregationMetadata, structure,
				new SummationProcessorFactory(classLoader), sorterItemsInMemory, aggregationChunkSize,
				consolidationTimeoutMillis, removeChunksAfterConsolidationMillis);
		aggregations.put(aggregation.getId(), aggregation);
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
	                                      final CommitCallback callback) {
		return consumer(inputClass, dimensions, measures, null, callback);
	}

	private Collection<Aggregation> findAggregationsForWriting(final AggregationQuery.QueryPredicates predicates) {
		Collection<Aggregation> allAggregations = aggregations.values();

		if (predicates == null)
			// return aggregations without predicates
			return newArrayList(filter(allAggregations, new Predicate<Aggregation>() {
				@Override
				public boolean apply(Aggregation aggregation) {
					return !aggregation.hasPredicates();
				}
			}));
		else
			// return aggregations with matching predicates
			return newArrayList(filter(allAggregations, new Predicate<Aggregation>() {
				@Override
				public boolean apply(Aggregation aggregation) {
					return aggregation.matchQueryPredicates(predicates);
				}
			}));
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
	                                      AggregationQuery.QueryPredicates predicates,
	                                      final CommitCallback callback) {
		logger.trace("Started building StreamConsumer for populating cube {}.", this);

		final StreamSplitter<T> streamSplitter = new StreamSplitter<>(eventloop);
		final Multimap<AggregationMetadata, AggregationChunk.NewChunk> resultChunks = LinkedHashMultimap.create();
		final int[] aggregationsDone = {0};

		Collection<Aggregation> preparedAggregations = findAggregationsForWriting(predicates);

		for (final Aggregation aggregation : preparedAggregations) {
			if (!aggregation.allKeysIn(dimensions))
				continue;

			List<String> aggregationMeasures = aggregation.getAggregationFieldsForConsumer(measures);
			if (aggregationMeasures.isEmpty())
				continue;

			StreamConsumer<T> groupReducer = aggregation.consumer(inputClass, aggregationMeasures,
					new ForwardingResultCallback<List<AggregationChunk.NewChunk>>(callback) {
						@Override
						public void onResult(List<AggregationChunk.NewChunk> chunks) {
							resultChunks.putAll(aggregation.getAggregationMetadata(), chunks);
							++aggregationsDone[0];
							if (aggregationsDone[0] == streamSplitter.getOutputsCount()) {
								callback.onCommit(resultChunks);
							}
						}
					});

			streamSplitter.newOutput().streamTo(groupReducer);
		}

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

	private Comparator<Aggregation> aggregationCostComparator(final AggregationQuery query) {
		return new Comparator<Aggregation>() {
			@Override
			public int compare(Aggregation aggregation1, Aggregation aggregation2) {
				return Double.compare(aggregation1.getCost(query), aggregation2.getCost(query));
			}
		};
	}

	private Comparator<Aggregation> descendingNumberOfPredicatesComparator() {
		return new Comparator<Aggregation>() {
			@Override
			public int compare(Aggregation aggregation1, Aggregation aggregation2) {
				return Integer.compare(aggregation2.getNumberOfPredicates(), aggregation1.getNumberOfPredicates());
			}
		};
	}

	private List<Aggregation> findAggregationsForQuery(final AggregationQuery query) {
		List<Aggregation> allAggregations = newArrayList(aggregations.values());
		Collections.sort(allAggregations, descendingNumberOfPredicatesComparator());

		List<Aggregation> aggregationsThatSatisfyPredicates = newArrayList();
		List<Aggregation> aggregationsWithoutPredicates = newArrayList();

		for (Aggregation aggregation : allAggregations) {
			boolean satisfiesPredicates = aggregation.applyQueryPredicates(query, structure);

			if (satisfiesPredicates) {
				aggregationsThatSatisfyPredicates.add(aggregation);
			} else if (!aggregation.hasPredicates()) {
				aggregationsWithoutPredicates.add(aggregation);
			}
		}

		Comparator<Aggregation> aggregationCostComparator = aggregationCostComparator(query);

		sort(aggregationsWithoutPredicates, aggregationCostComparator);

		return newArrayList(concat(aggregationsThatSatisfyPredicates, aggregationsWithoutPredicates));
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from cube for the specified query.
	 *
	 * @param revisionId       minimum revision of records
	 * @param resultClass      class of output records
	 * @param aggregationQuery query
	 * @param <T>              type of output objects
	 * @return producer that streams query results
	 */
	public <T> StreamProducer<T> query(int revisionId, Class<T> resultClass, AggregationQuery aggregationQuery) {
		logger.trace("Started building StreamProducer for query.");
		final AggregationQuery query = aggregationQuery.copyWithDuplicatedPredicatesAndKeys();

		StreamReducer<Comparable, T, Object> streamReducer = new StreamReducer<>(eventloop, Ordering.natural());

		List<Aggregation> preparedAggregations = findAggregationsForQuery(query);

		List<String> queryMeasures = newArrayList(query.getResultFields());
		List<String> resultDimensions = query.getResultKeys();
		Class resultKeyClass = structure.createKeyClass(resultDimensions);

		for (Aggregation aggregation : preparedAggregations) {
			if (queryMeasures.isEmpty())
				break;
			if (!aggregation.containsKeys(query.getAllKeys()))
				continue;
			List<String> aggregationMeasures = aggregation.getAggregationFieldsForQuery(queryMeasures);

			if (aggregationMeasures.isEmpty())
				continue;

			Class aggregationClass = structure.createRecordClass(aggregation.getKeys(), aggregationMeasures);

			StreamProducer<T> queryResultProducer = aggregation.query(revisionId, query, aggregationClass);

			Function keyFunction = structure.createKeyFunction(aggregationClass, resultKeyClass, resultDimensions);

			StreamReducers.Reducer reducer = structure.mergeFieldsReducer(aggregationClass, resultClass,
					resultDimensions, aggregationMeasures);

			StreamConsumer streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			queryResultProducer.streamTo(streamReducerInput);

			logger.trace("Streaming query {} result from aggregation {}.", query, aggregation);

			queryMeasures = newArrayList(filter(queryMeasures, not(in(aggregation.getInputFields()))));
		}

		checkArgument(queryMeasures.isEmpty());

		final StreamProducer<T> orderedResultStream = getOrderedResultStream(query, resultClass, streamReducer,
				query.getResultKeys(), query.getResultFields());

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

	/**
	 * Asynchronously saves metadata on this cube's aggregations.
	 *
	 * @param callback callback which is called when saving to metadata storage is completed
	 */
	public void saveAggregations(CompletionCallback callback) {
		cubeMetadataStorage.saveAggregations(this, callback);
	}

	public void loadAggregations(CompletionCallback callback) {
		cubeMetadataStorage.loadAggregations(this, callback);
	}

	public void reloadAllChunksConsolidations(CompletionCallback callback) {
		CompletionCallback waitAllCallback = AsyncCallbacks.waitAll(aggregations.size(), callback);
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.reloadAllChunksConsolidations(waitAllCallback);
		}
	}

	public void refreshChunkConsolidations(CompletionCallback callback) {
		CompletionCallback waitAllCallback = AsyncCallbacks.waitAll(aggregations.size(), callback);
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.refreshChunkConsolidations(waitAllCallback);
		}
	}

	public void refreshAllChunks(CompletionCallback callback) {
		refreshAllChunks(Integer.MAX_VALUE, callback);
	}

	public void refreshAllChunks(int maxRevisionId, CompletionCallback callback) {
		CompletionCallback waitAllCallback = AsyncCallbacks.waitAll(aggregations.size(), callback);
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.loadChunks(maxRevisionId, waitAllCallback);
		}
	}

	public void consolidateAllIndexes(CompletionCallback completionCallback) {
		CompletionCallback waitAllCallback = AsyncCallbacks.waitAll(aggregations.size(), completionCallback);
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.consolidate(waitAllCallback);
		}
	}

	public void consolidateGreedily(CompletionCallback completionCallback) {
		TreeMap<Integer, Aggregation> indexNumberOfChunks = new TreeMap<>(Collections.reverseOrder());

		for (Aggregation aggregation : aggregations.values()) {
			int numberOfChunksAvailableForConsolidation = aggregation.getNumberOfChunksAvailableForConsolidation();
			if (numberOfChunksAvailableForConsolidation != 0) {
				indexNumberOfChunks.put(numberOfChunksAvailableForConsolidation, aggregation);
			}
		}

		Map.Entry<Integer, Aggregation> indexNumberOfChunksEntry = indexNumberOfChunks.firstEntry();
		if (indexNumberOfChunksEntry != null) {
			indexNumberOfChunksEntry.getValue().consolidate(completionCallback);
		}
	}

	public void consolidateGreedily(ConsolidateCallback consolidateCallback) {
		TreeMap<Integer, Aggregation> indexNumberOfChunks = new TreeMap<>(Collections.reverseOrder());

		for (Aggregation aggregation : aggregations.values()) {
			int numberOfChunksAvailableForConsolidation = aggregation.getNumberOfChunksAvailableForConsolidation();
			indexNumberOfChunks.put(numberOfChunksAvailableForConsolidation, aggregation);
		}

		indexNumberOfChunks.firstEntry().getValue().consolidate(consolidateCallback);
	}

	public void removeOldChunksFromAllIndexes(CompletionCallback completionCallback) {
		CompletionCallback waitAllCallback = AsyncCallbacks.waitAll(aggregations.size(), completionCallback);
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.removeOldChunks(waitAllCallback);
		}
	}

	public AvailableDrillDowns getAvailableDrillDowns(Set<String> dimensions, AggregationQuery.QueryPredicates predicates,
	                                                  Set<String> measures) {
		Set<String> queryDimensions = newHashSet();
		Set<String> availableMeasures = newHashSet();
		Set<String> availableDimensions = newHashSet();

		queryDimensions.addAll(dimensions);
		for (AggregationQuery.QueryPredicate predicate : predicates.asCollection()) {
			if (predicate instanceof AggregationQuery.QueryPredicateEq) {
				queryDimensions.add(predicate.key);
			}
		}

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getInputFields());

			if (!all(queryDimensions, in(aggregation.getKeys())))
				continue;

			if (!any(measures, in(aggregationMeasures)))
				continue;

			Sets.intersection(aggregationMeasures, measures).copyInto(availableMeasures);

			availableDimensions.addAll(newArrayList(filter(aggregation.getKeys(), not(in(queryDimensions)))));
		}

		Set<List<String>> drillDownChains = structure.getParentChildRelationships().buildDrillDownChains(dimensions, availableDimensions);

		return new AvailableDrillDowns(drillDownChains, availableMeasures);
	}

	public Set<String> findChildrenDimensions(String parent) {
		return structure.getParentChildRelationships().findChildren(parent);
	}

	public List<String> buildDrillDownChain(Set<String> usedDimensions, String dimension) {
		return structure.getParentChildRelationships().buildDrillDownChain(usedDimensions, dimension);
	}

	public Set<List<String>> buildDrillDownChains(Set<String> usedDimensions, Set<String> availableDimensions) {
		return structure.getParentChildRelationships().buildDrillDownChains(usedDimensions, availableDimensions);
	}

	public Set<String> getAvailableMeasures(List<String> dimensions, List<String> allMeasures) {
		Set<String> availableMeasures = newHashSet();
		Set<String> allMeasuresSet = newHashSet();
		allMeasuresSet.addAll(allMeasures);

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getInputFields());

			if (!all(dimensions, in(aggregation.getKeys()))) {
				continue;
			}

			if (!any(allMeasures, in(aggregation.getInputFields()))) {
				continue;
			}

			Sets.intersection(aggregationMeasures, allMeasuresSet).copyInto(availableMeasures);
		}

		return availableMeasures;
	}

	private <T> StreamProducer<T> getOrderedResultStream(AggregationQuery query, Class<T> resultClass,
	                                                     StreamReducer<Comparable, T, Object> rawResultStream,
	                                                     List<String> dimensions, List<String> measures) {
		if (queryRequiresSorting(query)) {
			ArrayList<String> orderingFields = new ArrayList<>(query.getOrderingFields());
			Class<?> fieldClass = structure.createFieldClass(orderingFields);
			Function sortingMeasureFunction = structure.createFieldFunction(resultClass, fieldClass, orderingFields);
			Comparator fieldComparator = structure.createFieldComparator(query, fieldClass);

			StreamMergeSorterStorage sorterStorage = SorterStorageUtils.getSorterStorage(eventloop, structure, resultClass, dimensions, measures);
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

	private boolean queryRequiresSorting(AggregationQuery query) {
		int orderings = query.getOrderings().size();
		return orderings != 0;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("structure", structure)
				.add("aggregations", aggregations)
				.add("lastRevisionId", lastRevisionId)
				.toString();
	}
}
