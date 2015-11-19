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
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.datakernel.aggregation_db.*;
import io.datakernel.cube.api.AttributeResolver;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.api.ReportingConfiguration;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
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

	private final AggregationStructure structure;
	private ReportingConfiguration reportingConfiguration = new ReportingConfiguration();

	private Map<String, Aggregation> aggregations = new LinkedHashMap<>();

	private int lastRevisionId;

	/**
	 * Instantiates a cube with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages,
	 * and uses the specified parameters.
	 * @param eventloop                            event loop, in which the cube is to run
	 * @param classLoader                          class loader for defining dynamic classes
	 * @param cubeMetadataStorage                  storage for persisting cube metadata
	 * @param aggregationMetadataStorage           storage for aggregations metadata
	 * @param aggregationChunkStorage              storage for data chunks
	 * @param structure                            structure of a cube
	 * @param aggregationChunkSize                 maximum size of aggregation chunk
	 * @param sorterItemsInMemory                  maximum number of records that can stay in memory while sorting
	 */
	public Cube(Eventloop eventloop, DefiningClassLoader classLoader, CubeMetadataStorage cubeMetadataStorage,
	            AggregationMetadataStorage aggregationMetadataStorage, AggregationChunkStorage aggregationChunkStorage,
	            AggregationStructure structure, int aggregationChunkSize, int sorterItemsInMemory) {
		this.eventloop = eventloop;
		this.classLoader = classLoader;
		this.cubeMetadataStorage = cubeMetadataStorage;
		this.aggregationMetadataStorage = aggregationMetadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.structure = structure;
		this.aggregationChunkSize = aggregationChunkSize;
		this.sorterItemsInMemory = sorterItemsInMemory;
	}

	public Map<String, Aggregation> getAggregations() {
		return aggregations;
	}

	public AggregationStructure getStructure() {
		return structure;
	}

	public ReportingConfiguration getReportingConfiguration() {
		return reportingConfiguration;
	}

	public void setReportingConfiguration(ReportingConfiguration reportingConfiguration) {
		this.reportingConfiguration = reportingConfiguration;
		initResolverKeys();
	}

	public void initResolverKeys() {
		for (Map.Entry<String, String> attributeDimension : reportingConfiguration.getAttributeDimensions().entrySet()) {
			List<String> key = buildDrillDownChain(attributeDimension.getValue());
			reportingConfiguration.setKeyForAttribute(attributeDimension.getKey(), key);
		}
	}

	public Map<String, AttributeResolver> getResolvers() {
		return reportingConfiguration == null ? new HashMap<String, AttributeResolver>() : reportingConfiguration.getResolvers();
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
				new SummationProcessorFactory(classLoader), sorterItemsInMemory, aggregationChunkSize);
		checkArgument(!aggregations.containsKey(aggregation.getId()), "Aggregation '%s' is already defined", aggregation.getId());
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
		final int[] streamSplitterOutputs = {0};

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
							if (aggregationsDone[0] == streamSplitterOutputs[0]) {
								callback.onCommit(resultChunks);
							}
						}
					});

			streamSplitter.newOutput().streamTo(groupReducer);
			++streamSplitterOutputs[0];
		}

		return streamSplitter.getInput();
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

	private Map<Aggregation, List<String>> findAggregationsForQuery(final AggregationQuery query) {
		Map<String, List<String>> aggregationIdToAppliedPredicateKeys = new HashMap<>();
		Map<Aggregation, List<String>> aggregationToAppliedPredicateKeys = new LinkedHashMap<>();
		List<Aggregation> allAggregations = newArrayList(aggregations.values());
		Collections.sort(allAggregations, descendingNumberOfPredicatesComparator());

		List<Aggregation> aggregationsThatSatisfyPredicates = newArrayList();
		List<Aggregation> aggregationsWithoutPredicates = newArrayList();

		for (Aggregation aggregation : allAggregations) {
			AggregationFilteringResult result = aggregation.applyQueryPredicates(query, structure);
			aggregationIdToAppliedPredicateKeys.put(aggregation.getId(), result.getAppliedPredicateKeys());
			boolean satisfiesPredicates = result.isMatches();

			if (satisfiesPredicates) {
				aggregationsThatSatisfyPredicates.add(aggregation);
			} else if (!aggregation.hasPredicates()) {
				aggregationsWithoutPredicates.add(aggregation);
			}
		}

		Comparator<Aggregation> aggregationCostComparator = aggregationCostComparator(query);

		sort(aggregationsWithoutPredicates, aggregationCostComparator);

		List<Aggregation> resultAggregations = newArrayList(concat(aggregationsThatSatisfyPredicates, aggregationsWithoutPredicates));

		for (Aggregation aggregation : resultAggregations) {
			aggregationToAppliedPredicateKeys.put(aggregation, aggregationIdToAppliedPredicateKeys.get(aggregation.getId()));
		}

		return aggregationToAppliedPredicateKeys;
	}

	public AggregationQuery getQueryWithoutAppliedPredicateKeys(AggregationQuery query, List<String> appliedPredicateKeys) {
		Map<String, AggregationQuery.QueryPredicate> filteredQueryPredicates = new LinkedHashMap<>();
		for (Map.Entry<String, AggregationQuery.QueryPredicate> queryPredicateEntry : query.getPredicates().asMap().entrySet()) {
			if (!appliedPredicateKeys.contains(queryPredicateEntry.getKey()))
				filteredQueryPredicates.put(queryPredicateEntry.getKey(), queryPredicateEntry.getValue());
		}

		return new AggregationQuery(query.getResultKeys(), query.getResultFields(), AggregationQuery.QueryPredicates.fromMap(filteredQueryPredicates), query.getOrderings());
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from cube for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param resultClass class of output records
	 * @param query       query
	 * @return producer that streams query results
	 */
	public <T> StreamProducer<T> query(Class<T> resultClass, AggregationQuery query) {
		logger.trace("Started building StreamProducer for query.");

		StreamReducer<Comparable, T, Object> streamReducer = new StreamReducer<>(eventloop, Ordering.natural());

		Map<Aggregation, List<String>> aggregationsToAppliedPredicateKeys = findAggregationsForQuery(query);

		List<String> queryMeasures = newArrayList(query.getResultFields());
		List<String> resultDimensions = query.getResultKeys();
		Class resultKeyClass = structure.createKeyClass(resultDimensions);

		for (Map.Entry<Aggregation, List<String>> entry : aggregationsToAppliedPredicateKeys.entrySet()) {
			Aggregation aggregation = entry.getKey();
			AggregationQuery filteredQuery = getQueryWithoutAppliedPredicateKeys(query, entry.getValue());
			if (queryMeasures.isEmpty())
				break;
			if (!aggregation.containsKeys(filteredQuery.getAllKeys()))
				continue;
			List<String> aggregationMeasures = aggregation.getAggregationFieldsForQuery(queryMeasures);

			if (aggregationMeasures.isEmpty())
				continue;

			Class aggregationClass = structure.createRecordClass(aggregation.getKeys(), aggregationMeasures);

			StreamProducer<T> queryResultProducer = aggregation.query(filteredQuery, aggregationClass);

			Function keyFunction = structure.createKeyFunction(aggregationClass, resultKeyClass, resultDimensions);

			StreamReducers.Reducer reducer = structure.mergeFieldsReducer(aggregationClass, resultClass,
					resultDimensions, aggregationMeasures);

			StreamConsumer streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			queryResultProducer.streamTo(streamReducerInput);

			logger.info("Streaming query {} result from aggregation '{}'", filteredQuery, aggregation.getId());

			queryMeasures = newArrayList(filter(queryMeasures, not(in(aggregation.getOutputFields()))));
		}

		checkArgument(queryMeasures.isEmpty());

		final StreamProducer<T> orderedResultStream = getOrderedResultStream(query, resultClass, streamReducer,
				query.getResultKeys(), query.getResultFields());

		logger.trace("Finished building StreamProducer for query.");

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

	public void loadChunks(CompletionCallback callback) {
		CompletionCallback waitAllCallback = AsyncCallbacks.waitAll(aggregations.size(), callback);
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.loadChunks(waitAllCallback);
		}
	}

	public void consolidate(int maxChunksToConsolidate, ResultCallback<Boolean> callback) {
		consolidate(maxChunksToConsolidate, false, new ArrayList<>(this.aggregations.values()).iterator(), callback);
	}

	private void consolidate(final int maxChunksToConsolidate, final boolean found, final Iterator<Aggregation> iterator,
	                         final ResultCallback<Boolean> callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				if (iterator.hasNext()) {
					Aggregation aggregation = iterator.next();
					aggregation.consolidate(maxChunksToConsolidate, new ForwardingResultCallback<Boolean>(callback) {
						@Override
						public void onResult(Boolean result) {
							consolidate(maxChunksToConsolidate, result || found, iterator, callback);
						}
					});
				} else {
					callback.onResult(found);
				}
			}
		});
	}

	public AvailableDrillDowns getAvailableDrillDowns(Set<String> dimensions, AggregationQuery.QueryPredicates predicates,
	                                                  Set<String> measures) {
		Set<String> availableMeasures = newHashSet();
		Set<String> availableDimensions = newHashSet();
		Set<String> eqPredicateDimensions = newHashSet();

		AggregationQuery query = new AggregationQuery(newArrayList(dimensions), newArrayList(measures), predicates);

		for (AggregationQuery.QueryPredicate predicate : predicates.asCollection()) {
			if (predicate instanceof AggregationQuery.QueryPredicateEq) {
				eqPredicateDimensions.add(predicate.key);
			}
		}

		List<String> queryDimensions = newArrayList(concat(dimensions, eqPredicateDimensions));

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getOutputFields());

			if (!all(queryDimensions, in(aggregation.getKeys())))
				continue;

			if (!any(measures, in(aggregationMeasures)))
				continue;

			if (aggregation.hasPredicates() && !aggregation.applyQueryPredicates(query, structure).isMatches())
				continue;

			Sets.intersection(aggregationMeasures, measures).copyInto(availableMeasures);

			availableDimensions.addAll(newArrayList(filter(aggregation.getKeys(), not(in(queryDimensions)))));
		}

		Set<List<String>> drillDownChains = structure.getChildParentRelationships().buildDrillDownChains(dimensions, availableDimensions);

		return new AvailableDrillDowns(drillDownChains, availableMeasures);
	}

	public Set<String> findChildrenDimensions(String parent) {
		return structure.getChildParentRelationships().findChildren(parent);
	}

	public List<String> buildDrillDownChain(Set<String> usedDimensions, String dimension) {
		return structure.getChildParentRelationships().buildDrillDownChain(usedDimensions, dimension);
	}

	public List<String> buildDrillDownChain(String dimension) {
		return buildDrillDownChain(Sets.<String>newHashSet(), dimension);
	}

	public Set<String> getAvailableMeasures(List<String> dimensions, List<String> allMeasures) {
		Set<String> availableMeasures = newHashSet();
		Set<String> allMeasuresSet = newHashSet();
		allMeasuresSet.addAll(allMeasures);

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getOutputFields());

			if (!all(dimensions, in(aggregation.getKeys()))) {
				continue;
			}

			if (!any(allMeasures, in(aggregation.getOutputFields()))) {
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
			Comparator fieldComparator = structure.createFieldComparator(query, resultClass);
			StreamMergeSorterStorage sorterStorage = SorterStorageUtils.getSorterStorage(eventloop, structure, resultClass, dimensions, measures);
			StreamSorter sorter = new StreamSorter(eventloop, sorterStorage, Functions.identity(),
					fieldComparator, false, sorterItemsInMemory);
			rawResultStream.getOutput().streamTo(sorter.getInput());
			return sorter.getOutput();
		} else {
			return rawResultStream.getOutput();
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