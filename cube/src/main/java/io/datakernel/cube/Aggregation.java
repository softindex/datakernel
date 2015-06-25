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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.datakernel.cube.dimensiontype.DimensionType;
import io.datakernel.cube.dimensiontype.DimensionTypeEnumerable;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Describes cube aggregation. Stores chunks in an index (represented by an array of {@code RangeTree}) for efficient search.
 * Provides methods for managing index, querying for chunks by key, searching for chunks that are available for consolidation.
 */
public final class Aggregation {
	private final String id;
	private final ImmutableList<String> dimensions;
	private final ImmutableList<String> measures;

	private final RangeTree<PrimaryKey, AggregationChunk>[] prefixRanges;

	private static final int EQUALS_QUERIES_THRESHOLD = 1_000;

	/**
	 * Constructs an aggregation with the given id, dimensions and measures.
	 *
	 * @param id         id of aggregation
	 * @param dimensions list of dimension names
	 * @param measures   of measure names
	 */
	@SuppressWarnings("unchecked")
	public Aggregation(String id, List<String> dimensions, List<String> measures) {
		this.id = id;
		this.dimensions = ImmutableList.copyOf(dimensions);
		this.measures = ImmutableList.copyOf(measures);
		this.prefixRanges = new RangeTree[dimensions.size() + 1];
		for (int size = 0; size <= dimensions.size(); size++) {
			this.prefixRanges[size] = new RangeTree<>();
		}
	}

	public void addToIndex(AggregationChunk chunk) {
		checkArgument(measures.containsAll(chunk.getMeasures()));
//		this.chunks.put(chunk.getChunkId(), chunk);
		for (int size = 0; size <= dimensions.size(); size++) {
			RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];

			PrimaryKey lower = chunk.getMinPrimaryKey().prefix(size);
			PrimaryKey upper = chunk.getMaxPrimaryKey().prefix(size);
			index.put(lower, upper, chunk);
		}
	}

	public void removeFromIndex(AggregationChunk chunk) {
		for (int size = 0; size <= dimensions.size(); size++) {
			RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];

			PrimaryKey lower = chunk.getMinPrimaryKey().prefix(size);
			PrimaryKey upper = chunk.getMaxPrimaryKey().prefix(size);
			index.remove(lower, upper, chunk);
		}
	}

/*
	public void consolidate(List<AggregationChunk> originalChunks, List<AggregationChunk> consolidatedChunks) {
		for (AggregationChunk originalChunk : originalChunks) {
			remove(originalChunk);
		}
		for (AggregationChunk chunk : consolidatedChunks) {
			add(chunk);
		}
	}
*/

	public String getId() {
		return id;
	}

	public List<String> getDimensions() {
		return dimensions;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public List<AggregationChunk> findChunksToConsolidate(final List<Long> consolidationCandidateChunksIds) {
		int maxOverlaps = 2;
		Set<AggregationChunk> set = Collections.emptySet();
//		Set<AggregationChunk> closing = Collections.emptySet();
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[dimensions.size()];
		for (Map.Entry<PrimaryKey, RangeTree.Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			RangeTree.Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = segment.getSet().size();
			if (overlaps >= maxOverlaps) {
				maxOverlaps = overlaps;
				set = segment.getSet();
//				closing = segment.getClosingSet();
			}
		}
		List<AggregationChunk> result = new ArrayList<>();
		result.addAll(set);
//		result.addAll(closing);
		return Lists.newArrayList(Iterables.filter(result, new Predicate<AggregationChunk>() {
			@Override
			public boolean apply(AggregationChunk chunk) {
				return consolidationCandidateChunksIds.contains(chunk.getChunkId());
			}
		}));
	}

	private List<AggregationChunk> rangeQuery(Map<Long, AggregationChunk> chunks,
	                                          long revisionId, PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey) {
		checkArgument(minPrimaryKey.size() == maxPrimaryKey.size());
		int size = minPrimaryKey.size();
		RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];
		Collection<AggregationChunk> currentChunks = index.getRange(minPrimaryKey, maxPrimaryKey);

		List<AggregationChunk> resultChunks = new ArrayList<>();
		while (!currentChunks.isEmpty()) {
			List<AggregationChunk> newChunks = new ArrayList<>();
			for (AggregationChunk currentChunk : currentChunks) {
				if (currentChunk.getMaxRevisionId() <= revisionId)
					continue;
				if (currentChunk.getMinRevisionId() > revisionId) {
					resultChunks.add(currentChunk);
					continue;
				}
				for (Long sourceChunkId : currentChunk.getSourceChunkIds()) {
					AggregationChunk sourceChunk = chunks.get(sourceChunkId);
					if (sourceChunk.getMinPrimaryKey().prefix(size).compareTo(maxPrimaryKey) <= 0 &&
							sourceChunk.getMaxPrimaryKey().prefix(size).compareTo(minPrimaryKey) >= 0) {
						newChunks.add(sourceChunk);
					}
				}
			}
			currentChunks = newChunks;
		}
		return resultChunks;
	}

	private Predicate<CubeQuery.CubePredicate> isCubePredicateBetween() {
		return new Predicate<CubeQuery.CubePredicate>() {
			@Override
			public boolean apply(CubeQuery.CubePredicate predicate) {
				return predicate instanceof CubeQuery.CubePredicateBetween;
			}
		};
	}

	private Predicate<AggregationChunk> chunkMightContainQueryValuesPredicate(final PrimaryKey minQueryKey,
	                                                                          final PrimaryKey maxQueryKey) {
		return new Predicate<AggregationChunk>() {
			@Override
			public boolean apply(AggregationChunk chunk) {
				List<Object> queryMinValues = minQueryKey.values();
				List<Object> queryMaxValues = maxQueryKey.values();
				List<Object> chunkMinValues = chunk.getMinPrimaryKey().values();
				List<Object> chunkMaxValues = chunk.getMaxPrimaryKey().values();

				return chunkMightContainQueryValues(queryMinValues, queryMaxValues, chunkMinValues, chunkMaxValues);
			}
		};
	}

	@VisibleForTesting
	static boolean chunkMightContainQueryValues(PrimaryKey minQueryKey, PrimaryKey maxQueryKey,
	                                            PrimaryKey minChunkKey, PrimaryKey maxChunkKey) {
		return chunkMightContainQueryValues(minQueryKey.values(), maxQueryKey.values(),
				minChunkKey.values(), maxChunkKey.values());
	}

	@SuppressWarnings("unchecked")
	private static boolean chunkMightContainQueryValues(List<Object> queryMinValues, List<Object> queryMaxValues,
	                                                    List<Object> chunkMinValues, List<Object> chunkMaxValues) {
		checkArgument(queryMinValues.size() == queryMaxValues.size());
		checkArgument(chunkMinValues.size() == chunkMaxValues.size());

		for (int i = 0; i < queryMinValues.size(); ++i) {
			Comparable<Object> queryMinValue = (Comparable<Object>) queryMinValues.get(i);
			Comparable<Object> queryMaxValue = (Comparable<Object>) queryMaxValues.get(i);
			Comparable<Object> chunkMinValue = (Comparable<Object>) chunkMinValues.get(i);
			Comparable<Object> chunkMaxValue = (Comparable<Object>) chunkMaxValues.get(i);

			if (chunkMinValue.compareTo(chunkMaxValue) == 0) {
				if (!(queryMinValue.compareTo(chunkMinValue) <= 0 && queryMaxValue.compareTo(chunkMaxValue) >= 0)) {
					return false;
				}
			} else {
				return queryMinValue.compareTo(chunkMaxValue) <= 0 && queryMaxValue.compareTo(chunkMinValue) >= 0;
			}
		}

		return true;
	}

	private long countNumberOfEqualsQueries(List<CubeQuery.CubePredicate> betweenPredicates, CubeStructure structure) {
		long queries = 0;

		for (CubeQuery.CubePredicate predicate : betweenPredicates) {
			CubeQuery.CubePredicateBetween predicateBetween = (CubeQuery.CubePredicateBetween) predicate;
			DimensionType dimensionType = structure.getDimensionType(predicate.dimension);
			long difference = ((DimensionTypeEnumerable) dimensionType).difference(predicateBetween.to, predicateBetween.from) + 1;

			if (queries == 0)
				queries += difference;
			else
				queries *= difference;
		}

		return queries;
	}

	private boolean isEnumerable(String dimension, CubeStructure structure) {
		return structure.getDimensionType(dimension) instanceof DimensionTypeEnumerable;
	}

	private boolean areAllDimensionTypesEnumerable(List<CubeQuery.CubePredicate> predicates, CubeStructure structure) {
		for (CubeQuery.CubePredicate predicate : predicates) {
			if (!isEnumerable(predicate.dimension, structure)) {
				return false;
			}
		}

		return true;
	}

	private boolean shouldConvertBetweenPredicatesToEqualsQueries(List<CubeQuery.CubePredicate> betweenPredicates,
	                                                              CubeStructure structure) {
		if (!areAllDimensionTypesEnumerable(betweenPredicates, structure))
			return false;

		long numberOfEqualsQueries = countNumberOfEqualsQueries(betweenPredicates, structure);

		return numberOfEqualsQueries <= EQUALS_QUERIES_THRESHOLD;
	}

	private List<CubeQuery.CubePredicate> getPrefixPredicates(CubeQuery.CubePredicates predicates) {
		List<CubeQuery.CubePredicate> prefixPredicates = Lists.newArrayList();

		for (String dimension : dimensions) {
			CubeQuery.CubePredicate predicateForDimension = predicates.map().get(dimension);
			if (predicateForDimension != null)
				prefixPredicates.add(predicateForDimension);
			else
				break;
		}

		return prefixPredicates;
	}

	public List<AggregationChunk> queryByPredicates(CubeStructure structure, final Map<Long, AggregationChunk> chunks,
	                                                long revisionId, CubeQuery.CubePredicates predicates) {
		List<CubeQuery.CubePredicate> prefixPredicates = getPrefixPredicates(predicates);
		List<CubeQuery.CubePredicate> betweenPredicates = Lists.newArrayList(Iterables.filter(prefixPredicates,
				isCubePredicateBetween()));
		boolean containsBetweenPredicates = betweenPredicates.size() > 0;

		if (!containsBetweenPredicates) {
			return queryByEqualsPredicates(chunks, revisionId, predicates);
		} else if (shouldConvertBetweenPredicatesToEqualsQueries(betweenPredicates, structure)) {
			return queryByConvertingBetweenPredicatesToEqualsQueries(structure, prefixPredicates, chunks, revisionId);
		} else {
			return queryByFilteringListOfChunks(predicates);
		}
	}

	private List<AggregationChunk> queryByEqualsPredicates(Map<Long, AggregationChunk> chunks, long revisionId,
	                                                       CubeQuery.CubePredicates predicates) {
		final PrimaryKey minQueryKey = rangeScanMinPrimaryKeyPrefix(predicates);
		final PrimaryKey maxQueryKey = rangeScanMaxPrimaryKeyPrefix(predicates);
		return rangeQuery(chunks, revisionId, minQueryKey, maxQueryKey);
	}

	private List<AggregationChunk> queryByFilteringListOfChunks(CubeQuery.CubePredicates predicates) {
		PrimaryKey minQueryKey = rangeScanMinPrimaryKeyPrefix(predicates);
		PrimaryKey maxQueryKey = rangeScanMaxPrimaryKeyPrefix(predicates);

		RangeTree<PrimaryKey, AggregationChunk> zeroLengthPrefix = prefixRanges[0];
		Set<AggregationChunk> allAggregationChunks = zeroLengthPrefix.getAll();

		Iterable<AggregationChunk> filteredChunks = Iterables.filter(allAggregationChunks,
				chunkMightContainQueryValuesPredicate(minQueryKey, maxQueryKey));

		return Lists.newArrayList(filteredChunks);
	}

	private List<AggregationChunk> queryByConvertingBetweenPredicatesToEqualsQueries(CubeStructure structure,
	                                                                                 List<CubeQuery.CubePredicate> predicates,
	                                                                                 Map<Long, AggregationChunk> chunks,
	                                                                                 long revisionId) {
		List<PrimaryKey> equalsKeys = primaryKeysForEqualsQueries(structure, predicates);
		Set<AggregationChunk> resultChunks = Sets.newHashSet();

		for (PrimaryKey queryKey : equalsKeys) {
			resultChunks.addAll(rangeQuery(chunks, revisionId, queryKey, queryKey));
		}

		return new ArrayList<>(resultChunks);
	}

	private void transformPredicates(List<CubeQuery.CubePredicate> predicates, CubeStructure structure, List<Set<Object>> betweenSets,
	                                 List<Object> equalsList, boolean[] betweenPredicatePositions) {
		for (int j = 0; j < predicates.size(); ++j) {
			String field = dimensions.get(j);
			CubeQuery.CubePredicate predicate = predicates.get(j);
			DimensionType dimensionType = structure.getDimensionType(field);

			if (predicate instanceof CubeQuery.CubePredicateEq) {
				equalsList.add(((CubeQuery.CubePredicateEq) predicate).value);
			} else if (predicate instanceof CubeQuery.CubePredicateBetween) {
				Object from = ((CubeQuery.CubePredicateBetween) predicate).from;
				Object to = ((CubeQuery.CubePredicateBetween) predicate).to;

				Set<Object> set = new LinkedHashSet<>();

				for (Object i = from; dimensionType.compare(i, to) <= 0;
				     i = ((DimensionTypeEnumerable) dimensionType).increment(i)) {
					set.add(i);
				}

				betweenSets.add(set);
				betweenPredicatePositions[j] = true;
			} else
				break;
		}
	}

	private List<List<Object>> createKeyPrefixes(List<Set<Object>> betweenSets, List<Object> equalsList,
	                                             boolean[] betweenPredicatePositions, int numberOfPredicateDimensions) {
		List<List<Object>> keyPrefixes = new ArrayList<>();

		Set<List<Object>> cartesianProduct = Sets.cartesianProduct(betweenSets);

		for (List<Object> list : cartesianProduct) {
			List<Object> keyPrefix = Lists.newArrayList();
			int betweenListPosition = 0;
			int equalsListPosition = 0;

			for (int k = 0; k < numberOfPredicateDimensions; ++k) {
				boolean betweenPosition = betweenPredicatePositions[k];
				if (betweenPosition)
					keyPrefix.add(list.get(betweenListPosition++));
				else
					keyPrefix.add(equalsList.get(equalsListPosition++));
			}

			keyPrefixes.add(keyPrefix);
		}

		return keyPrefixes;
	}

	public List<PrimaryKey> createPrimaryKeysFromKeyPrefixes(List<List<Object>> keyPrefixList) {
		List<PrimaryKey> keys = new ArrayList<>();

		for (List<Object> keyPrefix : keyPrefixList) {
			keys.add(PrimaryKey.ofList(keyPrefix));
		}

		return keys;
	}

	private List<PrimaryKey> primaryKeysForEqualsQueries(CubeStructure structure, List<CubeQuery.CubePredicate> predicates) {
		int numberOfPredicateDimensions = predicates.size();

		List<Set<Object>> betweenSets = new ArrayList<>(numberOfPredicateDimensions);
		List<Object> equalsList = new ArrayList<>(numberOfPredicateDimensions);

		boolean[] betweenPredicatePositions = new boolean[numberOfPredicateDimensions];

		transformPredicates(predicates, structure, betweenSets, equalsList, betweenPredicatePositions);

		List<List<Object>> keyPrefixes = createKeyPrefixes(betweenSets, equalsList, betweenPredicatePositions,
				numberOfPredicateDimensions);

		return createPrimaryKeysFromKeyPrefixes(keyPrefixes);
	}

	private PrimaryKey rangeScanMaxPrimaryKeyPrefix(CubeQuery.CubePredicates predicates) {
		List<Object> keyPrefixList = new ArrayList<>();

		for (String field : dimensions) {
			CubeQuery.CubePredicate predicate = predicates.map().get(field);

			if (predicate instanceof CubeQuery.CubePredicateEq) {
				Object value = ((CubeQuery.CubePredicateEq) predicate).value;
				keyPrefixList.add(value);
			} else if (predicate instanceof CubeQuery.CubePredicateBetween) {
				Object to = ((CubeQuery.CubePredicateBetween) predicate).to;
				keyPrefixList.add(to);
			} else
				break;
		}

		return PrimaryKey.ofList(keyPrefixList);
	}

	private PrimaryKey rangeScanMinPrimaryKeyPrefix(CubeQuery.CubePredicates predicates) {
		List<Object> keyPrefixList = new ArrayList<>();

		for (String field : dimensions) {
			CubeQuery.CubePredicate predicate = predicates.map().get(field);
			if (predicate instanceof CubeQuery.CubePredicateEq) {
				Object value = ((CubeQuery.CubePredicateEq) predicate).value;
				keyPrefixList.add(value);
			} else if (predicate instanceof CubeQuery.CubePredicateBetween) {
				Object from = ((CubeQuery.CubePredicateBetween) predicate).from;
				keyPrefixList.add(from);
			} else
				break;
		}

		return PrimaryKey.ofList(keyPrefixList);
	}

	@Override
	public String toString() {
		return "Aggregation{id='" + id + '\'' + ", dimensions=" + dimensions + ", measures=" + measures + '}';
	}
}
