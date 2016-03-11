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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeEnumerable;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Represents aggregation metadata. Stores chunks in an index (represented by an array of {@link RangeTree}) for efficient search.
 * Provides methods for managing index, querying for chunks by key, searching for chunks that are available for consolidation.
 */
public class AggregationMetadata {
	private final ImmutableList<String> keys;
	private final ImmutableList<String> fields;
	private final AggregationQuery.Predicates predicates;

	private final RangeTree<PrimaryKey, AggregationChunk>[] prefixRanges;

	private static final int EQUALS_QUERIES_THRESHOLD = 1_000;
	private static final Random RANDOM = new Random();

	public AggregationMetadata(Collection<String> keys, Collection<String> fields) {
		this(keys, fields, null);
	}

	/**
	 * Constructs an aggregation metadata object with the given id, keys, fields.
	 *
	 * @param keys       list of key names
	 * @param fields     list of field names
	 * @param predicates list of predicates
	 */
	@SuppressWarnings("unchecked")
	public AggregationMetadata(Collection<String> keys,
	                           Collection<String> fields,
	                           AggregationQuery.Predicates predicates) {
		this.predicates = predicates;
		this.keys = ImmutableList.copyOf(keys);
		this.fields = ImmutableList.copyOf(fields);
		this.prefixRanges = new RangeTree[keys.size() + 1];
		for (int size = 0; size <= keys.size(); size++) {
			this.prefixRanges[size] = new RangeTree<>();
		}
	}

	public int getNumberOfPredicates() {
		return this.predicates == null ? 0 : this.predicates.asMap().size();
	}

	public boolean hasPredicates() {
		return this.predicates != null;
	}

	public boolean matchQueryPredicates(AggregationQuery.Predicates predicates) {
		if (this.predicates == null)
			return true;

		Map<String, AggregationQuery.Predicate> aggregationPredicateMap = this.predicates.asMap();
		Map<String, AggregationQuery.Predicate> predicateMap = predicates.asMap();

		for (Map.Entry<String, AggregationQuery.Predicate> predicateEntry : predicateMap.entrySet()) {
			String predicateKey = predicateEntry.getKey();
			if (aggregationPredicateMap.get(predicateKey) != null && !aggregationPredicateMap.get(predicateKey).equals(predicateEntry.getValue()))
				return false;
		}

		return true;
	}

	/**
	 * Checks whether this aggregation satisfies predicates of the given query.
	 * Returns true if query has predicates for every key, which has corresponding aggregation predicate
	 * and query predicates satisfy aggregation predicates.
	 * If this aggregation does not have any predicates defined this method returns false.
	 * Also removes applied predicates which are no longer needed from the given query,
	 *
	 * @param query query
	 * @return true if this aggregation satisfies given query predicates
	 */
	public AggregationFilteringResult applyQueryPredicates(AggregationQuery query, AggregationStructure structure) {
		if (this.predicates == null)
			return new AggregationFilteringResult(false);

		Map<String, AggregationQuery.Predicate> queryPredicates = query.getPredicates().asMap();
		Map<String, AggregationQuery.Predicate> aggregationPredicates = this.predicates.asMap();
		List<String> appliedPredicateKeys = newArrayList();

		for (Map.Entry<String, AggregationQuery.Predicate> aggregationPredicatesEntry : aggregationPredicates.entrySet()) {
			String aggregationPredicateKey = aggregationPredicatesEntry.getKey();
			AggregationQuery.Predicate queryPredicate = queryPredicates.get(aggregationPredicateKey);
			AggregationQuery.Predicate aggregationPredicate = aggregationPredicatesEntry.getValue();
			if (queryPredicate == null)
				return new AggregationFilteringResult(false); // no corresponding query predicate for this aggregation predicate

			KeyType keyType = structure.getKeyType(aggregationPredicateKey);

			if (queryPredicate instanceof AggregationQuery.PredicateEq) {
				Object queryPredicateEq = ((AggregationQuery.PredicateEq) queryPredicate).value;
				if (aggregationPredicate instanceof AggregationQuery.PredicateEq) {
					Object aggregationPredicateEq = ((AggregationQuery.PredicateEq) aggregationPredicate).value;
					if (keyType.compare(queryPredicateEq, aggregationPredicateEq) != 0)
						return new AggregationFilteringResult(false);
					else
						appliedPredicateKeys.add(aggregationPredicateKey); // no longer need this predicate as it is already applied
				} else if (aggregationPredicate instanceof AggregationQuery.PredicateBetween) {
					Object aggregationPredicateFrom = ((AggregationQuery.PredicateBetween) aggregationPredicate).from;
					Object aggregationPredicateTo = ((AggregationQuery.PredicateBetween) aggregationPredicate).to;
					// queryPredicateEq ∉ [aggregationPredicateFrom; aggregationPredicateTo]
					if (keyType.compare(queryPredicateEq, aggregationPredicateFrom) < 0
							|| keyType.compare(queryPredicateEq, aggregationPredicateTo) > 0)
						return new AggregationFilteringResult(false);
					// else aggregation may contain the requested value, but we still need to apply the predicate for this key
				} else
					return new AggregationFilteringResult(false); // unsupported predicate type
			} else if (queryPredicate instanceof AggregationQuery.PredicateBetween) {
				Object queryPredicateFrom = ((AggregationQuery.PredicateBetween) queryPredicate).from;
				Object queryPredicateTo = ((AggregationQuery.PredicateBetween) queryPredicate).to;
				if (aggregationPredicate instanceof AggregationQuery.PredicateEq) {
					/* If we are requesting the value of a key in range and this aggregation only contains records
					for the specific value of a key, this aggregations does not satisfy a predicate.
					Example: this aggregation contains records for 15 June 2015.
					Query is for data throughout the entire month (June).
					So we reject this aggregation hoping some other aggregation has all the requested data.
					 */
					return new AggregationFilteringResult(false);
				} else if (aggregationPredicate instanceof AggregationQuery.PredicateBetween) {
					Object aggregationPredicateFrom = ((AggregationQuery.PredicateBetween) aggregationPredicate).from;
					Object aggregationPredicateTo = ((AggregationQuery.PredicateBetween) aggregationPredicate).to;
					// [queryPredicateFrom; queryPredicateTo] ⊄ [aggregationPredicateFrom; aggregationPredicateTo]
					if (keyType.compare(queryPredicateFrom, aggregationPredicateFrom) < 0 ||
							keyType.compare(queryPredicateTo, aggregationPredicateTo) > 0) {
						// only accept aggregation if it fully contains the requested range of values of the specific key
						return new AggregationFilteringResult(false);
					}
				} else
					return new AggregationFilteringResult(false);
			}
		}

		return new AggregationFilteringResult(true, appliedPredicateKeys);
	}

	public void addToIndex(AggregationChunk chunk) {
		for (int size = 0; size <= keys.size(); size++) {
			RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];

			PrimaryKey lower = chunk.getMinPrimaryKey().prefix(size);
			PrimaryKey upper = chunk.getMaxPrimaryKey().prefix(size);
			index.put(lower, upper, chunk);
		}
	}

	public void removeFromIndex(AggregationChunk chunk) {
		for (int size = 0; size <= keys.size(); size++) {
			RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];

			PrimaryKey lower = chunk.getMinPrimaryKey().prefix(size);
			PrimaryKey upper = chunk.getMaxPrimaryKey().prefix(size);
			index.remove(lower, upper, chunk);
		}
	}

	public List<String> getKeys() {
		return keys;
	}

	public List<String> getFields() {
		return fields;
	}

	public AggregationQuery.Predicates getAggregationPredicates() {
		return this.predicates;
	}

	public boolean allKeysIn(List<String> requestedKeys) {
		return all(keys, in(requestedKeys));
	}

	public boolean containsKeys(List<String> requestedKeys) {
		return all(requestedKeys, in(keys));
	}

	public boolean containsKeysInKeysAndPredicatesKeys(List<String> requestedKeys) {
		if (this.predicates == null)
			return containsKeys(requestedKeys);

		return all(requestedKeys, in(newArrayList(concat(keys, predicates.keys()))));
	}

	public double getCost(AggregationQuery query) {
		int unfilteredKeyCost = 100;

		List<String> remainingFields = newArrayList(filter(query.getResultFields(), not(in(fields))));

		ArrayList<AggregationQuery.Predicate> equalsPredicates = newArrayList(filter(query.getPredicates().asCollection(),
				new Predicate<AggregationQuery.Predicate>() {
					@Override
					public boolean apply(AggregationQuery.Predicate predicate) {
						return predicate instanceof AggregationQuery.PredicateEq;
					}
				}));

		if (!this.containsKeysInKeysAndPredicatesKeys(query.getAllKeys()) || equalsPredicates.size() > keys.size()) {
			return Double.MAX_VALUE;
		}

		if (equalsPredicates.isEmpty()) {
			return Math.pow(Math.pow(unfilteredKeyCost, keys.size()), 1 + remainingFields.size());
		}

		int filteredKeys = 0;
		for (int i = 0; i < equalsPredicates.size(); ++i) {
			String predicateKey = equalsPredicates.get(i).key;
			String aggregationKey = keys.get(i);
			if (aggregationKey.equals(predicateKey)) {
				++filteredKeys;
			} else {
				break;
			}
		}

		return Math.pow(Math.pow(unfilteredKeyCost, keys.size() - filteredKeys), 1 + remainingFields.size());
	}

	public List<List<AggregationChunk>> findChunkGroupsForConsolidation(int maxChunks, int maxGroups) {
		Set<Set<AggregationChunk>> chunkGroups = getChunkGroupsWithOverlaps();

		if (chunkGroups.isEmpty())
			return newArrayList();

		Set<Set<AggregationChunk>> expandedChunkGroups = newHashSet();
		for (Set<AggregationChunk> chunkGroup : chunkGroups) {
			Set<AggregationChunk> expandedChunkGroup = newHashSet(chunkGroup);
			expandRange(expandedChunkGroup, Integer.MAX_VALUE);
			expandedChunkGroups.add(expandedChunkGroup);
		}

		List<List<AggregationChunk>> trimmedChunkGroups = newArrayList();
		for (Set<AggregationChunk> chunkGroup : expandedChunkGroups) {
			if (chunkGroup.size() > maxChunks) {
				trimmedChunkGroups.add(trimChunks(newArrayList(chunkGroup), maxChunks));
				continue;
			}

			trimmedChunkGroups.add(newArrayList(chunkGroup));
		}

		if (trimmedChunkGroups.size() <= maxGroups)
			return trimmedChunkGroups;

		Collections.sort(trimmedChunkGroups, new Comparator<List<AggregationChunk>>() {
			@Override
			public int compare(List<AggregationChunk> s1, List<AggregationChunk> s2) {
				return Integer.compare(s2.size(), s1.size());
			}
		});
		return trimmedChunkGroups.subList(0, maxGroups);
	}

	private Set<Set<AggregationChunk>> getChunkGroupsWithOverlaps() {
		int minOverlaps = 2;
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[keys.size()];

		Set<Set<AggregationChunk>> chunkGroups = newHashSet();
		for (RangeTree.Segment<AggregationChunk> segment : tree.getSegments().values()) {
			if (getNumberOfOverlaps(segment) < minOverlaps)
				continue;

			Set<AggregationChunk> segmentSet = newHashSet();
			segmentSet.addAll(segment.getSet());
			segmentSet.addAll(segment.getClosingSet());
			chunkGroups.add(segmentSet);
		}

		return chunkGroups;
	}

	private int getNumberOfOverlaps(RangeTree.Segment segment) {
		return segment.getSet().size() + segment.getClosingSet().size();
	}

	public List<AggregationChunk> findChunksGroupWithMostOverlaps() {
		int maxOverlaps = 2;
		Set<AggregationChunk> result = new HashSet<>();
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[keys.size()];
		for (Map.Entry<PrimaryKey, RangeTree.Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			RangeTree.Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = getNumberOfOverlaps(segment);
			if (overlaps >= maxOverlaps) {
				maxOverlaps = overlaps;
				result.clear();
				result.addAll(segment.getSet());
				result.addAll(segment.getClosingSet());
			}
		}
		return new ArrayList<>(result);
	}

	public List<AggregationChunk> findChunksGroupWithMinKey() {
		int minOverlaps = 2;
		Set<AggregationChunk> result = new HashSet<>();
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[keys.size()];
		for (Map.Entry<PrimaryKey, RangeTree.Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			RangeTree.Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = getNumberOfOverlaps(segment);
			if (overlaps >= minOverlaps) {
				result.addAll(segment.getSet());
				result.addAll(segment.getClosingSet());
				break;
			}
		}
		return new ArrayList<>(result);
	}

	public boolean useHotSegmentStrategy(double preferHotSegmentsCoef) {
		return RANDOM.nextDouble() <= preferHotSegmentsCoef;
	}

	public List<AggregationChunk> findChunksForConsolidation(int maxChunks, double preferHotSegmentsCoef) {
		List<AggregationChunk> chunks = useHotSegmentStrategy(preferHotSegmentsCoef) ?
				findChunksGroupWithMostOverlaps() : findChunksGroupWithMinKey();

		if (chunks.isEmpty() || chunks.size() == maxChunks)
			return chunks;

		if (chunks.size() > maxChunks)
			return trimChunks(chunks, maxChunks);

		List<AggregationChunk> expandedChunks = expandRange(chunks, maxChunks);

		if (expandedChunks.size() > maxChunks)
			return trimChunks(expandedChunks, maxChunks);

		return expandedChunks;
	}

	private List<AggregationChunk> trimChunks(List<AggregationChunk> chunks, int maxChunks) {
		Collections.sort(chunks, new Comparator<AggregationChunk>() {
			@Override
			public int compare(AggregationChunk chunk1, AggregationChunk chunk2) {
				return chunk1.getMinPrimaryKey().compareTo(chunk2.getMinPrimaryKey());
			}
		});
		return chunks.subList(0, maxChunks);
	}

	private boolean expandRange(Set<AggregationChunk> chunks) {
		PrimaryKey minKey = null;
		PrimaryKey maxKey = null;

		for (AggregationChunk chunk : chunks) {
			PrimaryKey chunkMinKey = chunk.getMinPrimaryKey();
			PrimaryKey chunkMaxKey = chunk.getMaxPrimaryKey();

			if (minKey == null) {
				minKey = chunkMinKey;
				maxKey = chunkMaxKey;
				continue;
			}

			if (chunkMinKey.compareTo(minKey) < 0)
				minKey = chunkMinKey;

			if (chunkMaxKey.compareTo(maxKey) > 0)
				maxKey = chunkMaxKey;
		}

		Set<AggregationChunk> chunksForRange = getChunksForRange(minKey, maxKey);
		return chunks.addAll(chunksForRange);
	}

	private void expandRange(Set<AggregationChunk> chunks, int maxChunks) {
		boolean expand = chunks.size() < maxChunks;

		while (expand) {
			boolean expanded = expandRange(chunks);
			expand = expanded && chunks.size() < maxChunks;
		}
	}

	private List<AggregationChunk> expandRange(List<AggregationChunk> chunks, int maxChunks) {
		Set<AggregationChunk> chunkSet = new HashSet<>(chunks);
		expandRange(chunkSet, maxChunks);
		return new ArrayList<>(chunkSet);
	}

	public List<ConsolidationDebugInfo> getConsolidationDebugInfo() {
		List<ConsolidationDebugInfo> infos = newArrayList();
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[keys.size()];

		for (Map.Entry<PrimaryKey, RangeTree.Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			PrimaryKey key = segmentEntry.getKey();
			RangeTree.Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = segment.getSet().size() + segment.getClosingSet().size();
			Set<AggregationChunk> segmentSet = segment.getSet();
			Set<AggregationChunk> segmentClosingSet = segment.getClosingSet();
			infos.add(new ConsolidationDebugInfo(key, segmentSet, segmentClosingSet, overlaps));
		}

		return infos;
	}

	public static class ConsolidationDebugInfo {
		public final PrimaryKey key;
		public final Set<AggregationChunk> segmentSet;
		public final Set<AggregationChunk> segmentClosingSet;
		public final int overlaps;

		public ConsolidationDebugInfo(PrimaryKey key, Set<AggregationChunk> segmentSet,
		                         Set<AggregationChunk> segmentClosingSet, int overlaps) {
			this.key = key;
			this.segmentSet = segmentSet;
			this.segmentClosingSet = segmentClosingSet;
			this.overlaps = overlaps;
		}
	}

	@VisibleForTesting
	public static boolean chunkMightContainQueryValues(PrimaryKey minQueryKey, PrimaryKey maxQueryKey,
	                                                   PrimaryKey minChunkKey, PrimaryKey maxChunkKey) {
		return chunkMightContainQueryValues(minQueryKey.values(), maxQueryKey.values(),
				minChunkKey.values(), maxChunkKey.values());
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

	// with various optimizations (like BETWEEN optimization)
	@SuppressWarnings("unchecked")
	public List<AggregationChunk> findChunks(AggregationStructure structure, AggregationQuery.Predicates predicates,
	                                         List<String> fields) {
		final Set<String> requestedFields = newHashSet(fields);
		List<AggregationQuery.Predicate> prefixPredicates = getPrefixPredicates(predicates);
		List<AggregationQuery.Predicate> betweenPredicates = newArrayList(filter(prefixPredicates,
				isBetweenPredicate()));
		boolean containsBetweenPredicates = betweenPredicates.size() > 0;

		List<AggregationChunk> chunks;

		if (!containsBetweenPredicates) {
			chunks = queryByEqualsPredicates(predicates);
		} else if (shouldConvertBetweenPredicatesToEqualsQueries(betweenPredicates, structure)) {
			chunks = queryByConvertingBetweenPredicatesToEqualsQueries(structure, prefixPredicates);
		} else {
			chunks = queryByFilteringListOfChunks(predicates);
		}

		return newArrayList(filter(chunks, new Predicate<AggregationChunk>() {
			@Override
			public boolean apply(AggregationChunk chunk) {
				return !intersection(newHashSet(chunk.getFields()), requestedFields).isEmpty();
			}
		}));
	}

	// without optimizations
	public List<AggregationChunk> findChunks(List<String> fields, AggregationQuery.Predicates predicates) {
		final Set<String> requestedFields = newHashSet(fields);
		List<AggregationChunk> chunks = queryByFilteringListOfChunks(predicates);
		return newArrayList(filter(chunks, new Predicate<AggregationChunk>() {
			@Override
			public boolean apply(AggregationChunk chunk) {
				return !intersection(newHashSet(chunk.getFields()), requestedFields).isEmpty();
			}
		}));
	}

	private List<AggregationChunk> queryByFilteringListOfChunks(AggregationQuery.Predicates predicates) {
		PrimaryKey minQueryKey = rangeScanMinPrimaryKeyPrefix(predicates);
		PrimaryKey maxQueryKey = rangeScanMaxPrimaryKeyPrefix(predicates);

		RangeTree<PrimaryKey, AggregationChunk> zeroLengthPrefix = prefixRanges[0];
		Set<AggregationChunk> allAggregationChunks = zeroLengthPrefix.getAll();

		Iterable<AggregationChunk> filteredChunks = filter(allAggregationChunks,
				chunkMightContainQueryValuesPredicate(minQueryKey, maxQueryKey));

		return newArrayList(filteredChunks);
	}

	private PrimaryKey rangeScanMaxPrimaryKeyPrefix(AggregationQuery.Predicates predicates) {
		List<Object> keyPrefixList = new ArrayList<>();

		for (String field : keys) {
			AggregationQuery.Predicate predicate = predicates.asUnmodifiableMap().get(field);

			if (predicate instanceof AggregationQuery.PredicateEq) {
				Object value = ((AggregationQuery.PredicateEq) predicate).value;
				keyPrefixList.add(value);
			} else if (predicate instanceof AggregationQuery.PredicateBetween) {
				Object to = ((AggregationQuery.PredicateBetween) predicate).to;
				keyPrefixList.add(to);
			} else
				break;
		}

		return PrimaryKey.ofList(keyPrefixList);
	}

	private PrimaryKey rangeScanMinPrimaryKeyPrefix(AggregationQuery.Predicates predicates) {
		List<Object> keyPrefixList = new ArrayList<>();

		for (String key : keys) {
			AggregationQuery.Predicate predicate = predicates.asUnmodifiableMap().get(key);
			if (predicate instanceof AggregationQuery.PredicateEq) {
				Object value = ((AggregationQuery.PredicateEq) predicate).value;
				keyPrefixList.add(value);
			} else if (predicate instanceof AggregationQuery.PredicateBetween) {
				Object from = ((AggregationQuery.PredicateBetween) predicate).from;
				keyPrefixList.add(from);
			} else
				break;
		}

		return PrimaryKey.ofList(keyPrefixList);
	}

	/* BETWEEN predicate optimization */
	private List<AggregationChunk> queryByEqualsPredicates(AggregationQuery.Predicates predicates) {
		final PrimaryKey minQueryKey = rangeScanMinPrimaryKeyPrefix(predicates);
		final PrimaryKey maxQueryKey = rangeScanMaxPrimaryKeyPrefix(predicates);
		return rangeQuery(minQueryKey, maxQueryKey);
	}

	private List<AggregationChunk> queryByConvertingBetweenPredicatesToEqualsQueries(AggregationStructure structure,
	                                                                                 List<AggregationQuery.Predicate> predicates) {
		List<PrimaryKey> equalsKeys = primaryKeysForEqualsQueries(structure, predicates);
		Set<AggregationChunk> resultChunks = newHashSet();

		for (PrimaryKey queryKey : equalsKeys) {
			resultChunks.addAll(rangeQuery(queryKey, queryKey));
		}

		return new ArrayList<>(resultChunks);
	}

	private List<PrimaryKey> primaryKeysForEqualsQueries(AggregationStructure structure, List<AggregationQuery.Predicate> predicates) {
		int numberOfPredicateKeys = predicates.size();

		List<Set<Object>> betweenSets = new ArrayList<>(numberOfPredicateKeys);
		List<Object> equalsList = new ArrayList<>(numberOfPredicateKeys);

		boolean[] betweenPredicatePositions = new boolean[numberOfPredicateKeys];

		transformPredicates(predicates, structure, betweenSets, equalsList, betweenPredicatePositions);

		List<List<Object>> keyPrefixes = createKeyPrefixes(betweenSets, equalsList, betweenPredicatePositions,
				numberOfPredicateKeys);

		return createPrimaryKeysFromKeyPrefixes(keyPrefixes);
	}

	public List<PrimaryKey> createPrimaryKeysFromKeyPrefixes(List<List<Object>> keyPrefixList) {
		List<PrimaryKey> keys = new ArrayList<>();

		for (List<Object> keyPrefix : keyPrefixList) {
			keys.add(PrimaryKey.ofList(keyPrefix));
		}

		return keys;
	}

	private List<List<Object>> createKeyPrefixes(List<Set<Object>> betweenSets, List<Object> equalsList,
	                                             boolean[] betweenPredicatePositions, int numberOfPredicateKeys) {
		List<List<Object>> keyPrefixes = new ArrayList<>();

		Set<List<Object>> cartesianProduct = Sets.cartesianProduct(betweenSets);

		for (List<Object> list : cartesianProduct) {
			List<Object> keyPrefix = newArrayList();
			int betweenListPosition = 0;
			int equalsListPosition = 0;

			for (int k = 0; k < numberOfPredicateKeys; ++k) {
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

	private void transformPredicates(List<AggregationQuery.Predicate> predicates, AggregationStructure structure, List<Set<Object>> betweenSets,
	                                 List<Object> equalsList, boolean[] betweenPredicatePositions) {
		for (int j = 0; j < predicates.size(); ++j) {
			String field = keys.get(j);
			AggregationQuery.Predicate predicate = predicates.get(j);
			KeyType keyType = structure.getKeyType(field);

			if (predicate instanceof AggregationQuery.PredicateEq) {
				equalsList.add(((AggregationQuery.PredicateEq) predicate).value);
			} else if (predicate instanceof AggregationQuery.PredicateBetween) {
				Object from = ((AggregationQuery.PredicateBetween) predicate).from;
				Object to = ((AggregationQuery.PredicateBetween) predicate).to;

				Set<Object> set = new LinkedHashSet<>();

				for (Object i = from; keyType.compare(i, to) <= 0;
				     i = ((KeyTypeEnumerable) keyType).increment(i)) {
					set.add(i);
				}

				betweenSets.add(set);
				betweenPredicatePositions[j] = true;
			}
		}
	}

	private boolean shouldConvertBetweenPredicatesToEqualsQueries(List<AggregationQuery.Predicate> betweenPredicates,
	                                                              AggregationStructure structure) {
		if (!areAllKeyTypesEnumerable(betweenPredicates, structure))
			return false;

		long numberOfEqualsQueries = countNumberOfEqualsQueries(betweenPredicates, structure);

		return numberOfEqualsQueries <= EQUALS_QUERIES_THRESHOLD;
	}

	private List<AggregationQuery.Predicate> getPrefixPredicates(AggregationQuery.Predicates predicates) {
		List<AggregationQuery.Predicate> prefixPredicates = newArrayList();

		for (String key : keys) {
			AggregationQuery.Predicate predicateForKey = predicates.asUnmodifiableMap().get(key);
			if (predicateForKey != null)
				prefixPredicates.add(predicateForKey);
			else
				break;
		}

		return prefixPredicates;
	}

	private long countNumberOfEqualsQueries(List<AggregationQuery.Predicate> betweenPredicates, AggregationStructure structure) {
		long queries = 0;

		for (AggregationQuery.Predicate predicate : betweenPredicates) {
			AggregationQuery.PredicateBetween predicateBetween = (AggregationQuery.PredicateBetween) predicate;
			KeyType keyType = structure.getKeyType(predicate.key);
			long difference = ((KeyTypeEnumerable) keyType).difference(predicateBetween.to, predicateBetween.from) + 1;

			if (queries == 0)
				queries += difference;
			else
				queries *= difference;
		}

		return queries;
	}

	private boolean areAllKeyTypesEnumerable(List<AggregationQuery.Predicate> predicates, AggregationStructure structure) {
		for (AggregationQuery.Predicate predicate : predicates) {
			if (!isEnumerable(predicate.key, structure)) {
				return false;
			}
		}

		return true;
	}

	private List<AggregationChunk> rangeQuery(PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey) {
		checkArgument(minPrimaryKey.size() == maxPrimaryKey.size());
		int size = minPrimaryKey.size();
		RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];
		return new ArrayList<>(index.getRange(minPrimaryKey, maxPrimaryKey));
	}

	private Set<AggregationChunk> getChunksForRange(PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey) {
		checkArgument(minPrimaryKey.size() == maxPrimaryKey.size());
		RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[minPrimaryKey.size()];
		return index.getRange(minPrimaryKey, maxPrimaryKey);
	}

	private Predicate isBetweenPredicate() {
		return new Predicate<AggregationQuery.Predicate>() {
			@Override
			public boolean apply(AggregationQuery.Predicate predicate) {
				return predicate instanceof AggregationQuery.PredicateBetween;
			}
		};
	}

	private boolean isEnumerable(String key, AggregationStructure structure) {
		return structure.getKeyType(key) instanceof KeyTypeEnumerable;
	}

	@Override
	public String toString() {
		return "Aggregation{keys=" + keys + ", fields=" + fields + '}';
	}
}
