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
import io.datakernel.aggregation_db.AggregationQuery.QueryPredicate;
import io.datakernel.aggregation_db.AggregationQuery.QueryPredicateBetween;
import io.datakernel.aggregation_db.AggregationQuery.QueryPredicateEq;
import io.datakernel.aggregation_db.AggregationQuery.QueryPredicates;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeEnumerable;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;

/**
 * Represents aggregation metadata. Stores chunks in an index (represented by an array of {@link RangeTree}) for efficient search.
 * Provides methods for managing index, querying for chunks by key, searching for chunks that are available for consolidation.
 */
public class AggregationMetadata {
	private final String id;
	private final ImmutableList<String> keys;
	private final ImmutableList<String> inputFields;
	private final ImmutableList<String> outputFields;
	private final QueryPredicates predicates;

	private final RangeTree<PrimaryKey, AggregationChunk>[] prefixRanges;

	private static final int EQUALS_QUERIES_THRESHOLD = 1_000;

	public AggregationMetadata(String id, Collection<String> keys, Collection<String> inputFields) {
		this(id, keys, inputFields, inputFields, null);
	}

	public AggregationMetadata(String id, Collection<String> keys, Collection<String> inputFields,
	                           Collection<String> outputFields) {
		this(id, keys, inputFields, outputFields, null);
	}

	public AggregationMetadata(String id, Collection<String> keys, Collection<String> inputFields,
	                           QueryPredicates predicates) {
		this(id, keys, inputFields, inputFields, predicates);
	}

	/**
	 * Constructs an aggregation metadata object with the given id, keys, input and output fields.
	 *
	 * @param id           id of aggregation
	 * @param keys         list of key names
	 * @param inputFields  list of input field names
	 * @param outputFields list of output field names
	 * @param predicates   list of predicates
	 */
	@SuppressWarnings("unchecked")
	public AggregationMetadata(String id, Collection<String> keys, Collection<String> inputFields,
	                           Collection<String> outputFields, QueryPredicates predicates) {
		this.id = id;
		this.predicates = predicates;
		this.keys = ImmutableList.copyOf(keys);
		this.inputFields = ImmutableList.copyOf(inputFields);
		this.outputFields = ImmutableList.copyOf(outputFields);
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

	public boolean matchQueryPredicates(QueryPredicates predicates) {
		if (this.predicates == null)
			return true;

		Map<String, QueryPredicate> aggregationPredicateMap = this.predicates.asMap();
		Map<String, QueryPredicate> predicateMap = predicates.asMap();

		for (Map.Entry<String, QueryPredicate> predicateEntry : predicateMap.entrySet()) {
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

		Map<String, QueryPredicate> queryPredicates = query.getPredicates().asMap();
		Map<String, QueryPredicate> aggregationPredicates = this.predicates.asMap();
		List<String> appliedPredicateKeys = newArrayList();

		for (Map.Entry<String, QueryPredicate> aggregationPredicatesEntry : aggregationPredicates.entrySet()) {
			String aggregationPredicateKey = aggregationPredicatesEntry.getKey();
			QueryPredicate queryPredicate = queryPredicates.get(aggregationPredicateKey);
			QueryPredicate aggregationPredicate = aggregationPredicatesEntry.getValue();
			if (queryPredicate == null)
				return new AggregationFilteringResult(false); // no corresponding query predicate for this aggregation predicate

			KeyType keyType = structure.getKeyType(aggregationPredicateKey);

			if (queryPredicate instanceof QueryPredicateEq) {
				Object queryPredicateEq = ((QueryPredicateEq) queryPredicate).value;
				if (aggregationPredicate instanceof QueryPredicateEq) {
					Object aggregationPredicateEq = ((QueryPredicateEq) aggregationPredicate).value;
					if (keyType.compare(queryPredicateEq, aggregationPredicateEq) != 0)
						return new AggregationFilteringResult(false);
					else
						appliedPredicateKeys.add(aggregationPredicateKey); // no longer need this predicate as it is already applied
				} else if (aggregationPredicate instanceof QueryPredicateBetween) {
					Object aggregationPredicateFrom = ((QueryPredicateBetween) aggregationPredicate).from;
					Object aggregationPredicateTo = ((QueryPredicateBetween) aggregationPredicate).to;
					// queryPredicateEq ∉ [aggregationPredicateFrom; aggregationPredicateTo]
					if (keyType.compare(queryPredicateEq, aggregationPredicateFrom) < 0
							|| keyType.compare(queryPredicateEq, aggregationPredicateTo) > 0)
						return new AggregationFilteringResult(false);
					// else aggregation may contain the requested value, but we still need to apply the predicate for this key
				} else
					return new AggregationFilteringResult(false); // unsupported predicate type
			} else if (queryPredicate instanceof QueryPredicateBetween) {
				Object queryPredicateFrom = ((QueryPredicateBetween) queryPredicate).from;
				Object queryPredicateTo = ((QueryPredicateBetween) queryPredicate).to;
				if (aggregationPredicate instanceof QueryPredicateEq) {
					/* If we are requesting the value of a key in range and this aggregation only contains records
					for the specific value of a key, this aggregations does not satisfy a predicate.
					Example: this aggregation contains records for 15 June 2015.
					Query is for data throughout the entire month (June).
					So we reject this aggregation hoping some other aggregation has all the requested data.
					 */
					return new AggregationFilteringResult(false);
				} else if (aggregationPredicate instanceof QueryPredicateBetween) {
					Object aggregationPredicateFrom = ((QueryPredicateBetween) aggregationPredicate).from;
					Object aggregationPredicateTo = ((QueryPredicateBetween) aggregationPredicate).to;
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
		checkArgument(outputFields.containsAll(chunk.getFields()));
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

	public String getId() {
		return id;
	}

	public List<String> getKeys() {
		return keys;
	}

	public List<String> getInputFields() {
		return inputFields;
	}

	public List<String> getOutputFields() {
		return outputFields;
	}

	public QueryPredicates getAggregationPredicates() {
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

		List<String> remainingFields = newArrayList(filter(query.getResultFields(), not(in(outputFields))));

		ArrayList<QueryPredicate> equalsPredicates = newArrayList(filter(query.getPredicates().asCollection(),
				new Predicate<QueryPredicate>() {
					@Override
					public boolean apply(QueryPredicate predicate) {
						return predicate instanceof QueryPredicateEq;
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

	public List<AggregationChunk> findChunksToConsolidate() {
		int maxOverlaps = 2;
		Set<AggregationChunk> result = new HashSet<>();
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[keys.size()];
		for (Map.Entry<PrimaryKey, RangeTree.Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			RangeTree.Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = segment.getSet().size() + segment.getClosingSet().size();
			if (overlaps >= maxOverlaps) {
				maxOverlaps = overlaps;
				result.clear();
				result.addAll(segment.getSet());
				result.addAll(segment.getClosingSet());
			}
		}
		return new ArrayList<>(result);
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

	/* BETWEEN predicate optimization is currently disabled */

//	public List<AggregationChunk> queryByPredicates(AggregationStructure structure, final Map<Long, AggregationChunk> chunks,
//	                                                QueryPredicates predicates) {
//		List<QueryPredicate> prefixPredicates = getPrefixPredicates(predicates);
//		List<QueryPredicate> betweenPredicates = newArrayList(filter(prefixPredicates,
//				isBetweenPredicate()));
//		boolean containsBetweenPredicates = betweenPredicates.size() > 0;
//
//		if (!containsBetweenPredicates) {
//			return queryByEqualsPredicates(chunks, predicates);
//		} else if (shouldConvertBetweenPredicatesToEqualsQueries(betweenPredicates, structure)) {
//			return queryByConvertingBetweenPredicatesToEqualsQueries(structure, prefixPredicates, chunks);
//		} else {
//			return queryByFilteringListOfChunks(predicates);
//		}
//	}

	public List<AggregationChunk> queryByPredicates(AggregationStructure structure, final Map<Long, AggregationChunk> chunks,
	                                                QueryPredicates predicates) {
		return queryByFilteringListOfChunks(predicates);
	}

	private List<AggregationChunk> queryByFilteringListOfChunks(QueryPredicates predicates) {
		PrimaryKey minQueryKey = rangeScanMinPrimaryKeyPrefix(predicates);
		PrimaryKey maxQueryKey = rangeScanMaxPrimaryKeyPrefix(predicates);

		RangeTree<PrimaryKey, AggregationChunk> zeroLengthPrefix = prefixRanges[0];
		Set<AggregationChunk> allAggregationChunks = zeroLengthPrefix.getAll();

		Iterable<AggregationChunk> filteredChunks = filter(allAggregationChunks,
				chunkMightContainQueryValuesPredicate(minQueryKey, maxQueryKey));

		return newArrayList(filteredChunks);
	}

	private PrimaryKey rangeScanMaxPrimaryKeyPrefix(QueryPredicates predicates) {
		List<Object> keyPrefixList = new ArrayList<>();

		for (String field : keys) {
			QueryPredicate predicate = predicates.asUnmodifiableMap().get(field);

			if (predicate instanceof QueryPredicateEq) {
				Object value = ((QueryPredicateEq) predicate).value;
				keyPrefixList.add(value);
			} else if (predicate instanceof QueryPredicateBetween) {
				Object to = ((QueryPredicateBetween) predicate).to;
				keyPrefixList.add(to);
			} else
				break;
		}

		return PrimaryKey.ofList(keyPrefixList);
	}

	private PrimaryKey rangeScanMinPrimaryKeyPrefix(QueryPredicates predicates) {
		List<Object> keyPrefixList = new ArrayList<>();

		for (String key : keys) {
			QueryPredicate predicate = predicates.asUnmodifiableMap().get(key);
			if (predicate instanceof QueryPredicateEq) {
				Object value = ((QueryPredicateEq) predicate).value;
				keyPrefixList.add(value);
			} else if (predicate instanceof QueryPredicateBetween) {
				Object from = ((QueryPredicateBetween) predicate).from;
				keyPrefixList.add(from);
			} else
				break;
		}

		return PrimaryKey.ofList(keyPrefixList);
	}

	/* BETWEEN predicate optimization */
	private List<AggregationChunk> queryByEqualsPredicates(Map<Long, AggregationChunk> chunks,
	                                                       QueryPredicates predicates) {
		final PrimaryKey minQueryKey = rangeScanMinPrimaryKeyPrefix(predicates);
		final PrimaryKey maxQueryKey = rangeScanMaxPrimaryKeyPrefix(predicates);
		return rangeQuery(chunks, minQueryKey, maxQueryKey);
	}

	private List<AggregationChunk> queryByConvertingBetweenPredicatesToEqualsQueries(AggregationStructure structure,
	                                                                                 List<QueryPredicate> predicates,
	                                                                                 Map<Long, AggregationChunk> chunks) {
		List<PrimaryKey> equalsKeys = primaryKeysForEqualsQueries(structure, predicates);
		Set<AggregationChunk> resultChunks = Sets.newHashSet();

		for (PrimaryKey queryKey : equalsKeys) {
			resultChunks.addAll(rangeQuery(chunks, queryKey, queryKey));
		}

		return new ArrayList<>(resultChunks);
	}

	private List<PrimaryKey> primaryKeysForEqualsQueries(AggregationStructure structure, List<QueryPredicate> predicates) {
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

	private void transformPredicates(List<QueryPredicate> predicates, AggregationStructure structure, List<Set<Object>> betweenSets,
	                                 List<Object> equalsList, boolean[] betweenPredicatePositions) {
		for (int j = 0; j < predicates.size(); ++j) {
			String field = keys.get(j);
			QueryPredicate predicate = predicates.get(j);
			KeyType keyType = structure.getKeyType(field);

			if (predicate instanceof QueryPredicateEq) {
				equalsList.add(((QueryPredicateEq) predicate).value);
			} else if (predicate instanceof QueryPredicateBetween) {
				Object from = ((QueryPredicateBetween) predicate).from;
				Object to = ((QueryPredicateBetween) predicate).to;

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

	private boolean shouldConvertBetweenPredicatesToEqualsQueries(List<QueryPredicate> betweenPredicates,
	                                                              AggregationStructure structure) {
		if (!areAllKeyTypesEnumerable(betweenPredicates, structure))
			return false;

		long numberOfEqualsQueries = countNumberOfEqualsQueries(betweenPredicates, structure);

		return numberOfEqualsQueries <= EQUALS_QUERIES_THRESHOLD;
	}

	private List<QueryPredicate> getPrefixPredicates(QueryPredicates predicates) {
		List<QueryPredicate> prefixPredicates = newArrayList();

		for (String key : keys) {
			QueryPredicate predicateForKey = predicates.asUnmodifiableMap().get(key);
			if (predicateForKey != null)
				prefixPredicates.add(predicateForKey);
			else
				break;
		}

		return prefixPredicates;
	}

	private long countNumberOfEqualsQueries(List<QueryPredicate> betweenPredicates, AggregationStructure structure) {
		long queries = 0;

		for (QueryPredicate predicate : betweenPredicates) {
			QueryPredicateBetween predicateBetween = (QueryPredicateBetween) predicate;
			KeyType keyType = structure.getKeyType(predicate.key);
			long difference = ((KeyTypeEnumerable) keyType).difference(predicateBetween.to, predicateBetween.from) + 1;

			if (queries == 0)
				queries += difference;
			else
				queries *= difference;
		}

		return queries;
	}

	private boolean areAllKeyTypesEnumerable(List<QueryPredicate> predicates, AggregationStructure structure) {
		for (QueryPredicate predicate : predicates) {
			if (!isEnumerable(predicate.key, structure)) {
				return false;
			}
		}

		return true;
	}

	private List<AggregationChunk> rangeQuery(Map<Long, AggregationChunk> chunks,
	                                          PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey) {
		checkArgument(minPrimaryKey.size() == maxPrimaryKey.size());
		int size = minPrimaryKey.size();
		RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];
		return new ArrayList<>(index.getRange(minPrimaryKey, maxPrimaryKey));
	}

	private Predicate<QueryPredicate> isBetweenPredicate() {
		return new Predicate<QueryPredicate>() {
			@Override
			public boolean apply(QueryPredicate predicate) {
				return predicate instanceof QueryPredicateBetween;
			}
		};
	}

	private boolean isEnumerable(String key, AggregationStructure structure) {
		return structure.getKeyType(key) instanceof KeyTypeEnumerable;
	}

	@Override
	public String toString() {
		return "Aggregation{id='" + id + '\'' + ", keys=" + keys + ", fields=" + inputFields + '}';
	}
}
