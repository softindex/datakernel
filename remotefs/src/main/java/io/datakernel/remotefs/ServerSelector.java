package io.datakernel.remotefs;

import io.datakernel.util.HashUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * Strategy interface, which desides what file goes on which server from given ones.
 */
@FunctionalInterface
public interface ServerSelector {

	ServerSelector FIRST_N = (fileName, partitionIds, topShards) -> partitionIds.stream().limit(topShards).collect(toList());

	/**
	 * Implementation of rendezvous hash sharding algorithm
	 */
	ServerSelector RENDEZVOUS_HASH_SHARDER = (fileName, partitionIds, topShards) -> {
		class HashedObject {
			public final Object obj;
			public final int hash;

			HashedObject(Object obj, int hash) {
				this.obj = obj;
				this.hash = hash;
			}
		}
		return partitionIds.stream()
			.map(k -> new HashedObject(k, HashUtils.murmur3hash(fileName.hashCode(), k.hashCode())))
			.sorted(Comparator.<HashedObject>comparingInt(h -> h.hash).reversed())
			.map(h -> h.obj)
			.limit(topShards)
			.collect(toList());
	};

	/**
	 * Selects partitions where given file should belong.
	 *
	 * @param fileName     name of the file
	 * @param partitionIds set of partition ids to choose from
	 * @param topShards    number of ids to return
	 * @return list of keys of servers ordered by priority where file with given name should be
	 */
	List<Object> selectFrom(String fileName, Set<Object> partitionIds, int topShards);
}
