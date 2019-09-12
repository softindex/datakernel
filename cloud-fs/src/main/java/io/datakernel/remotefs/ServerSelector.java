package io.datakernel.remotefs;

import java.util.List;
import java.util.Set;

import static io.datakernel.common.HashUtils.murmur3hash;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;

/**
 * Strategy interface, which decides what file goes on which server from given ones.
 */
@FunctionalInterface
public interface ServerSelector {
	/**
	 * Implementation of rendezvous hash sharding algorithm
	 */
	ServerSelector RENDEZVOUS_HASH_SHARDER = (fileName, shards, topShards) -> {
		class ShardWithHash {
			private final Object shard;
			private final int hash;

			private ShardWithHash(Object shard, int hash) {
				this.shard = shard;
				this.hash = hash;
			}
		}
		return shards.stream()
			.map(shard -> new ShardWithHash(shard, murmur3hash(fileName.hashCode(), shard.hashCode())))
			.sorted(comparingInt(h -> h.hash))
			.map(h -> h.shard)
			.limit(topShards)
			.collect(toList());
	};

	/**
	 * Selects partitions where given file should belong.
	 *
	 * @param fileName     name of the file
	 * @param shards set of partition ids to choose from
	 * @param topShards    number of ids to return
	 * @return list of keys of servers ordered by priority where file with given name should be
	 */
	List<Object> selectFrom(String fileName, Set<Object> shards, int topShards);
}
