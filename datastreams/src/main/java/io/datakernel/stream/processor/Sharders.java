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

package io.datakernel.stream.processor;

/**
 * Static utility methods pertaining to Sharders
 */
public final class Sharders {
	private Sharders() {
	}

	/**
	 * Instance of this class shares objects by hashcode to shards
	 */
	public static final class HashSharder<K> implements Sharder<K> {
		private final int partitions;

		/**
		 * Creates the sharder which contains specified number of parts
		 *
		 * @param partitions number of parts
		 */
		public HashSharder(int partitions) {
			assert partitions >= 0;
			this.partitions = partitions;
		}

		/**
		 * Returns number of part which must contains this object. It counts it with hash code
		 *
		 * @param object object for sharding
		 */
		@Override
		public int shard(K object) {
			int hash = object.hashCode();
			int hashAbs = hash < 0 ? (hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : -hash) : hash;
			return hashAbs % partitions;
		}
	}
}
