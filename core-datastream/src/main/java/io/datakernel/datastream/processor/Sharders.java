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

package io.datakernel.datastream.processor;

import static io.datakernel.common.Preconditions.checkArgument;

public final class Sharders {

	/**
	 * A sharder that distributes objects based on their hashcode.
	 * <p>
	 * It is optimized for when the number of partitions is a power of two.
	 */
	public static <T> Sharder<T> byHash(int partitions) {
		checkArgument(partitions > 0, "Number of partitions cannot be zero or less");
		int bits = partitions - 1;
		if ((partitions & bits) == 0) {
			return object -> object.hashCode() & bits;
		}
		return object -> {
			int hash = object.hashCode();
			int hashAbs = hash < 0 ? (hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : -hash) : hash;
			return hashAbs % partitions;
		};
	}
}
