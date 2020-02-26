/*
 * Copyright (C) 2015-2020 SoftIndex LLC.
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

package io.datakernel.crdt;

import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

/**
 * A type that defines its CRDT capabilities within itself.
 * <p>
 * This is similar to how {@link Comparable} works with {@link Comparator},
 * and there is a {@link CrdtOperator#ofCrdtType()} which is similar to {@link Comparator#naturalOrder()}.
 *
 * @param <Self> this must be only set to the actual type that you implement,
 *               a well-known Java limitation and technique for it.
 * @see CrdtOperator
 */
public interface Crdt<Self extends Crdt<Self>> {

	/**
	 * @see CrdtOperator#merge
	 */
	Self merge(Self other);

	/**
	 * @see CrdtOperator#extract
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	default Self extract(long revision) {
		return (Self) this;
	}
}
