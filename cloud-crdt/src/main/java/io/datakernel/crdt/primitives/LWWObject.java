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

package io.datakernel.crdt.primitives;

import io.datakernel.crdt.Crdt;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutput;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.Nullable;

public class LWWObject<T> implements Crdt<LWWObject<T>> {
	private final long revision;
	private final T object;

	public LWWObject(long revision, T object) {
		this.revision = revision;
		this.object = object;
	}

	public static <S> LWWObject<S> now(S state) {
		return new LWWObject<>(Eventloop.getCurrentEventloop().currentTimeMillis(), state);
	}

	public T getObject() {
		return object;
	}

	public long getTimestamp() {
		return revision;
	}

	@Override
	public LWWObject<T> merge(LWWObject<T> other) {
		if (revision < other.revision) {
			return other;
		}
		return this;
	}

	@Override
	@Nullable
	public LWWObject<T> extract(long revision) {
		return revision < this.revision ? this : null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		LWWObject<?> that = (LWWObject<?>) o;

		return revision == that.revision && object.equals(that.object);
	}

	@Override
	public int hashCode() {
		return 31 * (int) (revision ^ (revision >>> 32)) + object.hashCode();
	}

	@Override
	public String toString() {
		return "LWWObject{revision=" + revision + ", object=" + object + '}';
	}

	public static class Serializer<T> implements BinarySerializer<LWWObject<T>> {
		private final BinarySerializer<T> valueSerializer;

		public Serializer(BinarySerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, LWWObject<T> item) {
			out.writeLong(item.revision);
			valueSerializer.encode(out, item.object);
		}

		@Override
		public LWWObject<T> decode(BinaryInput in) {
			return new LWWObject<>(in.readLong(), valueSerializer.decode(in));
		}
	}
}
