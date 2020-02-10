/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutput;
import io.datakernel.serializer.BinarySerializer;

import static java.lang.Math.max;

public final class GCounterLong implements CrdtMergable<GCounterLong> {
	public static final BinarySerializer<GCounterLong> SERIALIZER = new Serializer();

	private final long[] state;

	private GCounterLong(long[] state) {
		this.state = state;
	}

	public GCounterLong(int n) {
		this(new long[n]);
	}

	public void increment(int id) {
		state[id]++;
	}

	public long value() {
		long v = 0;
		for (long c : state) {
			v += c;
		}
		return v;
	}

	@Override
	public GCounterLong merge(GCounterLong other) {
		assert state.length == other.state.length;
		long[] newState = new long[state.length];
		for (int i = 0; i < newState.length; i++) {
			newState[i] = max(state[i], other.state[i]);
		}
		return new GCounterLong(newState);
	}

	@Override
	public String toString() {
		return Long.toString(value());
	}

	private static class Serializer implements BinarySerializer<GCounterLong> {
		@Override
		public void encode(BinaryOutput out, GCounterLong item) {
			long[] state = item.state;
			out.writeVarInt(state.length);
			for (long c : state) {
				out.writeLong(c);
			}
		}

		@Override
		public GCounterLong decode(BinaryInput in) {
			long[] state = new long[in.readVarInt()];
			for (int i = 0; i < state.length; i++) {
				state[i] = in.readLong();
			}
			return new GCounterLong(state);
		}
	}
}
