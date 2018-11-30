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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.BufferSerializer;

import static java.lang.Math.max;

public final class GCounterLong {
	public static final BufferSerializer<GCounterLong> SERIALIZER = new Serializer();

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

	private static class Serializer implements BufferSerializer<GCounterLong> {
		@Override
		public void serialize(ByteBuf output, GCounterLong item) {
			long[] state = item.state;
			output.writeVarInt(state.length);
			for (long c : state) {
				output.writeLong(c);
			}
		}

		@Override
		public GCounterLong deserialize(ByteBuf input) {
			long[] state = new long[input.readVarInt()];
			for (int i = 0; i < state.length; i++) {
				state[i] = input.readLong();
			}
			return new GCounterLong(state);
		}
	}
}
