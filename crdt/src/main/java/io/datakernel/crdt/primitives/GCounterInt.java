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

public final class GCounterInt {
	public static final BufferSerializer<GCounterInt> SERIALIZER = new Serializer();

	private final int[] state;

	private GCounterInt(int[] state) {
		this.state = state;
	}

	public GCounterInt(int nodes) {
		this(new int[nodes]);
	}

	public void increment(int id) {
		state[id]++;
	}

	public int value() {
		int v = 0;
		for (int c : state) {
			v += c;
		}
		return v;
	}

	public GCounterInt merge(GCounterInt other) {
		assert state.length == other.state.length;
		int[] newState = new int[state.length];
		for (int i = 0; i < newState.length; i++) {
			newState[i] = max(state[i], other.state[i]);
		}
		return new GCounterInt(newState);
	}

	@Override
	public String toString() {
		return Integer.toString(value());
	}

	private static class Serializer implements BufferSerializer<GCounterInt> {
		@Override
		public void serialize(ByteBuf output, GCounterInt item) {
			int[] state = item.state;
			output.writeVarInt(state.length);
			for (int c : state) {
				output.writeInt(c);
			}
		}

		@Override
		public GCounterInt deserialize(ByteBuf input) {
			int[] state = new int[input.readVarInt()];
			for (int i = 0; i < state.length; i++) {
				state[i] = input.readInt();
			}
			return new GCounterInt(state);
		}
	}
}
