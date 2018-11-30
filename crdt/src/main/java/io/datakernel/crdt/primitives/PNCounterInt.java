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

public final class PNCounterInt {
	public static final BufferSerializer<PNCounterInt> SERIALIZER = new Serializer();

	private final GCounterInt p;
	private final GCounterInt n;

	private PNCounterInt(GCounterInt p, GCounterInt n) {
		this.p = p;
		this.n = n;
	}

	public PNCounterInt(int nodes) {
		p = new GCounterInt(nodes);
		n = new GCounterInt(nodes);
	}

	public void increment(int id) {
		p.increment(id);
	}

	public void decrement(int id) {
		n.increment(id);
	}

	public int value() {
		return p.value() - n.value();
	}

	public PNCounterInt merge(PNCounterInt other) {
		return new PNCounterInt(p.merge(other.p), n.merge(other.n));
	}

	@Override
	public String toString() {
		return Integer.toString(value());
	}

	private static class Serializer implements BufferSerializer<PNCounterInt> {
		@Override
		public void serialize(ByteBuf output, PNCounterInt item) {
			GCounterInt.SERIALIZER.serialize(output, item.p);
			GCounterInt.SERIALIZER.serialize(output, item.n);
		}

		@Override
		public PNCounterInt deserialize(ByteBuf input) {
			return new PNCounterInt(GCounterInt.SERIALIZER.deserialize(input), GCounterInt.SERIALIZER.deserialize(input));
		}
	}
}
