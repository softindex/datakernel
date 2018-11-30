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

public final class PNCounterLong {
	public static final BufferSerializer<PNCounterLong> SERIALIZER = new Serializer();

	private final GCounterLong p;
	private final GCounterLong n;

	private PNCounterLong(GCounterLong p, GCounterLong n) {
		this.p = p;
		this.n = n;
	}

	public PNCounterLong(int nodes) {
		p = new GCounterLong(nodes);
		n = new GCounterLong(nodes);
	}

	public void increment(int id) {
		p.increment(id);
	}

	public void decrement(int id) {
		n.increment(id);
	}

	public long value() {
		return p.value() - n.value();
	}

	public PNCounterLong merge(PNCounterLong other) {
		return new PNCounterLong(p.merge(other.p), n.merge(other.n));
	}

	@Override
	public String toString() {
		return Long.toString(value());
	}

	private static class Serializer implements BufferSerializer<PNCounterLong> {
		@Override
		public void serialize(ByteBuf output, PNCounterLong item) {
			GCounterLong.SERIALIZER.serialize(output, item.p);
			GCounterLong.SERIALIZER.serialize(output, item.n);
		}

		@Override
		public PNCounterLong deserialize(ByteBuf input) {
			return new PNCounterLong(GCounterLong.SERIALIZER.deserialize(input), GCounterLong.SERIALIZER.deserialize(input));
		}
	}
}
