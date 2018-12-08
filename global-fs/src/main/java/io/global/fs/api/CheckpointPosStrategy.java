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

package io.global.fs.api;

import java.util.concurrent.ThreadLocalRandom;

@FunctionalInterface
public interface CheckpointPosStrategy {

	long nextPosition(long position);

	static CheckpointPosStrategy fixed(long offset) {
		return pos -> pos + offset;
	}

	static CheckpointPosStrategy randRange(long min, long max) {
		return pos -> pos + ThreadLocalRandom.current().nextLong(min, max);
	}

	static CheckpointPosStrategy alterating(long first, long second) {
		return new CheckpointPosStrategy() {
			boolean state = false;

			@Override
			public long nextPosition(long pos) {
				return pos + ((state ^= true) ? first : second);
			}
		};
	}
}
