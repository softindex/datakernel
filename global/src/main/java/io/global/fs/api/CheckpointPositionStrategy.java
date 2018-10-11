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

public interface CheckpointPositionStrategy {

	long nextPosition(long position);

	static CheckpointPositionStrategy fixed(long offset) {
		return pos -> pos + offset;
	}

	static CheckpointPositionStrategy randRange(long min, long max) {
		return pos -> pos + ThreadLocalRandom.current().nextLong(min, max);
	}

	static CheckpointPositionStrategy alterating(long first, long second) {
		return new CheckpointPositionStrategy() {
			boolean state = false;

			@Override
			public long nextPosition(long pos) {
				return pos + ((state ^= true) ? first : second);
			}
		};
	}
}
