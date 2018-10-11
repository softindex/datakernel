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

package io.global.globalfs;

import io.global.globalfs.api.GlobalFsCheckpoint;
import org.junit.ComparisonFailure;
import org.junit.Test;

public class GlobalFsCheckpointTest {

	private void checkpointTest(int expectedStart, int expectedFinish, long[] checkpoints, long offset, long length) {
		int[] result = GlobalFsCheckpoint.getExtremes(checkpoints, offset, length);

		if (expectedStart != result[0] || expectedFinish != result[1]) {
			throw new ComparisonFailure("Wrong checkpoints found", expectedStart + " to " + expectedFinish, result[0] + " to " + result[1]);
		}
	}

	private void boundaryTest(long[] checkpoints) {
		int lastIndex = checkpoints.length - 1;
		long size = checkpoints[lastIndex];

		// first
		checkpointTest(0, 0, checkpoints, 0, 0);

		// whole
		checkpointTest(0, lastIndex, checkpoints, 0, size);
		checkpointTest(0, lastIndex, checkpoints, 0, -1);

		// last
		checkpointTest(lastIndex, lastIndex, checkpoints, size, 0);
		checkpointTest(lastIndex, lastIndex, checkpoints, size, -1);

		// over
		checkpointTest(0, lastIndex, checkpoints, 0, 2 * size);
		checkpointTest(lastIndex, lastIndex, checkpoints, size, size);
		checkpointTest(lastIndex, lastIndex, checkpoints, 2 * size, 3 * size);
	}

	@Test
	public void testEmpty() {
		boundaryTest(new long[]{0});
	}

	@Test
	public void testTwo() {
		long[] checkpoints = {0, 123};
		boundaryTest(checkpoints);

		checkpointTest(0, 1, checkpoints, 0, 52);
		checkpointTest(0, 1, checkpoints, 52, 2);
		checkpointTest(0, 1, checkpoints, 52, 123);
	}

	@Test
	public void testN() {
		long[] checkpoints = {0, 123, 456, 789, 1024, 1525};
		boundaryTest(checkpoints);

		checkpointTest(1, 2, checkpoints, 234, 200);
		checkpointTest(1, 3, checkpoints, 234, 789 - 234 - 1);
		checkpointTest(1, 3, checkpoints, 234, 789 - 234);
		checkpointTest(1, 4, checkpoints, 234, 789 - 234 + 1);
	}

	@Test
	public void testUnboundedEnd() {
		long[] checkpoints = {0, 10, 20, 30, 40, 50};
		boundaryTest(checkpoints);

		checkpointTest(1, 5, checkpoints, 15, -1);
	}
}
