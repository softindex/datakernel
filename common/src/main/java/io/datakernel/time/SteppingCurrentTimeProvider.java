/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.time;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class SteppingCurrentTimeProvider implements CurrentTimeProvider {
	private static final Logger logger = Logger.getLogger(SteppingCurrentTimeProvider.class.getName());

	private long timeMillis;
	private long step;

	private SteppingCurrentTimeProvider(long timeMillis, long step) {
		this.timeMillis = timeMillis;
		this.step = step;
	}

	public static SteppingCurrentTimeProvider create(long timeMillis, long step) {
		return new SteppingCurrentTimeProvider(timeMillis, step);
	}

	@Override
	public long currentTimeMillis() {
		long currentTime = timeMillis;
		timeMillis += step;
		logger.log(Level.INFO, "Time: " + currentTime);
		return currentTime;
	}
}
