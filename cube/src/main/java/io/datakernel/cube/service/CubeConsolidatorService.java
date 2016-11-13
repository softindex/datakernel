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

package io.datakernel.cube.service;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CubeConsolidatorService implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final Cube cube;
	private final long nothingToConsolidateSleepTimeMillis;

	private ScheduledRunnable consolidationTask;

	private CubeConsolidatorService(Eventloop eventloop, Cube cube, long nothingToConsolidateSleepTimeMillis) {
		this.eventloop = eventloop;
		this.cube = cube;
		this.nothingToConsolidateSleepTimeMillis = nothingToConsolidateSleepTimeMillis;
	}

	public static CubeConsolidatorService create(Eventloop eventloop, Cube cube, long nothingToConsolidateSleepTimeMillis,
	                                             int maxChunksToConsolidate) {
		return new CubeConsolidatorService(eventloop, cube, nothingToConsolidateSleepTimeMillis);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private void consolidate() {
		cube.consolidate(new ResultCallback<Boolean>() {
			@Override
			protected void onResult(Boolean consolidated) {
				if (consolidated) {
					// if previous consolidation merged some chunks, proceed consolidating other chunks
					logger.info("Consolidation finished. Launching consolidation task again.");
					eventloop.post(new Runnable() {
						@Override
						public void run() {
							consolidate();
						}
					});
				} else {
					// previous consolidation did not consolidate any chunks -> wait before next attempt
					logger.info("Previous consolidation did not merge any chunks. Scheduling next attempt in {} millis.", CubeConsolidatorService.this.nothingToConsolidateSleepTimeMillis);
					scheduleNext(CubeConsolidatorService.this.nothingToConsolidateSleepTimeMillis);
				}
			}

			@Override
			protected void onException(Exception e) {
				logger.error("Consolidation failed", e);
				scheduleNext(CubeConsolidatorService.this.nothingToConsolidateSleepTimeMillis);
			}
		});
	}

	private void scheduleNext(long timeout) {
		if (consolidationTask != null && consolidationTask.isCancelled())
			return;

		consolidationTask = eventloop.scheduleBackground(eventloop.currentTimeMillis() + timeout, new Runnable() {
			@Override
			public void run() {
				consolidate();
			}
		});
	}

	@Override
	public void start(final CompletionCallback callback) {
		callback.setComplete();
		consolidate();
	}

	@Override
	public void stop(CompletionCallback callback) {
		if (consolidationTask != null) {
			consolidationTask.cancel();
		}

		callback.setComplete();
	}
}
