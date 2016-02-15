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
	private static final Logger logger = LoggerFactory.getLogger(CubeConsolidatorService.class);

	private final Eventloop eventloop;
	private final Cube cube;
	private final long nothingToConsolidateSleepTimeMillis;
	private final int maxChunksToConsolidate;
	private final String processId;

	private ScheduledRunnable consolidationTask;

	public CubeConsolidatorService(Eventloop eventloop, Cube cube, long nothingToConsolidateSleepTimeMillis,
	                               int maxChunksToConsolidate, String processId) {
		this.eventloop = eventloop;
		this.cube = cube;
		this.nothingToConsolidateSleepTimeMillis = nothingToConsolidateSleepTimeMillis;
		this.maxChunksToConsolidate = maxChunksToConsolidate;
		this.processId = processId;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private void loadChunksAndConsolidate() {
		cube.loadChunks(new CompletionCallback() {
			@Override
			public void onComplete() {
				cube.consolidate(maxChunksToConsolidate, processId, new ResultCallback<Boolean>() {
					@Override
					public void onResult(Boolean consolidated) {
						if (consolidated) {
							// if previous consolidation merged some chunks, proceed consolidating other chunks
							logger.info("Consolidation finished. Launching consolidation task again.");
							eventloop.post(new Runnable() {
								@Override
								public void run() {
									loadChunksAndConsolidate();
								}
							});
						} else {
							// previous consolidation did not consolidate any chunks -> wait before next attempt
							logger.info("Previous consolidation did not merge any chunks. Scheduling next attempt in {} millis.", CubeConsolidatorService.this.nothingToConsolidateSleepTimeMillis);
							scheduleNext(CubeConsolidatorService.this.nothingToConsolidateSleepTimeMillis);
						}
					}

					@Override
					public void onException(Exception e) {
						logger.error("Consolidation failed", e);
						scheduleNext(CubeConsolidatorService.this.nothingToConsolidateSleepTimeMillis);
					}
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Loading chunk metadata failed", e);
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
				loadChunksAndConsolidate();
			}
		});
	}

	@Override
	public void start(final CompletionCallback callback) {
		callback.onComplete();
		loadChunksAndConsolidate();
	}

	@Override
	public void stop(CompletionCallback callback) {
		if (consolidationTask != null) {
			consolidationTask.cancel();
		}

		callback.onComplete();
	}
}
