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
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.eventloop.ScheduledRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CubeAutoReloadingChunks implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(CubeAutoReloadingChunks.class);

	private final Cube cube;
	private final Eventloop eventloop;
	private final long refreshPeriodMillis;

	private final Runnable refreshChunksTask = createRefreshChunksTask();

	private ScheduledRunnable scheduledRefreshChunksTask;

	public CubeAutoReloadingChunks(Cube cube, Eventloop eventloop, long refreshPeriodMillis) {
		this.cube = cube;
		this.eventloop = eventloop;
		this.refreshPeriodMillis = refreshPeriodMillis;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private void scheduleChunksRefresh() {
		if (scheduledRefreshChunksTask != null && scheduledRefreshChunksTask.isCancelled())
			return;

		scheduledRefreshChunksTask = eventloop.scheduleBackground(eventloop.currentTimeMillis() + refreshPeriodMillis,
				refreshChunksTask);
	}

	private Runnable createRefreshChunksTask() {
		return new Runnable() {
			@Override
			public void run() {
				cube.loadChunks(new CompletionCallback() {
					@Override
					public void onComplete() {
						scheduleChunksRefresh();
					}

					@Override
					public void onException(Exception exception) {
						logger.error("Refreshing chunks failed.");
						scheduleChunksRefresh();
					}
				});
			}
		};
	}

	@Override
	public void start(final CompletionCallback callback) {
		cube.loadChunks(new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
				scheduleChunksRefresh();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
			}
		});
	}

	@Override
	public void stop(CompletionCallback callback) {
		if (scheduledRefreshChunksTask != null)
			scheduledRefreshChunksTask.cancel();

		callback.onComplete();
	}
}
