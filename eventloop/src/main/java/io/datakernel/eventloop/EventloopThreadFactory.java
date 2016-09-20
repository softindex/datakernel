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

package io.datakernel.eventloop;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public final class EventloopThreadFactory implements ThreadFactory {
	private static final ThreadFactory DEFAULT_THREAD_FACTORY = createEventloopThreadFactory(Thread.NORM_PRIORITY, true);

	private final int priority;
	private final boolean daemon;
	private final AtomicLong count = new AtomicLong(1);

	private EventloopThreadFactory(int priority, boolean daemon) {
		this.priority = priority;
		this.daemon = daemon;
	}

	public static ThreadFactory defaultEventloopThreadFactory() {
		return DEFAULT_THREAD_FACTORY;
	}

	public static ThreadFactory createEventloopThreadFactory(int priority, boolean daemon) {
		return new EventloopThreadFactory(priority, daemon);
	}

	@Override
	public Thread newThread(Runnable runnable) {
		Thread thread = new Thread(runnable, "EventloopThread-" + count.getAndIncrement());
		thread.setDaemon(daemon);
		thread.setPriority(priority);
		return thread;
	}

}
