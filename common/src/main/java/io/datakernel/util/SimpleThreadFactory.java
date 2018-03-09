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

package io.datakernel.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.util.Preconditions.checkArgument;

public final class SimpleThreadFactory implements ThreadFactory {
	public static final String NAME_PATTERN = "{}";

	private String name = "Thread-{}";
	private int priority;
	private boolean daemon;

	private final AtomicInteger count = new AtomicInteger(0);

	private SimpleThreadFactory() {
	}

	private SimpleThreadFactory(String name) {
		this.name = name;
	}

	public static ThreadFactory create() {
		return new SimpleThreadFactory();
	}

	public static ThreadFactory create(String name) {
		return new SimpleThreadFactory(name);
	}

	public SimpleThreadFactory withPriority(int priority) {
		checkArgument(priority == 0 || (priority >= Thread.MIN_PRIORITY && priority <= Thread.MAX_PRIORITY));
		this.priority = priority;
		return this;
	}

	public SimpleThreadFactory withDaemon(boolean daemon) {
		this.daemon = daemon;
		return this;
	}

	public int getCount() {
		return count.get();
	}

	@Override
	public Thread newThread(Runnable runnable) {
		Thread thread = name == null ?
				new Thread(runnable) :
				new Thread(runnable, name.replace(NAME_PATTERN, "" + count.incrementAndGet()));
		thread.setDaemon(daemon);
		if (priority != 0) {
			thread.setPriority(priority);
		}
		return thread;
	}

}
