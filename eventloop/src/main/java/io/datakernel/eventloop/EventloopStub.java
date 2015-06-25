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

import io.datakernel.bytebuf.ByteBufPool;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;

public final class EventloopStub implements Eventloop {

	/**
	 * The number of loop executions since instantiating eventloop
	 */
	private long tick;

	/**
	 * Current time for this Eventloop
	 */
	private long timestamp;
	/**
	 * Queue contains tasks which was added from Eventloop's thread
	 */
	private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
	/**
	 * Queue contains tasks which was added from other thread
	 */
	private final ConcurrentLinkedQueue<Runnable> queueEx = new ConcurrentLinkedQueue<>();
	/**
	 * Lock for working with concurrent tasks in this class
	 */
	private final ReentrantLock lock = new ReentrantLock();
	/**
	 * Countdown for working with concurrent tasks in this class
	 */
	private final Condition countdown = lock.newCondition();

	/**
	 * Count of operations from other threads which affect eventloop's work
	 */
	private volatile int concurrentOperationsCount;

	/**
	 * Queue contains tasks which becomes enabled after the given delay.
	 */
	private final PriorityQueue<ScheduledRunnable> scheduledTasks = new PriorityQueue<>(1);

	private final PriorityQueue<ScheduledRunnable> backgroundTasks = new PriorityQueue<>();

	private final HashMap<Class<?>, Object> localMap = new HashMap<>();

	@Override
	public ByteBufPool getByteBufferPool() {
		return ByteBufPool.defaultInstance();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Class<T> type) {
		return (T) localMap.get(type);
	}

	@Override
	public <T> void set(Class<T> type, T value) {
		localMap.put(type, value);
	}

	@Override
	public long tick() {
		return tick;
	}

	/**
	 * Adds the task to queue from Eventloop's thread
	 *
	 * @param runnable runnable for task
	 */
	@Override
	public void post(Runnable runnable) {
		queue.addFirst(runnable);
	}

	/**
	 * Adds the task to the end of queue from Eventloop's thread
	 *
	 * @param runnable runnable for task
	 */
	@Override
	public void postLater(Runnable runnable) {
		queue.addLast(runnable);
	}

	/**
	 * Adds the task from other thread
	 *
	 * @param runnable runnable for task
	 */
	@Override
	public void postConcurrently(Runnable runnable) {
		queueEx.offer(runnable);
	}

	/**
	 * Returns current time for this Eventloop
	 */
	@Override
	public long currentTimeMillis() {
		return timestamp;
	}

	/**
	 * Sets the time of Eventloop as system time
	 */
	public void refreshTimestamp() {
		this.timestamp = System.currentTimeMillis();
	}

	/**
	 * Sets the time of Eventloop as time from parameters
	 *
	 * @param timestamp new timstamp for setting
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Adds tasks which can be executed after certain time
	 *
	 * @param timestamp time after which this task will be executed
	 * @param runnable  runnable for task
	 */
	@Override
	public ScheduledRunnable schedule(long timestamp, Runnable runnable) {
		return addScheduledTask(timestamp, runnable, false);
	}

	@Override
	public ScheduledRunnable scheduleBackground(long timestamp, Runnable runnable) {
		return addScheduledTask(timestamp, runnable, true);
	}

	private ScheduledRunnable addScheduledTask(long timestamp, Runnable runnable, boolean background) {
		final ScheduledRunnable scheduledRunnable = new ScheduledRunnable(timestamp, runnable);
		final PriorityQueue<ScheduledRunnable> taskQueue = background ? backgroundTasks : scheduledTasks;
		if (this.timestamp != 0) {
			taskQueue.add(scheduledRunnable);
		} else {
			post(new Runnable() {
				@Override
				public void run() {
					taskQueue.add(scheduledRunnable);
				}
			});
		}
		return scheduledRunnable;
	}

	/**
	 * Principal method in Eventloop.
	 * Executes tasks until it has it.
	 */
	public void run() {
		while (!queue.isEmpty() || !queueEx.isEmpty() || !scheduledTasks.isEmpty() || concurrentOperationsCount != 0) {
			tick = (tick + (1L << 32)) & 0xFFFFFFFF00000000L;
			while (true) {
				Runnable runnable = queue.poll();
				if (runnable == null)
					break;
				runnable.run();
			}
			while (true) {
				Runnable runnable = queueEx.poll();
				if (runnable == null)
					break;
				runnable.run();
			}
			while (true) {
				ScheduledRunnable scheduledRunnable = scheduledTasks.peek();
				if (scheduledRunnable == null || (scheduledRunnable.getTimestamp() > this.timestamp && this.timestamp != 0))
					break;
				scheduledTasks.poll();
				if (scheduledRunnable.isCancelled())
					continue;
				scheduledRunnable.getRunnable().run();
				scheduledRunnable.complete();
			}
			lock.lock();
			try {
				if (concurrentOperationsCount != 0) {
					try {
						countdown.await(); // TODO - spurious wakeup?
					} catch (InterruptedException e) {
						return;
					}
				}
			} finally {
				lock.unlock();
			}
		}
	}

	/**
	 * Call this method if some operation from other thread affects eventloop.
	 * Eventloop can not stop before all concurrent operations are ended.
	 *
	 * @return ConcurrentOperationTracker with method Complete, witch can resume
	 * eventloop's work.
	 */
	@Override
	public ConcurrentOperationTracker startConcurrentOperation() {
		lock.lock();
		try {
			concurrentOperationsCount++;
		} finally {
			lock.unlock();
		}

		return new ConcurrentOperationTracker() {
			boolean complete;

			@Override
			public void complete() {
				lock.lock();
				try {
					checkState(!complete);
					complete = true;
					concurrentOperationsCount--;
					countdown.signal();
				} finally {
					lock.unlock();
				}
			}
		};
	}
}
