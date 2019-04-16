package io.datakernel.aio.file.service;

import io.datakernel.aio.nativeio.AioNative;
import io.datakernel.async.Callback;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFileService;
import io.datakernel.service.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static io.datakernel.aio.buffer.DirectBufferPool.*;
import static io.datakernel.aio.util.Util.*;
import static io.datakernel.util.Preconditions.checkArgument;

@SuppressWarnings("unused")
public final class AioAsyncFileService implements AsyncFileService, BlockingService {
	private static final int DEFAULT_CONTEXT_SIZE = 1 << 6;
	private static final int EVENT_BLOCK_SIZE = 1 << 5;
	private static final long TIMEOUT_FOREVER = -1;
	private static final long MIN_SELECTED = 1;

	private final Queue<Long> queueTasks = new LinkedList<>();
	private int queueLimit = Integer.MAX_VALUE;
	private int currentObservedTasks;

	private final Map<Long, Task> tasks = new ConcurrentHashMap<>();
	private final AtomicLong taskId = new AtomicLong();

	private int contextSize = DEFAULT_CONTEXT_SIZE;
	private ByteBuffer eventsResult;
	private long aioContext;

	private Exception CONTEXT_CLOSED = new AioException("Context is closed");
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	private Thread aioThread;

	public AioAsyncFileService() {}

	@Override
	public void start() throws Exception {
		aioContext = AioNative.io_setup(contextSize);
		eventsResult = allocate(contextSize * EVENT_BLOCK_SIZE);
		aioThread = new Thread(this::run);
		aioThread.start();
		logger.trace("Aio Service started");
	}

	@Override
	public void stop() throws Exception {
		AioNative.destroy(aioContext);
		aioThread.join();
		logger.trace("Aio Service stopped");
	}

	public AioAsyncFileService withContextSize(int size) {
		checkArgument(size > 0);
		contextSize = size;
		logger.trace("Context size was settled - {}", size);
		return this;
	}

	public AioAsyncFileService withQueueLimit(int limit) {
		queueLimit = checkArgument(limit, v -> v > 0);
		logger.trace("Limit for queue was settled - {}", limit);
		return this;
	}

	@Override
	public Promise<Integer> read(FileChannel channel, long position, byte[] array, int offset, int size) {
		long newTaskId = 0;
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		try {
			final Integer fd = FileDescriptorResolver.getFd(channel);
			SettablePromise<Integer> result = new SettablePromise<>();

			newTaskId = taskId.incrementAndGet();
			ByteBuffer resultBuffer = allocate(size);
			ByteBuffer nativeUnit = wrapToNativeReadUnit(resultBuffer, position, fd, newTaskId, size);

			tasks.put(newTaskId, new Task((processedBytes, e) -> {
				if (e == null) {
					resultBuffer.get(array, offset, size);
					recycle(resultBuffer);
					recycle(nativeUnit);

					eventloop.execute(() -> result.set(processedBytes));
				} else {
					result.setException(e);
				}
				eventloop.completeExternalTask();
			}, nativeUnit));
			pushOperation(newTaskId, nativeUnit);

			return result;
		} catch (QueueOverflow | IllegalAccessException e) {
			eventloop.completeExternalTask();
			logger.error("Read operation with id = {} was denied", taskId);
			tasks.remove(newTaskId);
			return Promise.ofException(e);
		}
	}

	@Override
	public Promise<Integer> write(FileChannel channel, long position, byte[] array, int offset, int size) {
		long newTaskId = 0;
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.startExternalTask();
		try {
			final Integer fd = FileDescriptorResolver.getFd(channel);
			SettablePromise<Integer> result = new SettablePromise<>();
			newTaskId = taskId.incrementAndGet();

			ByteBuffer newBuffer = allocate(size);
			newBuffer.put(array, offset, size);
			ByteBuffer nativeUnit = wrapToNativeWriteUnit(size, position, fd, newTaskId, newBuffer);

			tasks.put(newTaskId, new Task((processedBytes, e) -> {
				recycle(newBuffer);
				recycle(nativeUnit);

				if (e == null) {
					eventloop.execute(() -> result.set(processedBytes));
				} else {
					eventloop.execute(() -> result.setException(e));
				}
				eventloop.completeExternalTask();
			}, nativeUnit));
			pushOperation(newTaskId, nativeUnit);

			return result;
		} catch (QueueOverflow | IllegalAccessException e) {
			eventloop.completeExternalTask();
			logger.error("Write operation with id = {} was denied", taskId);
			tasks.remove(newTaskId);
			return Promise.ofException(e);
		}
	}

	private void pushOperation(long newTaskId, ByteBuffer nativeUnit) throws QueueOverflow {
		boolean submit = false;

		synchronized (this) {
			if (currentObservedTasks >= contextSize) {
				if (queueTasks.size() < queueLimit) {
					queueTasks.add(newTaskId);
				} else {
					throw new QueueOverflow();
				}
			} else {
				currentObservedTasks++;
				submit = true;
			}
		}

		if (submit) {
			try {
				AioNative.io_submit(aioContext, 1, getAddress(nativeUnit));
			} catch (IOException e) {
				Task task = tasks.get(newTaskId);
				task.callback.accept(null, CONTEXT_CLOSED);
			}
		}
	}

	private void run() {
		while (true) {
			int numberResults;
			try {
				long address = getAddress(eventsResult);
				numberResults = AioNative.io_getevents(aioContext, MIN_SELECTED, contextSize, address, TIMEOUT_FOREVER);
				logger.trace("Reuslt was received with number result - {}", numberResults);
			} catch (IOException ignore) {
				/*
				 * Called when context is destroyed
				 */
				tasks.forEach((id, task) -> task.callback.accept(null, CONTEXT_CLOSED));
				break;
			}

			for (int i = 0; i < numberResults; i++) {
				int numberBlock = i * EVENT_BLOCK_SIZE;
				long taskId = eventsResult.getLong(numberBlock);

				long processedBytes = getResultCode(numberBlock);
				Task readyTask = tasks.remove(taskId);

				Callback<Integer> callback = readyTask.callback;
				callback.accept((int) processedBytes, null);
			}

			synchronized (this) {
				currentObservedTasks -= numberResults;
			}

			tryRavageQueue();
		}
	}

	private long getResultCode(int index) {
		return eventsResult.getLong(index + 16);
	}

	private void tryRavageQueue() {
		while (true) {
			Long taskId;

			synchronized (this) {
				if (currentObservedTasks >= contextSize || queueTasks.isEmpty()) {
					break;
				}
				currentObservedTasks++;
				taskId = queueTasks.poll();
			}

			Task task = tasks.get(taskId);
			try {
				logger.trace("Operation from queue was polled with id - {}", taskId);
				AioNative.io_submit(aioContext, 1, getAddress(task.buffer));
			} catch (IOException e) {
				logger.error("Wrong operation from queue was polled with id - {}", taskId);
				task.callback.accept(null, CONTEXT_CLOSED);
			}
		}
	}

	static final class Task {
		final Callback<Integer> callback;
		final ByteBuffer buffer;

		public Task(Callback<Integer> callback, ByteBuffer buffer) {
			this.callback = callback;
			this.buffer = buffer;
		}
	}
}
