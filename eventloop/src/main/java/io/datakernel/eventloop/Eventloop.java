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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.*;
import io.datakernel.exception.SimpleException;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.time.CurrentTimeProviderSystem;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * It is internal class for asynchronous programming. Eventloop represents infinite loop with only one
 * blocking operation selector.select() which selects a set of keys whose corresponding channels are
 * ready for I/O operations. With this keys and queues with tasks, which was added to Eventloop
 * from the outside, it begins asynchronous executing from one thread it in method run() which is overridden
 * because it is implementation of {@link Runnable}. Working of this eventloop will be ended, when it has
 * not selected keys and its queues with tasks are empty.
 */
public final class Eventloop implements Runnable, CurrentTimeProvider, Scheduler, EventloopExecutor, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final TimeoutException CONNECT_TIMEOUT = new TimeoutException("Connection timed out");
	private static final long DEFAULT_EVENT_TIMEOUT = 20L;

	private static volatile FatalErrorHandler globalFatalErrorHandler = FatalErrorHandlers.ignoreAllErrors();

	/**
	 * Collection of local tasks which was added from this thread.
	 */
	private final ArrayDeque<Runnable> localTasks = new ArrayDeque<>();

	/**
	 * Collection of concurrent tasks which was added from other threads.
	 */
	private final ConcurrentLinkedQueue<Runnable> concurrentTasks = new ConcurrentLinkedQueue<>();

	/**
	 * Collection of scheduled tasks that are scheduled at particular timestamp.
	 */
	private final PriorityQueue<ScheduledRunnable> scheduledTasks = new PriorityQueue<>();

	/**
	 * Collection of background tasks,
	 * which mean that if eventloop contains only background tasks, it will be closed
	 */
	private final PriorityQueue<ScheduledRunnable> backgroundTasks = new PriorityQueue<>();

	/**
	 * Count of concurrent operations in other threads, non-zero value prevents event loop from termination.
	 */
	private final AtomicInteger concurrentOperationsCount = new AtomicInteger(0);

	private final Map<Class<?>, Object> localMap = new HashMap<>();

	private final CurrentTimeProvider timeProvider;

	private long timeBeforeSelectorSelect;
	private long timeAfterSelectorSelect;

	/**
	 * The NIO selector which selects a set of keys whose corresponding channels
	 */
	private Selector selector;

	/**
	 * The thread where eventloop is running
	 */
	private Thread eventloopThread;

	private static final ThreadLocal<Eventloop> CURRENT_EVENTLOOP = new ThreadLocal<>();
	/**
	 * The desired name of the thread
	 */
	private final String threadName;
	private final int threadPriority;

	private final FatalErrorHandler fatalErrorHandler;

	private volatile boolean keepAlive;
	private volatile boolean breakEventloop;

	private long tick;

	/**
	 * Current time, cached to avoid System.currentTimeMillis() system calls, and to facilitate unit testing.
	 * It is being refreshed with each event loop execution.
	 */
	private long timestamp;

	private ThrottlingController throttlingController;

	/**
	 * Count of selected keys for last Selector.select()
	 */
	private int lastSelectedKeys;

	// JMX

	private static final double DEFAULT_SMOOTHING_WINDOW = 10.0;
	private double smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
	private final EventloopStats stats = EventloopStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ConcurrentCallsStats concurrentCallsStats = ConcurrentCallsStats.create(DEFAULT_SMOOTHING_WINDOW);

	private boolean monitoring = false;

	// region builders
	private Eventloop(CurrentTimeProvider timeProvider, String threadName, int threadPriority, ThrottlingController throttlingController, FatalErrorHandler fatalErrorHandler) {
		this.timeProvider = timeProvider;
		this.threadName = threadName;
		this.threadPriority = threadPriority;
		this.fatalErrorHandler = fatalErrorHandler;
		this.throttlingController = throttlingController;
		if (throttlingController != null) {
			throttlingController.setEventloop(this);
		}
		refreshTimestampAndGet();
		CURRENT_EVENTLOOP.set(this);
	}

	public static Eventloop create() {
		return new Eventloop(CurrentTimeProviderSystem.instance(), null, 0, null, null);
	}

	public Eventloop withCurrentTimeProvider(CurrentTimeProvider timeProvider) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler);
	}

	public Eventloop withThreadName(String threadName) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler);
	}

	public Eventloop withThreadPriority(int threadPriority) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler);
	}

	public Eventloop withThrottlingController(ThrottlingController throttlingController) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler);
	}

	public Eventloop withFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler);
	}
	// endregion

	public ThrottlingController getThrottlingController() {
		return throttlingController;
	}

	private void openSelector() {
		if (selector == null) {
			try {
				selector = SelectorProvider.provider().openSelector();
			} catch (Exception exception) {
				logger.error("Could not open selector", exception);
				throw new RuntimeException(exception);
			}
		}
	}

	/**
	 * Closes the selector if it has been opened.
	 */
	private void closeSelector() {
		if (selector != null) {
			try {
				selector.close();
				selector = null;
			} catch (IOException exception) {
				logger.error("Could not close selector", exception);
			}
		}
	}

	Selector ensureSelector() {
		if (selector == null) {
			openSelector();
		}
		return selector;
	}

	public boolean inEventloopThread() {
		return eventloopThread == null || eventloopThread == Thread.currentThread();
	}

	/**
	 * Sets the flag keep alive, if it is true it means that working of this Eventloop will be
	 * continued even in case when all tasks have been executed and it doesn't have selected keys.
	 *
	 * @param keepAlive flag for setting
	 */
	public void keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public void breakEventloop() {
		this.breakEventloop = true;
	}

	private boolean isKeepAlive() {
		if (breakEventloop)
			return false;
		return !localTasks.isEmpty() || !scheduledTasks.isEmpty() || !concurrentTasks.isEmpty()
				|| concurrentOperationsCount.get() > 0
				|| keepAlive || !selector.keys().isEmpty();
	}

	public static Eventloop getCurrentEventloop() {
		return CURRENT_EVENTLOOP.get();
	}

	/**
	 * Overridden method from Runnable that executes tasks while this eventloop is alive.
	 */
	@Override
	public void run() {
		eventloopThread = Thread.currentThread();
		if (threadName != null)
			eventloopThread.setName(threadName);
		if (threadPriority != 0)
			eventloopThread.setPriority(threadPriority);
		CURRENT_EVENTLOOP.set(this);
		ensureSelector();
		breakEventloop = false;

		timeBeforeSelectorSelect = timeAfterSelectorSelect = 0;
		while (true) {
			if (!isKeepAlive()) {
				logger.info("Eventloop {} is complete, exiting...", this);
				break;
			}

			tick = (tick + (1L << 32)) & ~0xFFFFFFFFL;

			updateBusinessLogicTimeStats();

			try {
				selector.select(getSelectTimeout());
			} catch (ClosedChannelException e) {
				logger.error("Selector is closed, exiting...", e);
				break;
			} catch (IOException e) {
				recordIoError(e, selector);
			}

			updateSelectorSelectTimeStats();

			processSelectedKeys(selector.selectedKeys());
			executeConcurrentTasks();
			executeScheduledTasks();
			executeBackgroundTasks();
			executeLocalTasks();
		}
		logger.info("Eventloop {} finished", this);
		eventloopThread = null;
		if (selector.keys().isEmpty()) {
			closeSelector();
		} else {
			logger.warn("Selector is still open, because event loop {} has {} keys", this, selector.keys());
		}
	}

	private void updateBusinessLogicTimeStats() {
		timeBeforeSelectorSelect = refreshTimestampAndGet();
		if (timeAfterSelectorSelect != 0) {
			long businessLogicTime = timeBeforeSelectorSelect - timeAfterSelectorSelect;
			if (throttlingController != null) {
				throttlingController.updateInternalStats(lastSelectedKeys, (int) businessLogicTime);
			}
			stats.updateBusinessLogicTime(businessLogicTime);
		}
	}

	private void updateSelectorSelectTimeStats() {
		timeAfterSelectorSelect = refreshTimestampAndGet();
		long selectorSelectTime = timeAfterSelectorSelect - timeBeforeSelectorSelect;
		stats.updateSelectorSelectTime(selectorSelectTime);
	}

	private long getSelectTimeout() {
		if (!concurrentTasks.isEmpty())
			return 1L;
		if (scheduledTasks.isEmpty() && backgroundTasks.isEmpty())
			return DEFAULT_EVENT_TIMEOUT;
		long timeout = Math.min(getTimeBeforeExecution(scheduledTasks), getTimeBeforeExecution(backgroundTasks));
		if (timeout < 1L)
			return 1L;
		return Math.min(DEFAULT_EVENT_TIMEOUT, timeout);
	}

	private long getTimeBeforeExecution(PriorityQueue<ScheduledRunnable> taskQueue) {
		while (!taskQueue.isEmpty()) {
			ScheduledRunnable first = taskQueue.peek();
			assert first != null; // unreachable condition
			if (first.isCancelled()) {
				taskQueue.poll();
				continue;
			}
			return first.getTimestamp() - currentTimeMillis();
		}
		return DEFAULT_EVENT_TIMEOUT;
	}

	/**
	 * Processes selected keys related to various I/O events: accept, connect, read, write.
	 *
	 * @param selectedKeys set that contains all selected keys, returned from NIO Selector.select()
	 */
	private void processSelectedKeys(Set<SelectionKey> selectedKeys) {
		int invalidKeys = 0, acceptKeys = 0, connectKeys = 0, readKeys = 0, writeKeys = 0;

		lastSelectedKeys = selectedKeys.size();
		if (throttlingController != null) {
			throttlingController.calculateThrottling(lastSelectedKeys);
		}

		Stopwatch sw = monitoring ? Stopwatch.createStarted() : null;

		Iterator<SelectionKey> iterator = selectedKeys.iterator();
		while (iterator.hasNext()) {
			SelectionKey key = iterator.next();
			iterator.remove();

			if (!key.isValid()) {
				invalidKeys++;
				continue;
			}

			try {
				if (key.isAcceptable()) {
					onAccept(key);
					acceptKeys++;
				} else if (key.isConnectable()) {
					onConnect(key);
					connectKeys++;
				} else {
					if (key.isReadable()) {
						onRead(key);
						readKeys++;
					}
					if (key.isValid()) {
						if (key.isWritable()) {
							onWrite(key);
							writeKeys++;
						}
					} else {
						invalidKeys++;
					}
				}
			} catch (Throwable e) {
				recordFatalError(e, key.attachment());
				closeQuietly(key.channel());
			}
		}
		stats.updateSelectedKeysTimeStats(sw);
		stats.updateSelectedKeysStats(lastSelectedKeys, invalidKeys, acceptKeys, connectKeys, readKeys, writeKeys);
	}

	/**
	 * Executes local tasks which were added from current thread
	 */
	private void executeLocalTasks() {
		int newRunnables = 0;

		Stopwatch swTotal = monitoring ? Stopwatch.createStarted() : null;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		while (true) {
			Runnable runnable = localTasks.poll();
			if (runnable == null) {
				break;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				runnable.run();
				tick++;
				if (sw != null)
					stats.updateLocalTaskDuration(runnable, sw);
			} catch (Throwable e) {
				recordFatalError(e, runnable);
			}
			newRunnables++;
		}
		stats.updateLocalTasksStats(newRunnables, swTotal);
	}

	/**
	 * Executes concurrent tasks which were added from other threads.
	 */
	private void executeConcurrentTasks() {
		int newRunnables = 0;

		Stopwatch swTotal = monitoring ? Stopwatch.createStarted() : null;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		while (true) {
			Runnable runnable = concurrentTasks.poll();
			if (runnable == null) {
				break;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				runnable.run();
				if (sw != null)
					stats.updateConcurrentTaskDuration(runnable, sw);
			} catch (Throwable e) {
				recordFatalError(e, runnable);
			}
			newRunnables++;
		}
		stats.updateConcurrentTasksStats(newRunnables, swTotal);
	}

	/**
	 * Executes tasks, scheduled for execution at particular timestamps
	 */
	private void executeScheduledTasks() {
		executeScheduledTasks(scheduledTasks);
	}

	private void executeBackgroundTasks() {
		executeScheduledTasks(backgroundTasks);
	}

	private void executeScheduledTasks(PriorityQueue<ScheduledRunnable> taskQueue) {
		int newRunnables = 0;
		Stopwatch swTotal = monitoring ? Stopwatch.createStarted() : null;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		for (; ; ) {
			ScheduledRunnable peeked = taskQueue.peek();
			if (peeked == null)
				break;
			if (peeked.isCancelled()) {
				taskQueue.poll();
				continue;
			}
			if (peeked.getTimestamp() >= currentTimeMillis()) {
				break;
			}
			ScheduledRunnable polled = taskQueue.poll();
			assert polled == peeked;

			Runnable runnable = polled.getRunnable();
			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				runnable.run();
				tick++;
				polled.complete();
				if (sw != null)
					stats.updateScheduledTaskDuration(runnable, sw);
			} catch (Throwable e) {
				recordFatalError(e, runnable);
			}

			newRunnables++;
		}
		stats.updateScheduledTasksStats(newRunnables, swTotal);
	}

	/**
	 * Accepts an incoming socketChannel connections without blocking event loop thread.
	 *
	 * @param key key of this action.
	 */
	private void onAccept(SelectionKey key) {
		assert inEventloopThread();

		ServerSocketChannel channel = (ServerSocketChannel) key.channel();
		if (!channel.isOpen()) { // TODO - remove?
			key.cancel();
			return;
		}

		AcceptCallback acceptCallback = (AcceptCallback) key.attachment();
		for (; ; ) {
			SocketChannel socketChannel;
			try {
				socketChannel = channel.accept();
				if (socketChannel == null)
					break;
				socketChannel.configureBlocking(false);
			} catch (ClosedChannelException e) {
				break;
			} catch (IOException e) {
				recordIoError(e, channel);
				break;
			}

			try {
				acceptCallback.onAccept(socketChannel);
			} catch (Throwable e) {
				recordFatalError(e, acceptCallback);
				closeQuietly(socketChannel);
			}
		}
	}

	/**
	 * Processes newly established TCP connections without blocking event loop thread.
	 *
	 * @param key key of this action.
	 */
	private void onConnect(SelectionKey key) {
		assert inEventloopThread();
		ConnectCallback connectCallback = (ConnectCallback) key.attachment();
		SocketChannel channel = (SocketChannel) key.channel();
		boolean connected;
		try {
			connected = channel.finishConnect();
		} catch (IOException e) {
			recordIoError(e, channel);
			closeQuietly(channel);
			connectCallback.setException(e);
			return;
		}

		if (connected) {
			connectCallback.setConnect(channel);
		} else {
			connectCallback.setException(new SimpleException("Not connected"));
		}
	}

	/**
	 * Processes socketChannels available for read, without blocking event loop thread.
	 *
	 * @param key key of this action.
	 */
	private void onRead(SelectionKey key) {
		assert inEventloopThread();
		NioChannelEventHandler handler = (NioChannelEventHandler) key.attachment();
		handler.onReadReady();
	}

	/**
	 * Processes socketChannels available for write, without blocking thread.
	 *
	 * @param key key of this action.
	 */
	private void onWrite(SelectionKey key) {
		assert inEventloopThread();
		NioChannelEventHandler handler = (NioChannelEventHandler) key.attachment();
		handler.onWriteReady();
	}

	private static void closeQuietly(AutoCloseable closeable) {
		if (closeable == null)
			return;
		try {
			closeable.close();
		} catch (Exception ignored) {
		}
	}

	/**
	 * Creates {@link ServerSocketChannel} that listens on InetSocketAddress.
	 *
	 * @param address              InetSocketAddress that server will listen
	 * @param serverSocketSettings settings from this server channel
	 * @param acceptCallback       callback that will be called when new incoming connection is being accepted. It can be called multiple times.
	 * @return server channel
	 * @throws IOException If some  I/O error occurs
	 */
	public ServerSocketChannel listen(InetSocketAddress address, ServerSocketSettings serverSocketSettings, AcceptCallback acceptCallback) throws IOException {
		assert inEventloopThread();
		ServerSocketChannel serverChannel = null;
		try {
			serverChannel = ServerSocketChannel.open();
			serverSocketSettings.applySettings(serverChannel);
			serverChannel.configureBlocking(false);
			serverChannel.bind(address, serverSocketSettings.getBacklog());
			serverChannel.register(ensureSelector(), SelectionKey.OP_ACCEPT, acceptCallback);
			return serverChannel;
		} catch (IOException e) {
			recordIoError(e, address);
			closeQuietly(serverChannel);
			throw e;
		}
	}

	/**
	 * Registers new UDP connection in this eventloop.
	 *
	 * @param bindAddress address for binding DatagramSocket for this connection.
	 * @return DatagramSocket of this connection
	 * @throws IOException if an I/O error occurs on opening DatagramChannel
	 */
	public static DatagramChannel createDatagramChannel(DatagramSocketSettings datagramSocketSettings,
	                                                    @Nullable InetSocketAddress bindAddress,
	                                                    @Nullable InetSocketAddress connectAddress) throws IOException {
		DatagramChannel datagramChannel = null;
		try {
			datagramChannel = DatagramChannel.open();
			datagramSocketSettings.applySettings(datagramChannel);
			datagramChannel.configureBlocking(false);
			datagramChannel.bind(bindAddress);
			if (connectAddress != null) {
				datagramChannel.connect(connectAddress);
			}
			return datagramChannel;
		} catch (IOException e) {
			if (datagramChannel != null) {
				try {
					datagramChannel.close();
				} catch (Exception ignored) {
				}
			}
			throw e;
		}
	}

	/**
	 * Connects to given socket address asynchronously.
	 *
	 * @param address         socketChannel's address
	 * @param connectCallback callback for connecting, it will be called once when connection is established or failed.
	 */
	public void connect(SocketAddress address, ConnectCallback connectCallback) {
		connect(address, 0, connectCallback);
	}

	/**
	 * Connects to given socket address asynchronously with a specified timeout value.
	 * A timeout of zero is interpreted as an default system timeout
	 *
	 * @param address         socketChannel's address
	 * @param timeout         the timeout value to be used in milliseconds, 0 as default system connection timeout
	 * @param connectCallback callback for connecting, it will be called once when connection is established or failed.
	 */
	public void connect(SocketAddress address, int timeout, ConnectCallback connectCallback) {
		assert inEventloopThread();
		SocketChannel socketChannel = null;
		try {
			socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);
			socketChannel.connect(address);
			socketChannel.register(ensureSelector(), SelectionKey.OP_CONNECT,
					timeoutConnectCallback(socketChannel, timeout, connectCallback));

		} catch (IOException e) {
			recordIoError(e, address);
			closeQuietly(socketChannel);
			try {
				connectCallback.setException(e);
			} catch (Throwable e1) {
				recordFatalError(e1, connectCallback);
			}
		}
	}

	/**
	 * Returns modified connectCallback to enable timeout.
	 * If connectionTime is zero, method returns input connectCallback.
	 * Otherwise schedules special task that will close SocketChannel and call onException method in case of timeout.
	 * If there is no timeout before connection - onConnect method will be called
	 */
	private ConnectCallback timeoutConnectCallback(final SocketChannel socketChannel, final long connectionTime, final ConnectCallback connectCallback) {
		if (connectionTime == 0)
			return connectCallback;

		return new ConnectCallback() {
			private final ScheduledRunnable scheduledTimeout = schedule(currentTimeMillis() + connectionTime, new Runnable() {
				@Override
				public void run() {
					recordIoError(CONNECT_TIMEOUT, socketChannel);
					closeQuietly(socketChannel);
					setException(CONNECT_TIMEOUT);
				}
			});

			@Override
			public void onConnect(SocketChannel socketChannel) {
				assert !scheduledTimeout.isComplete();
				scheduledTimeout.cancel();
				connectCallback.setConnect(socketChannel);
			}

			@Override
			public void onException(Exception exception) {
				assert !scheduledTimeout.isComplete();
				scheduledTimeout.cancel();
				connectCallback.setException(exception);
			}
		};
	}

	@SuppressWarnings("unchecked")
	public <T> T get(Class<T> type) {
		assert inEventloopThread();
		return (T) localMap.get(type);
	}

	public <T> void set(Class<T> type, T value) {
		assert inEventloopThread();
		localMap.put(type, value);
	}

	public long tick() {
		assert inEventloopThread();
		return tick;
	}

	/**
	 * Posts a new task to the beginning of localTasks.
	 * This method is recommended, since task will be executed as soon as possible without invalidating CPU cache.
	 *
	 * @param runnable runnable of this task
	 */
	public void post(Runnable runnable) {
		assert inEventloopThread();
		localTasks.addFirst(runnable);
	}

	/**
	 * Posts a new task to the end localTasks.
	 *
	 * @param runnable runnable of this task
	 */
	public void postLater(Runnable runnable) {
		assert inEventloopThread();
		localTasks.addLast(runnable);
	}

	/**
	 * Posts a new task from other threads.
	 * This is the preferred method of communicating with eventloop from another threads.
	 *
	 * @param runnable runnable of this task
	 */
	@Override
	public void execute(Runnable runnable) {
		concurrentTasks.offer(runnable);
		if (selector != null) {
			selector.wakeup();
		}
	}

	/**
	 * Schedules new task. Returns {@link ScheduledRunnable} with this runnable.
	 *
	 * @param timestamp timestamp after which task will be ran
	 * @param runnable  runnable of this task
	 * @return scheduledRunnable, which could used for cancelling the task
	 */
	@Override
	public ScheduledRunnable schedule(long timestamp, Runnable runnable) {
		assert inEventloopThread();
		return addScheduledTask(timestamp, runnable, false);
	}

	/**
	 * Schedules new background task. Returns {@link ScheduledRunnable} with this runnable.
	 * <p/>
	 * If eventloop contains only background tasks, it will be closed
	 *
	 * @param timestamp timestamp after which task will be ran
	 * @param runnable  runnable of this task
	 * @return scheduledRunnable, which could used for cancelling the task
	 */
	@Override
	public ScheduledRunnable scheduleBackground(long timestamp, Runnable runnable) {
		assert inEventloopThread();
		return addScheduledTask(timestamp, runnable, true);
	}

	private ScheduledRunnable addScheduledTask(long timestamp, Runnable runnable, boolean background) {
		ScheduledRunnable scheduledRunnable = ScheduledRunnable.create(timestamp, runnable);
		PriorityQueue<ScheduledRunnable> taskQueue = background ? backgroundTasks : scheduledTasks;
		taskQueue.offer(scheduledRunnable);
		return scheduledRunnable;
	}

	/**
	 * Notifies the event loop of concurrent operation in another thread(s).
	 * Eventloop will not exit until all concurrent operations are complete.
	 *
	 * @return {@link ConnectionPendingException}, that have method complete() which must be
	 * called after completing concurrent operation.
	 * Failure to call complete() method will prevent the event loop from exiting.
	 */
	public ConcurrentOperationTracker startConcurrentOperation() {
		concurrentOperationsCount.incrementAndGet();

		return new ConcurrentOperationTracker() {
			private final AtomicBoolean complete = new AtomicBoolean(false);

			@Override
			public void complete() {
				if (complete.compareAndSet(false, true)) {
					if (concurrentOperationsCount.decrementAndGet() < 0) {
						logger.error("Concurrent operations count < 0");
					}
				} else {
					logger.error("Concurrent operation is already complete");
				}
			}
		};
	}

	public long refreshTimestampAndGet() {
		timestamp = timeProvider.currentTimeMillis();
		return timestamp;
	}

	/**
	 * Returns current time of this eventloop
	 */
	@Override
	public long currentTimeMillis() {
		return timestamp;
	}

	public String getThreadName() {
		return (eventloopThread == null) ? null : eventloopThread.getName();
	}

	@Override
	public Eventloop getEventloop() {
		return this;
	}

	/**
	 * Interface for reporting to Eventloop about the end of concurrent operation
	 */
	public interface ConcurrentOperationTracker {
		void complete();
	}

	@Override
	public Future<?> submit(final Runnable runnable) {
		return submit(runnable, null);
	}

	@Override
	public Future<?> submit(AsyncRunnable asyncRunnable) {
		return submit(asyncRunnable, null);
	}

	@Override
	public <T> Future<T> submit(final Runnable runnable, final T result) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		execute(new Runnable() {
			@Override
			public void run() {
				Exception exception = null;
				try {
					runnable.run();
				} catch (Exception e) {
					exception = e;
				}
				if (exception == null) {
					future.setResult(result);
				} else {
					future.setException(exception);
				}
			}
		});
		return future;
	}

	@Override
	public <T> Future<T> submit(final AsyncRunnable asyncRunnable, final T result) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		execute(new Runnable() {
			@Override
			public void run() {
				asyncRunnable.run(new CompletionCallback() {
					@Override
					protected void onComplete() {
						future.setResult(result);
					}

					@Override
					protected void onException(Exception e) {
						future.setException(e);
					}
				});
			}
		});
		return future;
	}

	@Override
	public <T> Future<T> submit(final Callable<T> callable) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		execute(new Runnable() {
			@Override
			public void run() {
				T result = null;
				Exception exception = null;
				try {
					result = callable.call();
				} catch (Exception e) {
					exception = e;
				}
				if (exception == null) {
					future.setResult(result);
				} else {
					future.setException(exception);
				}
			}
		});
		return future;
	}

	@Override
	public <T> Future<T> submit(final AsyncCallable<T> asyncCallable) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		execute(new Runnable() {
			@Override
			public void run() {
				asyncCallable.call(future);
			}
		});
		return future;
	}

	public AsyncCancellable runConcurrently(ExecutorService executor,
	                                        final Runnable runnable, final CompletionCallback callback) {
		assert inEventloopThread();

		final ConcurrentOperationTracker tracker = startConcurrentOperation();

		// jmx
		final String taskName = runnable.getClass().getName();
		concurrentCallsStats.recordCall(taskName);
		final long submissionStart = currentTimeMillis();

		try {
			final Future<?> future = executor.submit(new Runnable() {
				@Override
				public void run() {

					// jmx
					final long executingStart = System.currentTimeMillis();

					try {
						runnable.run();

						// jmx
						final long executingFinish = System.currentTimeMillis();

						Eventloop.this.execute(new Runnable() {
							@Override
							public void run() {
								// jmx
								updateConcurrentCallsStatsTimings(
										taskName, submissionStart, executingStart, executingFinish);

								tracker.complete();
								callback.setComplete();
							}
						});
					} catch (final Exception e) {
						// jmx
						final long executingFinish = System.currentTimeMillis();

						final Exception actualException =
								e instanceof RunnableException ? ((RunnableException) e).getActualException() : e;

						Eventloop.this.execute(new Runnable() {
							@Override
							public void run() {
								// jmx
								updateConcurrentCallsStatsTimings(
										taskName, submissionStart, executingStart, executingFinish);

								tracker.complete();
								callback.setException(actualException);
							}
						});
					} catch (final Throwable throwable) {
						recordFatalError(throwable, runnable);
					}
				}
			});
			return new AsyncCancellable() {
				@Override
				public void cancel() {
					future.cancel(true);
				}
			};
		} catch (RejectedExecutionException e) {
			// jmx
			concurrentCallsStats.recordRejectedCall(taskName);

			tracker.complete();
			callback.setException(e);

			return new AsyncCancellable() {
				@Override
				public void cancel() {
					// do nothing
				}
			};
		}
	}

	public <T> AsyncCancellable callConcurrently(ExecutorService executor,
	                                             final Callable<T> callable, final ResultCallback<T> callback) {
		assert inEventloopThread();

		final ConcurrentOperationTracker tracker = startConcurrentOperation();

		// jmx
		final String taskName = callable.getClass().getName();
		concurrentCallsStats.recordCall(taskName);
		final long submissionStart = currentTimeMillis();

		try {
			final Future<?> future = executor.submit(new Runnable() {
				@Override
				public void run() {
					// jmx
					final long executingStart = System.currentTimeMillis();

					try {
						final T result = callable.call();

						// jmx
						final long executingFinish = System.currentTimeMillis();

						Eventloop.this.execute(new Runnable() {
							@Override
							public void run() {
								// jmx
								updateConcurrentCallsStatsTimings(
										taskName, submissionStart, executingStart, executingFinish);

								tracker.complete();
								callback.setResult(result);
							}
						});
					} catch (final Exception e) {
						// jmx
						final long executingFinish = System.currentTimeMillis();

						Eventloop.this.execute(new Runnable() {
							@Override
							public void run() {
								// jmx
								updateConcurrentCallsStatsTimings(
										taskName, submissionStart, executingStart, executingFinish);

								tracker.complete();
								callback.setException(e);
							}
						});
					} catch (final Throwable throwable) {
						recordFatalError(throwable, callable);
					}
				}
			});
			return new AsyncCancellable() {
				@Override
				public void cancel() {
					future.cancel(true);
				}
			};
		} catch (RejectedExecutionException e) {
			// jmx
			concurrentCallsStats.recordRejectedCall(taskName);

			tracker.complete();
			callback.setException(e);

			return new AsyncCancellable() {
				@Override
				public void cancel() {
					// do nothing
				}
			};
		}
	}

	public static void setGlobalFatalErrorHandler(FatalErrorHandler handler) {
		globalFatalErrorHandler = checkNotNull(handler);
	}

	// JMX
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, most of the durationStats are collected only when monitoring is enabled) ]")
	public void startMonitoring() {
		this.monitoring = true;
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, most of the durationStats are collected only when monitoring is enabled) ]")
	public void stopMonitoring() {
		this.monitoring = false;
	}

	@JmxAttribute(
			description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
					"(for example, most of the durationStats are collected only when monitoring is enabled)")
	public boolean isMonitoring() {
		return monitoring;
	}

	@JmxOperation
	public void resetStats() {
		stats.resetStats();
	}

	public void recordIoError(Exception e, Object context) {
		logger.warn("IO Error in {}: {}", context, e.toString());
		stats.recordIoError(e, context);
	}

	public void recordFatalError(final Throwable e, final Object context) {
		if (e instanceof RethrowedError) {
			propagate(e.getCause());
		}
		logger.error("Fatal Error in " + context, e);
		if (fatalErrorHandler != null) {
			handleFatalError(fatalErrorHandler, e, context);
		} else {
			handleFatalError(globalFatalErrorHandler, e, context);
		}
		if (inEventloopThread()) {
			stats.recordFatalError(e, context);
		} else {
			execute(new Runnable() {
				@Override
				public void run() {
					stats.recordFatalError(e, context);
				}
			});
		}
	}

	private void handleFatalError(final FatalErrorHandler handler, final Throwable e, final Object context) {
		if (inEventloopThread()) {
			handler.handle(e, context);
		} else {
			try {
				handler.handle(e, context);
			} catch (final Throwable handlerError) {
				execute(new Runnable() {
					@Override
					public void run() {
						throw new RethrowedError(handlerError);
					}
				});
			}
		}
	}

	private static void propagate(Throwable throwable) {
		if (throwable instanceof Error) {
			throw (Error) throwable;
		} else if (throwable instanceof RuntimeException) {
			throw (RuntimeException) throwable;
		} else {
			throw new RuntimeException(throwable);
		}
	}

	private static class RethrowedError extends Error {
		public RethrowedError(Throwable cause) {
			super(cause);
		}
	}

	private void updateConcurrentCallsStatsTimings(String taskName,
	                                               long submissionStart, long executingStart, long executingFinish) {
		concurrentCallsStats.recordAwaitingStartDuration(
				taskName,
				(int) (executingStart - submissionStart)
		);
		concurrentCallsStats.recordCallDuration(
				taskName,
				(int) (executingFinish - executingStart)
		);
	}

	@JmxAttribute(reducer = JmxReducers.JmxReducerSum.class)
	public long getTick() {
		return tick;
	}

	@JmxAttribute(
			description = "number of concurrent tasks to be executed",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getCurrentConcurrentTasks() {
		return concurrentTasks.size();
	}

	@JmxAttribute(
			description = "number of scheduled tasks to be executed",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getCurrentScheduledTasks() {
		return scheduledTasks.size();
	}

	@JmxAttribute(
			description = "number of background tasks to be executed",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getCurrentBackgroundTasks() {
		return backgroundTasks.size();
	}

	@JmxAttribute(
			description = "amount of local tasks to be executed",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getCurrentLocalTasks() {
		return localTasks.size();
	}

	@JmxAttribute
	public boolean getKeepAlive() {
		return keepAlive;
	}

	@JmxAttribute(name = "")
	public EventloopStats getStats() {
		return stats;
	}

	@JmxAttribute
	public ConcurrentCallsStats getConcurrentCallsStats() {
		return concurrentCallsStats;
	}

	@JmxAttribute
	public double getSmoothingWindow() {
		return smoothingWindow;
	}

	@JmxAttribute
	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;

		stats.setSmoothingWindow(smoothingWindow);
		concurrentCallsStats.setSmoothingWindow(smoothingWindow);
	}
}
