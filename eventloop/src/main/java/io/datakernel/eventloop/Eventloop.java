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
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.SimpleException;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.ValueStats;
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

import static io.datakernel.async.AsyncCallbacks.completionToStage;
import static io.datakernel.async.AsyncCallbacks.resultToStage;
import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * It is internal class for asynchronous programming. In asynchronous
 * programming model, blocking operations (like I/O or long-running computations)
 * in {@code Eventloop} thread must be avoided. Async versions
 * of such operations should be used.
 * <p>
 * Eventloop represents infinite loop with only one blocking operation
 * {@code selector.select()} which selects a set of keys whose corresponding
 * channels are ready for I/O operations. With these keys and queues with
 * tasks, which was added to {@code Eventloop} from the outside, it begins
 * asynchronous executing from one thread it in method {@code run()} which is
 * overridden because it is implementation of {@link Runnable}. Working of this
 * eventloop will be ended, when it has not selected keys and its queues with
 * tasks are empty.
 */
public final class Eventloop implements Runnable, EventloopExecutor, Scheduler, CurrentTimeProvider, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final AsyncTimeoutException CONNECT_TIMEOUT = new AsyncTimeoutException("Connection timed out");
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

	private long timeAfterSelectorSelect;
	private long timeAfterBusinessLogic;

	/**
	 * The NIO selector which selects a set of keys whose corresponding channels
	 */
	private Selector selector;

	private final SelectorProvider selectorProvider;

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
	private int throttlingKeys;

	/**
	 * Count of selected keys for last Selector.select()
	 */
	private int lastSelectedKeys;

	// JMX

	private static final double DEFAULT_SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;
	private double smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
	private final EventloopStats stats = EventloopStats.create(DEFAULT_SMOOTHING_WINDOW, new ExtraStatsExtractor());
	private final ConcurrentCallsStats concurrentCallsStats = ConcurrentCallsStats.create(DEFAULT_SMOOTHING_WINDOW);

	private boolean monitoring = false;

	// region builders
	private Eventloop(CurrentTimeProvider timeProvider, String threadName, int threadPriority,
	                  ThrottlingController throttlingController, FatalErrorHandler fatalErrorHandler, SelectorProvider selectorProvider) {
		this.timeProvider = timeProvider;
		this.threadName = threadName;
		this.threadPriority = threadPriority;
		this.fatalErrorHandler = fatalErrorHandler;
		this.throttlingController = throttlingController;
		this.selectorProvider = selectorProvider;
		if (throttlingController != null) {
			throttlingController.setEventloop(this);
		}
		refreshTimestamp();
		CURRENT_EVENTLOOP.set(this);
	}

	public static Eventloop create() {
		return new Eventloop(CurrentTimeProviderSystem.instance(), null, 0, null, null, null);
	}

	public Eventloop withCurrentTimeProvider(CurrentTimeProvider timeProvider) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler, selectorProvider);
	}

	public Eventloop withThreadName(String threadName) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler, selectorProvider);
	}

	public Eventloop withThreadPriority(int threadPriority) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler, selectorProvider);
	}

	public Eventloop withThrottlingController(ThrottlingController throttlingController) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler, selectorProvider);
	}

	public Eventloop withFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler, selectorProvider);
	}

	public Eventloop withSelectorProvider(SelectorProvider selectorProvider) {
		return new Eventloop(timeProvider, threadName, threadPriority, throttlingController, fatalErrorHandler, selectorProvider);
	}

	// endregion

	public ThrottlingController getThrottlingController() {
		return throttlingController;
	}

	private void openSelector() {
		if (selector == null) {
			try {
				selector = (selectorProvider != null ? selectorProvider : SelectorProvider.provider()).openSelector();
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
		return checkNotNull(CURRENT_EVENTLOOP.get());
	}

	public Thread getEventloopThread() {
		return eventloopThread;
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

		timeAfterBusinessLogic = timeAfterSelectorSelect = 0;
		while (true) {
			if (!isKeepAlive()) {
				logger.info("Eventloop {} is complete, exiting...", this);
				break;
			}
			try {
				long selectTimeout = getSelectTimeout();
				stats.updateSelectorSelectTimeout(selectTimeout);
				if (selectTimeout <= 0) {
					lastSelectedKeys = selector.selectNow();
				} else {
					lastSelectedKeys = selector.select(selectTimeout);
				}
			} catch (ClosedChannelException e) {
				logger.error("Selector is closed, exiting...", e);
				break;
			} catch (IOException e) {
				recordIoError(e, selector);
			}
			updateSelectorSelectStats();

			final int keys = processSelectedKeys(selector.selectedKeys());
			final int concurrentTasks = executeConcurrentTasks();
			final int scheduledTasks = executeScheduledTasks();
			final int backgroundTasks = executeBackgroundTasks();
			final int localTasks = executeLocalTasks();

			stats.updateProcessedTasksAndKeys(keys + concurrentTasks + scheduledTasks + backgroundTasks + localTasks);

			updateBusinessLogicStats();
			tick = (tick + (1L << 32)) & ~0xFFFFFFFFL;
		}
		logger.info("Eventloop {} finished", this);
		eventloopThread = null;
		if (selector.keys().isEmpty()) {
			closeSelector();
		} else {
			logger.warn("Selector is still open, because event loop {} has {} keys", this, selector.keys());
		}
	}

	private void updateSelectorSelectStats() {
		timeAfterSelectorSelect = refreshTimestampAndGet();
		if (timeAfterBusinessLogic != 0) {
			long selectorSelectTime = timeAfterSelectorSelect - timeAfterBusinessLogic;
			stats.updateSelectorSelectTime(selectorSelectTime);
		}
		if (throttlingController != null) {
			throttlingKeys = lastSelectedKeys + concurrentTasks.size();
			throttlingController.calculateThrottling(throttlingKeys);
		}
	}

	private void updateBusinessLogicStats() {
		timeAfterBusinessLogic = timestamp; //refreshTimestampAndGet();
		long businessLogicTime = timeAfterBusinessLogic - timeAfterSelectorSelect;
		stats.updateBusinessLogicTime(businessLogicTime);
		if (throttlingController != null) {
			throttlingController.updateInternalStats(throttlingKeys, (int) businessLogicTime);
		}
	}

	private long getSelectTimeout() {
		if (!concurrentTasks.isEmpty() || !localTasks.isEmpty())
			return 0L;
		if (scheduledTasks.isEmpty() && backgroundTasks.isEmpty())
			return DEFAULT_EVENT_TIMEOUT;
		return Math.min(getTimeBeforeExecution(scheduledTasks), getTimeBeforeExecution(backgroundTasks));
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
	private int processSelectedKeys(Set<SelectionKey> selectedKeys) {
		long startTimestamp = timestamp;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		int invalidKeys = 0, acceptKeys = 0, connectKeys = 0, readKeys = 0, writeKeys = 0;

		Iterator<SelectionKey> iterator = lastSelectedKeys != 0 ? selectedKeys.iterator()
				: Collections.<SelectionKey>emptyIterator();
		while (iterator.hasNext()) {
			SelectionKey key = iterator.next();
			iterator.remove();

			if (!key.isValid()) {
				invalidKeys++;
				continue;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
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
				if (sw != null) stats.updateSelectedKeyDuration(sw);
			} catch (Throwable e) {
				recordFatalError(e, key.attachment());
				closeQuietly(key.channel());
			}
		}
		long loopTime = refreshTimestampAndGet() - startTimestamp;
		stats.updateSelectedKeysStats(lastSelectedKeys,
				invalidKeys, acceptKeys, connectKeys, readKeys, writeKeys, loopTime);

		return acceptKeys + connectKeys + readKeys + writeKeys + invalidKeys;
	}

	/**
	 * Executes local tasks which were added from current thread
	 */
	private int executeLocalTasks() {
		long startTimestamp = timestamp;

		int newRunnables = 0;

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
		long loopTime = refreshTimestampAndGet() - startTimestamp;
		stats.updateLocalTasksStats(newRunnables, loopTime);

		return newRunnables;
	}

	/**
	 * Executes concurrent tasks which were added from other threads.
	 */
	private int executeConcurrentTasks() {
		long startTimestamp = timestamp;

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
		long loopTime = refreshTimestampAndGet() - startTimestamp;
		stats.updateConcurrentTasksStats(newRunnables, loopTime);

		return newRunnables;
	}

	/**
	 * Executes tasks, scheduled for execution at particular timestamps
	 */
	private int executeScheduledTasks() {
		return executeScheduledTasks(scheduledTasks);
	}

	private int executeBackgroundTasks() {
		return executeScheduledTasks(backgroundTasks);
	}

	private int executeScheduledTasks(PriorityQueue<ScheduledRunnable> taskQueue) {
		long startTimestamp = timestamp;
		boolean background = taskQueue == backgroundTasks;

		int newRunnables = 0;
		Stopwatch sw = monitoring ? Stopwatch.createUnstarted() : null;

		for (; ; ) {
			ScheduledRunnable peeked = taskQueue.peek();
			if (peeked == null)
				break;
			if (peeked.isCancelled()) {
				taskQueue.poll();
				continue;
			}
			if (peeked.getTimestamp() > currentTimeMillis()) {
				break;
			}
			ScheduledRunnable polled = taskQueue.poll();
			assert polled == peeked;

			Runnable runnable = polled.getRunnable();
			if (sw != null) {
				sw.reset();
				sw.start();
			}

			if (monitoring) {
				int overdue = (int) (System.currentTimeMillis() - peeked.getTimestamp());
				stats.recordScheduledTaskOverdue(overdue, background);
			}

			try {
				runnable.run();
				tick++;
				polled.complete();
				if (sw != null)
					stats.updateScheduledTaskDuration(runnable, sw, background);
			} catch (Throwable e) {
				recordFatalError(e, runnable);
			}

			newRunnables++;
		}

		long loopTime = refreshTimestampAndGet() - startTimestamp;
		stats.updateScheduledTasksStats(newRunnables, loopTime, background);

		return newRunnables;
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
		SettableStage<SocketChannel> connectStage = (SettableStage<SocketChannel>) key.attachment();
		SocketChannel channel = (SocketChannel) key.channel();
		boolean connected;
		try {
			connected = channel.finishConnect();
		} catch (IOException e) {
			closeQuietly(channel);
			connectStage.setException(e);
			return;
		}

		if (connected) {
			connectStage.set(channel);
		} else {
			connectStage.setException(new SimpleException("Not connected"));
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
	 * @throws IOException If some I/O error occurs
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
	 * @param address socketChannel's address
	 */
	public CompletionStage<SocketChannel> connect(SocketAddress address) {
		return connect(address, 0);
	}

	/**
	 * Connects to given socket address asynchronously with a specified timeout value.
	 * A timeout of zero is interpreted as an default system timeout
	 *
	 * @param address socketChannel's address
	 * @param timeout the timeout value to be used in milliseconds, 0 as default system connection timeout
	 */
	public CompletionStage<SocketChannel> connect(SocketAddress address, int timeout) {
		assert inEventloopThread();
		SocketChannel socketChannel = null;
		final SettableStage<SocketChannel> stage = SettableStage.create();
		try {
			socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);
			socketChannel.connect(address);
			socketChannel.register(ensureSelector(), SelectionKey.OP_CONNECT,
					timeoutConnectStage(socketChannel, timeout, stage));

		} catch (IOException e) {
			closeQuietly(socketChannel);
			try {
				stage.setException(e);
			} catch (Throwable e1) {
				recordFatalError(e1, stage);
			}
		}
		return stage;
	}

	/**
	 * Returns modified connect stage to enable timeout.
	 * If connectionTime is zero, method returns input connect stage.
	 * Otherwise schedules special task that will close SocketChannel and call onException method in case of timeout.
	 * If there is no timeout before connection - onConnect method will be called
	 */
	private SettableStage<SocketChannel> timeoutConnectStage(final SocketChannel socketChannel, final long connectionTime, SettableStage<SocketChannel> stage) {
		if (connectionTime == 0) return stage;

		final ScheduledRunnable scheduledTimeout = schedule(currentTimeMillis() + connectionTime, () -> {
			closeQuietly(socketChannel);
			stage.setException(CONNECT_TIMEOUT);
		});

		final SettableStage<SocketChannel> timeoutStage = SettableStage.create();
		timeoutStage.whenComplete((socketChannel1, throwable) -> {
			assert !scheduledTimeout.isComplete();
			scheduledTimeout.cancel();
			AsyncCallbacks.forwardTo(stage, socketChannel1, throwable);
		});

		return timeoutStage;
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
		ScheduledRunnable scheduledTask = ScheduledRunnable.create(timestamp, runnable);
		PriorityQueue<ScheduledRunnable> taskQueue = background ? backgroundTasks : scheduledTasks;
		taskQueue.offer(scheduledTask);
		return scheduledTask;
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
		refreshTimestamp();
		return timestamp;
	}

	private void refreshTimestamp() {
		timestamp = timeProvider.currentTimeMillis();
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
	public CompletableFuture<Void> submit(final Runnable runnable) {
		return submit(runnable, null);
	}

	@Override
	public CompletableFuture<Void> submit(AsyncRunnable asyncRunnable) {
		return submit(asyncRunnable, null);
	}

	@Override
	public <T> CompletableFuture<T> submit(final Runnable runnable, final T result) {
		final CompletableFuture<T> future = new CompletableFuture<>();
		execute(() -> {
			Exception exception = null;
			try {
				runnable.run();
			} catch (Exception e) {
				exception = e;
			}
			if (exception == null) {
				future.complete(result);
			} else {
				future.completeExceptionally(exception);
			}
		});
		return future;
	}

	@Override
	public <T> CompletableFuture<T> submit(final AsyncRunnable asyncRunnable, final T result) {
		final CompletableFuture<T> future = new CompletableFuture<>();
		execute(() -> asyncRunnable.run().whenComplete(($, throwable) -> {
			if (throwable == null) {
				future.complete(result);
			} else {
				future.completeExceptionally(AsyncCallbacks.throwableToException(throwable));
			}
		}));
		return future;
	}

	@Override
	public <T> CompletableFuture<T> submit(final Callable<T> callable) {
		final CompletableFuture<T> future = new CompletableFuture<>();
		execute(() -> {
			T result = null;
			Exception exception = null;
			try {
				result = callable.call();
			} catch (Exception e) {
				exception = e;
			}
			if (exception == null) {
				future.complete(result);
			} else {
				future.completeExceptionally(exception);
			}
		});
		return future;
	}

	@Override
	public <T> CompletableFuture<T> submit(final AsyncCallable<T> asyncCallable) {
		final CompletableFuture<T> future = new CompletableFuture<>();
		execute(() -> asyncCallable.call().whenComplete((t, throwable) -> {
			if (throwable == null) {
				future.complete(t);
			} else {
				future.completeExceptionally(throwable);
			}
		}));
		return future;
	}

	public CompletionStage<Void> runConcurrently(ExecutorService executor, final Runnable runnable) {
		SettableStage<Void> stage = SettableStage.create();
		runConcurrently(executor, runnable, completionToStage(stage));
		return stage;
	}

	public CompletionStage<Void> runConcurrentlyWithException(ExecutorService executor, final RunnableWithException runnable) {
		SettableStage<Void> stage = SettableStage.create();
		runConcurrently(executor, () -> {
			try {
				runnable.runWithException();
			} catch (Exception e) {
				throw new RunnableException(e);
			}
		}, completionToStage(stage));
		return stage;
	}

	private AsyncCancellable runConcurrentlyWithException(ExecutorService executor,
	                                                      final RunnableWithException runnable, final CompletionCallback callback) {
		return runConcurrently(executor, () -> {
			try {
				runnable.runWithException();
			} catch (Exception e) {
				throw new RunnableException(e);
			}
		}, callback);
	}

	private AsyncCancellable runConcurrently(ExecutorService executor,
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

	public <T> CompletionStage<T> callConcurrently(ExecutorService executor, final Callable<T> callable) {
		SettableStage<T> stage = SettableStage.create();
		callConcurrently(executor, callable, resultToStage(stage));
		return stage;
	}

	private <T> AsyncCancellable callConcurrently(ExecutorService executor,
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
	public void startExtendedMonitoring() {
		this.monitoring = true;
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, most of the durationStats are collected only when monitoring is enabled) ]")
	public void stopExtendedMonitoring() {
		this.monitoring = false;
	}

	@JmxAttribute(
			description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
					"(for example, most of the durationStats are collected only when monitoring is enabled)")
	public boolean isExtendedMonitoring() {
		return monitoring;
	}

	@JmxOperation
	public void resetStats() {
		stats.reset();
	}

	private void recordIoError(Exception e, Object context) {
		logger.warn("IO Error in {}: {}", context, e.toString());
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

	public int getTick() {
		return (int) (tick >>> 32);
	}

	public long getMicroTick() {
		return tick;
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

	final class ExtraStatsExtractor {
		int getLocalTasksCount() {
			return localTasks.size();
		}

		int getConcurrentTasksCount() {
			return concurrentTasks.size();
		}

		int getScheduledTasksCount() {
			return scheduledTasks.size();
		}

		int getBackgroundTasksCount() {
			return backgroundTasks.size();
		}
	}
}
