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
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.SettableStage;
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
import io.datakernel.util.MutableBuilder;
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
import java.util.concurrent.atomic.AtomicInteger;

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
public final class Eventloop implements Runnable, EventloopExecutor, Scheduler, CurrentTimeProvider, MutableBuilder<Eventloop>, EventloopJmxMBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final AsyncTimeoutException CONNECT_TIMEOUT = new AsyncTimeoutException("Connection timed out");
	private static final long DEFAULT_IDLE_INTERVAL = 1000L;

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
	private final AtomicInteger externalTasksCount = new AtomicInteger(0);

	private final CurrentTimeProvider timeProvider;

	private long timeAfterSelectorSelect;
	private long timeAfterBusinessLogic;

	/**
	 * The NIO selector which selects a set of keys whose corresponding channels
	 */
	private Selector selector;

	private SelectorProvider selectorProvider;

	/**
	 * The thread where eventloop is running
	 */
	private Thread eventloopThread;

	private static final ThreadLocal<Eventloop> CURRENT_EVENTLOOP = new ThreadLocal<>();
	/**
	 * The desired name of the thread
	 */
	private String threadName;
	private int threadPriority;

	private FatalErrorHandler fatalErrorHandler;

	private volatile boolean keepAlive;
	private volatile boolean breakEventloop;

	private long tick;

	/**
	 * Current time, cached to avoid System.currentTimeMillis() system calls, and to facilitate unit testing.
	 * It is being refreshed with each event loop execution.
	 */
	private long timestamp;

	private long idleInterval = DEFAULT_IDLE_INTERVAL;

	private ThrottlingController throttlingController;
	private int throttlingKeys;

	/**
	 * Count of selected keys for last Selector.select()
	 */
	private int lastSelectedKeys;
	private int lastExternalTasksCount;

	// JMX

	private static final double DEFAULT_SMOOTHING_WINDOW = ValueStats.SMOOTHING_WINDOW_1_MINUTE;
	private double smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
	private final EventloopStats stats = EventloopStats.create(DEFAULT_SMOOTHING_WINDOW, new ExtraStatsExtractor());
	private final ExecutorCallsStats executorsCallsStats = ExecutorCallsStats.create(DEFAULT_SMOOTHING_WINDOW);

	private boolean monitoring = false;

	// region builders
	private Eventloop(CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		refreshTimestamp();
	}

	public static Eventloop create() {
		return create(CurrentTimeProviderSystem.instance());
	}

	public static Eventloop create(CurrentTimeProvider currentTimeProvider) {
		return new Eventloop(currentTimeProvider);
	}

	public Eventloop withThreadName(String threadName) {
		this.threadName = threadName;
		return this;
	}

	public Eventloop withThreadPriority(int threadPriority) {
		this.threadPriority = threadPriority;
		return this;
	}

	public Eventloop withThrottlingController(ThrottlingController throttlingController) {
		this.throttlingController = throttlingController;
		if (throttlingController != null) {
			throttlingController.setEventloop(this);
		}
		return this;
	}

	public Eventloop withFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
		this.fatalErrorHandler = fatalErrorHandler;
		return this;
	}

	public Eventloop withSelectorProvider(SelectorProvider selectorProvider) {
		this.selectorProvider = selectorProvider;
		return this;
	}

	public Eventloop withIdleInterval(long idleIntervalMillis) {
		this.idleInterval = idleIntervalMillis;
		return this;
	}

	public Eventloop withCurrentThread() {
		CURRENT_EVENTLOOP.set(this);
		return this;
	}

	// endregion

	public final class Scope implements AutoCloseable {
		private final Eventloop previousEventloop;
		private boolean closed;

		public Scope(Eventloop previousEventloop) {
			this.previousEventloop = previousEventloop;
		}

		@Override
		public void close() {
			if (closed) return;
			closed = true;
			if (previousEventloop == null) {
				CURRENT_EVENTLOOP.remove();
			} else {
				CURRENT_EVENTLOOP.set(previousEventloop);
			}
		}
	}

	public Scope useCurrentThread() {
		Eventloop previousEventloop = CURRENT_EVENTLOOP.get();
		CURRENT_EVENTLOOP.set(this);
		return new Scope(previousEventloop);
	}

	private static final String NO_CURRENT_EVENTLOOP_ERROR = "Trying to start async operations prior eventloop.run(), or from outside of eventloop.run() \n" +
			"Possible solutions: " +
			"1) Eventloop.create().withCurrentThread() ... {your code block} ... eventloop.run() \n" +
			"2) try_with_resources Eventloop.useCurrentThread() ... {your code block} \n" +
			"3) refactor application so it starts async operations within eventloop.run(), \n" +
			"   i.e. by implementing EventloopService::start() {your code block} and using ServiceGraphModule";

	public static Eventloop getCurrentEventloop() {
		Eventloop eventloop = CURRENT_EVENTLOOP.get();
		if (eventloop != null) {
			return eventloop;
		}
		throw new IllegalStateException(NO_CURRENT_EVENTLOOP_ERROR);
	}

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

	private boolean isAlive() {
		if (breakEventloop)
			return false;
		lastExternalTasksCount = externalTasksCount.get();
		return !localTasks.isEmpty() || !scheduledTasks.isEmpty() || !concurrentTasks.isEmpty()
				|| lastExternalTasksCount > 0
				|| keepAlive || !selector.keys().isEmpty();
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
			if (!isAlive()) {
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

			int keys = processSelectedKeys(selector.selectedKeys());
			int concurrentTasks = executeConcurrentTasks();
			int scheduledTasks = executeScheduledTasks();
			int backgroundTasks = executeBackgroundTasks();
			int localTasks = executeLocalTasks();

			updateBusinessLogicStats(keys + concurrentTasks + scheduledTasks + backgroundTasks + localTasks);

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

	private void updateBusinessLogicStats(int tasksAndKeys) {
		timeAfterBusinessLogic = timestamp; //refreshTimestampAndGet();
		long businessLogicTime = timeAfterBusinessLogic - timeAfterSelectorSelect;
		stats.updateBusinessLogicTime(tasksAndKeys, lastExternalTasksCount, businessLogicTime);
		if (throttlingController != null) {
			throttlingController.updateInternalStats(throttlingKeys, (int) businessLogicTime);
		}
	}

	private long getSelectTimeout() {
		if (!concurrentTasks.isEmpty() || !localTasks.isEmpty())
			return 0L;
		if (scheduledTasks.isEmpty() && backgroundTasks.isEmpty())
			return idleInterval;
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
		return idleInterval;
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
		SettableStage<SocketChannel> stage = SettableStage.create();
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
	private SettableStage<SocketChannel> timeoutConnectStage(SocketChannel socketChannel, long connectionTime, SettableStage<SocketChannel> stage) {
		if (connectionTime == 0) return stage;

		ScheduledRunnable scheduledTimeout = schedule(currentTimeMillis() + connectionTime, () -> {
			closeQuietly(socketChannel);
			stage.setException(CONNECT_TIMEOUT);
		});

		SettableStage<SocketChannel> timeoutStage = SettableStage.create();
		timeoutStage.whenComplete((socketChannel1, throwable) -> {
			assert !scheduledTimeout.isComplete();
			scheduledTimeout.cancel();
			stage.set(socketChannel1, throwable);
		});

		return timeoutStage;
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
	public void startExternalTask() {
		externalTasksCount.incrementAndGet();
	}

	public void completeExternalTask() {
		externalTasksCount.decrementAndGet();
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

	@Override
	public Eventloop getEventloop() {
		return this;
	}

	@Override
	public CompletableFuture<Void> submit(Runnable runnable) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		execute(() -> {
			Exception exception = null;
			try {
				runnable.run();
			} catch (Exception e) {
				exception = e;
			}
			if (exception == null) {
				future.complete(null);
			} else {
				future.completeExceptionally(exception);
			}
		});
		return future;
	}

	@Override
	public <T> CompletableFuture<T> submit(Callable<T> callable) {
		CompletableFuture<T> future = new CompletableFuture<>();
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
	public <T> CompletableFuture<T> submit(AsyncCallable<T> asyncCallable) {
		CompletableFuture<T> future = new CompletableFuture<>();
		execute(() -> asyncCallable.call().whenComplete((t, throwable) -> {
			if (throwable == null) {
				future.complete(t);
			} else {
				future.completeExceptionally(throwable);
			}
		}));
		return future;
	}

	public <T> CompletionStage<T> callExecutor(ExecutorService executor, Callable<T> callable) {
		assert inEventloopThread();
		SettableStage<T> stage = SettableStage.create();
		startExternalTask();
		// jmx
		String taskName = callable.getClass().getName();
		executorsCallsStats.recordCall(taskName);
		long submissionStart = currentTimeMillis();
		try {
			executor.submit(() -> {
				// jmx
				long executingStart = System.currentTimeMillis();
				try {
					T result = callable.call();
					// jmx
					long executingFinish = System.currentTimeMillis();
					this.execute(() -> {
						// jmx
						updateConcurrentCallsStatsTimings(taskName, submissionStart, executingStart, executingFinish);
						completeExternalTask();
						stage.set(result);
					});
				} catch (Exception e) {
					// jmx
					long executingFinish = System.currentTimeMillis();
					this.execute(() -> {
						// jmx
						updateConcurrentCallsStatsTimings(taskName, submissionStart, executingStart, executingFinish);
						completeExternalTask();
						stage.setException(e);
					});
				} catch (Throwable throwable) {
					recordFatalError(throwable, callable);
				}
			});
		} catch (RejectedExecutionException e) {
			// jmx
			executorsCallsStats.recordRejectedCall(taskName);
			completeExternalTask();
			stage.setException(e);
		}
		return stage;
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

	public void recordFatalError(Throwable e, Object context) {
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

	private void handleFatalError(FatalErrorHandler handler, Throwable e, Object context) {
		if (inEventloopThread()) {
			handler.handle(e, context);
		} else {
			try {
				handler.handle(e, context);
			} catch (Throwable handlerError) {
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
		executorsCallsStats.recordAwaitingStartDuration(taskName, (int) (executingStart - submissionStart));
		executorsCallsStats.recordCallDuration(taskName, (int) (executingFinish - executingStart));
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
	public ExecutorCallsStats getExecutorsCallsStats() {
		return executorsCallsStats;
	}

	@JmxAttribute
	public double getSmoothingWindow() {
		return smoothingWindow;
	}

	@JmxAttribute
	public void setSmoothingWindow(double smoothingWindow) {
		this.smoothingWindow = smoothingWindow;

		stats.setSmoothingWindow(smoothingWindow);
		executorsCallsStats.setSmoothingWindow(smoothingWindow);
	}

	@JmxAttribute
	public long getIdleInterval() {
		return idleInterval;
	}

	@JmxAttribute
	public void setIdleInterval(long idleInterval) {
		this.idleInterval = idleInterval;
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

	@Override
	public String toString() {
		return threadName != null ? threadName : super.toString();
	}
}
