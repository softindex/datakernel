package io.datakernel.launchers.adapters;

import io.datakernel.async.callback.Callback;
import io.datakernel.async.service.EventloopService;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.net.BlockingSocketServer;
import io.datakernel.net.EventloopServer;
import io.datakernel.service.ServiceAdapter;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

public final class ServiceAdapters {

	public static ServiceAdapter<EventloopService> forEventloopService() {
		return new ServiceAdapter<EventloopService>() {
			@Override
			public CompletableFuture<?> start(EventloopService instance, Executor executor) {
				CompletableFuture<?> future = new CompletableFuture<>();
				instance.getEventloop().execute(wrapContext(instance, () -> instance.start().whenComplete(completeFuture(future))));
				return future;
			}

			@Override
			public CompletableFuture<?> stop(EventloopService instance, Executor executor) {
				CompletableFuture<?> future = new CompletableFuture<>();
				instance.getEventloop().execute(wrapContext(instance, () -> instance.stop().whenComplete(completeFuture(future))));
				return future;
			}
		};
	}

	public static ServiceAdapter<EventloopServer> forEventloopServer() {
		return new ServiceAdapter<EventloopServer>() {
			@Override
			public CompletableFuture<?> start(EventloopServer instance, Executor executor) {
				CompletableFuture<?> future = new CompletableFuture<>();
				instance.getEventloop().execute(wrapContext(instance, () -> {
					try {
						instance.listen();
						future.complete(null);
					} catch (IOException e) {
						future.completeExceptionally(e);
					}
				}));
				return future;
			}

			@Override
			public CompletableFuture<?> stop(EventloopServer instance, Executor executor) {
				CompletableFuture<?> future = new CompletableFuture<>();
				instance.getEventloop().execute(wrapContext(instance, () -> instance.close().whenComplete(completeFuture(future))));
				return future;
			}
		};
	}

	public static ServiceAdapter<Eventloop> forEventloop(ThreadFactory threadFactory) {
		return new ServiceAdapter<Eventloop>() {
			@Override
			public CompletableFuture<?> start(Eventloop eventloop, Executor executor) {
				CompletableFuture<?> future = new CompletableFuture<>();
				threadFactory.newThread(() -> {
					eventloop.keepAlive(true);
					future.complete(null);
					eventloop.run();
				}).start();
				return future;
			}

			@Override
			public CompletableFuture<?> stop(Eventloop eventloop, Executor executor) {
				Thread eventloopThread = eventloop.getEventloopThread();
				if (eventloopThread == null) {
					// already stopped
					return CompletableFuture.completedFuture(null);
				}
				CompletableFuture<?> future = new CompletableFuture<>();
				eventloop.execute(() -> {
					eventloop.keepAlive(false);
					logStopping(eventloop);
					Eventloop.logger.info("Waiting for " + eventloop);
				});
				executor.execute(() -> {
					try {
						eventloopThread.join();
						future.complete(null);
					} catch (InterruptedException e) {
						future.completeExceptionally(e);
					}
				});
				return future;
			}

			private void logStopping(Eventloop eventloop) {
				eventloop.delayBackground(1000L, () -> {
					if (eventloop.getEventloopThread() != null) {
						Eventloop.logger.info("...Waiting for " + eventloop);
						logStopping(eventloop);
					}
				});
			}
		};
	}

	public static ServiceAdapter<Eventloop> forEventloop() {
		return forEventloop(r -> {
			Thread thread = Executors.defaultThreadFactory().newThread(r);
			thread.setName("eventloop: " + thread.getName());
			return thread;
		});
	}

	public static ServiceAdapter<BlockingSocketServer> forBlockingSocketServer() {
		return new io.datakernel.service.ServiceAdapters.SimpleServiceAdapter<BlockingSocketServer>() {
			@Override
			protected void start(BlockingSocketServer instance) throws Exception {
				instance.start();
			}

			@Override
			protected void stop(BlockingSocketServer instance) throws Exception {
				instance.stop();
			}
		};
	}

	private static <T> Callback<T> completeFuture(CompletableFuture<?> future) {
		return ($, e) -> {
			if (e == null) {
				future.complete(null);
			} else {
				future.completeExceptionally(e);
			}
		};
	}

}
