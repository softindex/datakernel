package io.datakernel.async;

import io.datakernel.async.Stage.Handler.StageCallback;
import io.datakernel.eventloop.Eventloop;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

abstract class AbstractStage<T> implements Stage<T> {

	private static final NextStage COMPLETED_STAGE = new CompletedStage();

	protected NextStage<? super T, ?> next;

	public boolean isComplete() {
		return next == COMPLETED_STAGE;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions", "unchecked"})
	protected void complete(T value) {
		assert !isComplete();
		if (next != null) {
			next.onComplete(value);
			next = COMPLETED_STAGE;
		}
		assert (next = COMPLETED_STAGE) != null;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions", "unchecked", "WeakerAccess"})
	protected void completeExceptionally(Throwable error) {
		assert !isComplete();
		if (next != null) {
			next.onCompleteExceptionally(error);
			next = COMPLETED_STAGE;
		}
		assert (next = COMPLETED_STAGE) != null;
	}

	protected void complete(T value, Throwable error) {
		assert !isComplete();
		if (error == null) {
			complete(value);
		} else {
			completeExceptionally(error);
		}
	}

	protected void tryComplete(T value) {
		if (!isComplete()) {
			complete(value);
		}
	}

	protected void tryCompleteExceptionally(Throwable error) {
		if (!isComplete()) {
			completeExceptionally(error);
		}
	}

	protected void tryComplete(T value, Throwable error) {
		if (!isComplete()) {
			complete(value, error);
		}
	}

	@SuppressWarnings("unchecked")
	private static final class SplittingStage<T> extends NextStage<T, T> {
		private int size;
		private NextStage<? super T, ?>[] list = new NextStage[10];

		private static <T> SplittingStage<T> of(NextStage<? super T, ?> existing, NextStage<? super T, ?> next) {
			SplittingStage<T> stage = new SplittingStage<>();
			stage.list[0] = existing;
			stage.list[1] = next;
			stage.size = 2;
			return stage;
		}

		protected void add(NextStage<? super T, ?> next) {
			if (size >= list.length) {
				list = Arrays.copyOf(list, list.length * 2);
			}
			list[size++] = next;
		}

		@SuppressWarnings("ForLoopReplaceableByForEach")
		@Override
		protected void onComplete(T value) {
			NextStage<? super T, ?>[] list = this.list;
			for (int i = 0; i < size; i++) {
				NextStage<? super T, ?> next = list[i];
				next.onComplete(value);
			}
		}

		@SuppressWarnings("ForLoopReplaceableByForEach")
		@Override
		protected void onCompleteExceptionally(Throwable error) {
			NextStage<? super T, ?>[] list = this.list;
			for (int i = 0; i < size; i++) {
				NextStage<? super T, ?> next = list[i];
				next.onCompleteExceptionally(error);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> Stage<U> then(NextStage<? super T, ? extends U> stage) {
		if (this.next == null) {
			this.next = stage;
		} else {
			if (this.next == COMPLETED_STAGE)
				throw new IllegalStateException("Stage has already been completed");
			if (this.next instanceof SplittingStage) {
				((SplittingStage<T>) this.next).add(stage);
			} else {
				this.next = SplittingStage.of(this.next, stage);
			}
		}
		return (Stage<U>) stage;
	}

	private static abstract class HandlerStage<F, T> extends NextStage<F, T> implements StageCallback<T> {
		@Override
		public void complete(T result) {
			super.complete(result);
		}

		@Override
		public void completeExceptionally(Throwable t) {
			super.completeExceptionally(t);
		}

		@Override
		public void accept(T value, Throwable throwable) {
			if (throwable == null) {
				complete(value);
			} else {
				completeExceptionally(throwable);
			}
		}
	}

	@Override
	public <U> Stage<U> handle(Handler<? super T, U> handler) {
		return then(new HandlerStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				handler.handle(value, null, this);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				handler.handle(null, error, this);
			}

		});
	}

	@Override
	public <U> Stage<U> handleAsync(Handler<? super T, U> handler) {
		return then(new HandlerStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				getCurrentEventloop().post(() -> handler.handle(value, null, this));
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				getCurrentEventloop().post(() -> handler.handle(null, error, this));
			}
		});
	}

	@Override
	public <U> Stage<U> handleAsync(Handler<? super T, U> handler, Executor executor) {
		return then(new HandlerStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				doComplete(value, null);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				doComplete(null, error);
			}

			private void doComplete(T value, Throwable error) {
				Eventloop eventloop = getCurrentEventloop();
				executor.execute(() -> handler.handle(value, error, new StageCallback<U>() {
					@Override
					public void complete(U result) {
						eventloop.execute(() -> complete(result));
					}

					@Override
					public void completeExceptionally(Throwable t) {
						eventloop.execute(() -> completeExceptionally(t));
					}

					@Override
					public void accept(U result, Throwable throwable) {
						if (throwable == null) {
							complete(result);
						} else {
							completeExceptionally(throwable);
						}
					}
				}));
			}
		});
	}

	@Override
	public <U> Stage<U> thenApply(Function<? super T, ? extends U> fn) {
		return then(new NextStage<T, U>() {
			@Override
			protected void onComplete(T result1) {
				U newResult = fn.apply(result1);
				complete(newResult);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		});
	}

	@Override
	public <U> Stage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
		Eventloop eventloop = getCurrentEventloop();
		return then(new NextStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				eventloop.post(() -> {
					U resultValue = fn.apply(value);
					complete(resultValue);
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	@Override
	public Stage<Void> thenAccept(Consumer<? super T> action) {
		return then(new NextStage<T, Void>() {
			@Override
			protected void onComplete(T result1) {
				action.accept(result1);
				complete(null);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}
		});
	}

	@Override
	public Stage<Void> thenAcceptAsync(Consumer<? super T> action) {
		Eventloop eventloop = getCurrentEventloop();
		return then(new NextStage<T, Void>() {
			@Override
			protected void onComplete(T value) {
				eventloop.post(() -> {
					action.accept(value);
					complete(null);
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	@Override
	public Stage<Void> thenRun(Runnable action) {
		return then(new NextStage<T, Void>() {
			@Override
			protected void onComplete(T result1) {
				action.run();
				complete(null);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				completeExceptionally(throwable);
			}

		});
	}

	@Override
	public Stage<Void> thenRunAsync(Runnable action) {
		Eventloop eventloop = getCurrentEventloop();
		return then(new NextStage<T, Void>() {
			@Override
			protected void onComplete(T value) {
				eventloop.post(() -> {
					action.run();
					complete(null);
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	@Override
	public <U> Stage<U> thenCompose(Function<? super T, ? extends Stage<U>> fn) {
		return then(new NextStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				Stage<U> stage = fn.apply(value);
				if (stage instanceof SettableStage) {
					SettableStage<U> settableStage = (SettableStage<U>) stage;
					if (settableStage.isSet()) {
						if (settableStage.exception == null) {
							complete(settableStage.result);
						} else {
							completeExceptionally(settableStage.exception);
						}
						return;
					}
				}
				stage.whenComplete((u, throwable) -> {
					if (throwable == null) {
						complete(u);
					} else {
						completeExceptionally(throwable);
					}
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				this.completeExceptionally(error);
			}
		});
	}

	@Override
	public <U> Stage<U> thenComposeAsync(Function<? super T, ? extends Stage<U>> fn) {
		Eventloop eventloop = getCurrentEventloop();
		return then(new NextStage<T, U>() {
			@Override
			protected void onComplete(T value) {
				eventloop.post(() -> {
					Stage<U> stage = fn.apply(value);
					if (stage instanceof SettableStage) {
						SettableStage<U> settableStage = (SettableStage<U>) stage;
						if (settableStage.isSet()) {
							if (settableStage.exception == null) {
								complete(settableStage.result);
							} else {
								completeExceptionally(settableStage.exception);
							}
							return;
						}
					}
					stage.whenComplete((u, throwable) -> {
						if (throwable == null) {
							complete(u);
						} else {
							completeExceptionally(throwable);
						}
					});
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> completeExceptionally(error));
			}
		});
	}

	@Override
	public Stage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
		return then(new NextStage<T, T>() {
			@Override
			protected void onComplete(T result1) {
				action.accept(result1, null);
				complete(result1);
			}

			@Override
			protected void onCompleteExceptionally(Throwable throwable) {
				action.accept(null, throwable);
				completeExceptionally(throwable);
			}
		});
	}

	@Override
	public Stage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
		Eventloop eventloop = getCurrentEventloop();
		return then(new NextStage<T, T>() {
			@Override
			protected void onComplete(T value) {
				eventloop.post(() -> {
					action.accept(value, null);
					complete(value);
				});
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				eventloop.post(() -> {
					action.accept(null, error);
					completeExceptionally(error);
				});
			}
		});
	}

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		CompletableFuture<T> future = new CompletableFuture<>();
		then(new NextStage<T, Object>() {
			@Override
			protected void onComplete(T value) {
				future.complete(value);
			}

			@Override
			protected void onCompleteExceptionally(Throwable error) {
				future.completeExceptionally(error);
			}
		});
		return future;
	}

	private static class CompletedStage extends NextStage {
		@Override
		protected void onComplete(Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void onCompleteExceptionally(Throwable error) {
			throw new UnsupportedOperationException();
		}
	}

}
