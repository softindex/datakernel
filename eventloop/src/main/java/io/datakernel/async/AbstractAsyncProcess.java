package io.datakernel.async;

import io.datakernel.annotation.Nullable;

public abstract class AbstractAsyncProcess implements AsyncProcess {
	private boolean processComplete;
	private SettableStage<Void> process;

	protected void beforeProcess() {
	}

	protected void afterProcess() {
	}

	public boolean isProcessStarted() {
		return process != null;
	}

	public boolean isProcessComplete() {
		return processComplete;
	}

	protected void completeProcess() {
		process.trySet(null);
	}

	protected void completeProcess(@Nullable Throwable e) {
		if (e == null) {
			process.trySet(null);
		} else {
			closeWithError(e);
		}
	}

	@Override
	public final Stage<Void> process() {
		if (process == null) {
			process = new SettableStage<>();
			process.thenRunEx(() -> processComplete = true);
			beforeProcess();
			doProcess();
			afterProcess();
		}
		return process;
	}

	protected abstract void doProcess();

	@Override
	public final void closeWithError(Throwable e) {
		if (isProcessComplete()) return;
		processComplete = true;
		doCloseWithError(e);
		process.trySetException(e);
	}

	@Override
	public final void cancel() {
		AsyncProcess.super.cancel();
	}

	protected abstract void doCloseWithError(Throwable e);
}
