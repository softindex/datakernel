package io.datakernel.async;

import io.datakernel.annotation.Nullable;

public abstract class AbstractAsyncProcess implements AsyncProcess {
	private boolean processStarted;
	private boolean processComplete;
	private SettableStage<Void> processResult = new SettableStage<>();

	protected void beforeProcess() {
	}

	protected void afterProcess() {
	}

	public boolean isProcessStarted() {
		return processStarted;
	}

	public boolean isProcessComplete() {
		return processComplete;
	}

	protected void completeProcess() {
		completeProcess(null);
	}

	protected void completeProcess(@Nullable Throwable e) {
		processComplete = true;
		if (e == null) {
			processResult.trySet(null);
		} else {
			closeWithError(e);
		}
	}

	@Override
	public MaterializedStage<Void> getResult() {
		return processResult;
	}

	@Override
	public final MaterializedStage<Void> start() {
		if (!processStarted) {
			processStarted = true;
			beforeProcess();
			doProcess();
			afterProcess();
		}
		return processResult;
	}

	protected abstract void doProcess();

	@Override
	public final void closeWithError(Throwable e) {
		if (isProcessComplete()) return;
		processComplete = true;
		doCloseWithError(e);
		processResult.trySetException(e);
	}

	@Override
	public final void cancel() {
		AsyncProcess.super.cancel();
	}

	protected abstract void doCloseWithError(Throwable e);
}
