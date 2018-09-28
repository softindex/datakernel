/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.async;

import io.datakernel.annotation.Nullable;

public abstract class AbstractAsyncProcess implements AsyncProcess {
	private boolean processStarted;
	private boolean processComplete;
	private SettableStage<Void> processResult = new SettableStage<>();

	protected void beforeProcess() {
	}

	protected void afterProcess(@Nullable Throwable e) {
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
		if (isProcessComplete()) return;
		processComplete = true;
		if (e == null) {
			processResult.trySet(null);
			afterProcess(null);
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
		afterProcess(e);
	}

	@Override
	public final void cancel() {
		AsyncProcess.super.cancel();
	}

	protected abstract void doCloseWithError(Throwable e);
}
