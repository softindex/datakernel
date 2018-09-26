/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.datakernel.serial.processor;

import io.datakernel.async.Stage;

public abstract class SimpleSerialTransformer<S extends SimpleSerialTransformer<S, I, O>, I, O>
		extends AbstractSerialTransformer<S, I, O> {

	protected abstract Stage<Void> handle(I item);

	@Override
	protected final void doProcess() {
		input.get()
				.whenComplete((item, e) -> {
					if (e == null) {
						if (item == null) {
							output.accept(null)
									.thenRun(this::completeProcess);
							return;
						}
						handle(item).whenComplete(($, e2) -> {
							if (e2 == null) {
								doProcess();
							} else {
								closeWithError(e2);
							}
						});
					} else {
						closeWithError(e);
					}
				});
	}
}
