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

package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * NOTE: this rule must apply before checks like {@link ByteBufRule},
 * so either use Rule annotation with priority (from JUnit 4.13)
 * or use the {@link DatakernelRunner}
 */
public final class EventloopRule implements TestRule {
	@Override
	public Statement apply(Statement base, Description description) {
		return new LambdaStatement(() -> {
			Eventloop eventloop = Eventloop.create()
					.withCurrentThread()
					.withFatalErrorHandler(rethrowOnAnyError());
			base.evaluate();
			eventloop.run();
		});
	}
}
