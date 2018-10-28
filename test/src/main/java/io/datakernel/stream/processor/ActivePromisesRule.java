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

import io.datakernel.test.TestUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.junit.Assert.assertEquals;

/**
 * {@link TestRule} that fails if not all active promises has been completed either succesfully or exceptionally.
 * Promises to be monitored should call either {@link TestUtils#assertFailure()} or {@link TestUtils#assertComplete()}
 */
public class ActivePromisesRule implements TestRule {
	@Override
	public Statement apply(Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				TestUtils.clearActivePromises();
				base.evaluate();
				assertEquals("Some promises has not been completed", 0, TestUtils.getActivePromises());
			}
		};
	}
}
