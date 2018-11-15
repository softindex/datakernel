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

/**
 * {@link TestRule} that fails if not all active promises have been completed either succesfully or exceptionally.
 * Promises to be monitored should have either a {@link TestUtils#assertComplete()} or a {@link TestUtils#assertFailure()}
 * listener attached
 */
public final class ActivePromisesRule implements TestRule {
	@Override
	public Statement apply(Statement base, Description description) {
		return new LambdaStatement(() -> {
			TestUtils.clearActivePromises();
			base.evaluate();
			assert TestUtils.getActivePromises() == 0 : "Some promises have not been completed";
		});
	}
}
