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

package io.datakernel.test.rules;

import io.datakernel.logger.LoggerFactory;
import io.datakernel.test.TestUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.logging.Logger;

/**
 * {@link TestRule} that fails if not all active promises have been completed either succesfully or exceptionally.
 * Promises to be monitored should have either a {@link TestUtils#assertComplete()}
 * listener attached
 */
public final class ActivePromisesRule implements TestRule {
	private static final Logger logger = LoggerFactory.getLogger(ActivePromisesRule.class.getName());

	@Override
	public Statement apply(Statement base, Description description) {
		return new LambdaStatement(() -> {
			TestUtils.clearActivePromises();
			try {
				base.evaluate();
			} catch (Throwable t) {
				int n = TestUtils.getActivePromises();
				if (n != 0) {
					logger.info(n + " promise assertion" + (n == 1 ? " was" : "s were") + " not checked");
				}
				throw t;
			}
			assert TestUtils.getActivePromises() == 0 : "Some promises have not been completed";
		});
	}
}
