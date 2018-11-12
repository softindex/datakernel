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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.LoggerFactory;

/**
 * {@link TestRule} that enables deeper logger levels for specific tests that request it.
 */
public final class LoggingRule implements TestRule {
	@Override
	public Statement apply(Statement base, Description description) {
		Enable info = description.getAnnotation(Enable.class);
		if (info == null) {
			return base;
		}
		return new LambdaStatement(() -> {
			Logger logger = (Logger) LoggerFactory.getLogger(info.value());
			Level oldLevel = logger.getLevel();
			logger.setLevel(Level.toLevel(info.level()));
			try {
				base.evaluate();
			} finally {
				logger.setLevel(oldLevel);
			}
		});
	}

	public @interface Enable {
		String value() default Logger.ROOT_LOGGER_NAME;

		String level() default "TRACE";
	}
}
