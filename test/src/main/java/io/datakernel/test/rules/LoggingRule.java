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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link TestRule} that enables deeper logger levels for specific tests that request it.
 */
public final class LoggingRule implements TestRule {
	@Override
	public Statement apply(Statement base, Description description) {
		LoggerConfig[] clauses;
		LoggerConfig single = description.getAnnotation(LoggerConfig.class);
		if (single == null) {
			LoggerConfig.Container container = description.getAnnotation(LoggerConfig.Container.class);
			if (container == null) {
				return base;
			}
			clauses = container.value();
		} else {
			clauses = new LoggerConfig[]{single};
		}
		return new LambdaStatement(() -> {
			Level[] oldLevels = new Level[clauses.length];
			Logger[] loggers = new Logger[clauses.length];
			for (int i = 0; i < clauses.length; i++) {
				LoggerConfig clause = clauses[i];
				Logger logger = Logger.getLogger(clause.logger());
				oldLevels[i] = logger.getLevel();
				loggers[i] = logger;
				logger.setLevel(Level.parse(clause.value()));
			}
			try {
				base.evaluate();
			} finally {
				for (int i = 0; i < loggers.length; i++) {
					loggers[i].setLevel(oldLevels[i]);
				}
			}
		});
	}

	@Repeatable(LoggerConfig.Container.class)
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.TYPE})
	public @interface LoggerConfig {
		String logger() default Logger.GLOBAL_LOGGER_NAME;

		String value();

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ElementType.METHOD, ElementType.TYPE})
		@interface Container {
			LoggerConfig[] value();
		}
	}
}
