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

import org.jetbrains.annotations.Nullable;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * {@link TestRule} that enables deeper logger levels for specific tests that request it.
 */
public final class LoggingRule implements TestRule {

	private interface AnnotationExtractor {
		<A extends Annotation> @Nullable A get(Class<A> annotation);
	}

	private static List<LoggerConfig> getAnnotations(AnnotationExtractor fn) {
		LoggerConfig single = fn.get(LoggerConfig.class);
		if (single == null) {
			LoggerConfig.Container container = fn.get(LoggerConfig.Container.class);
			if (container == null) {
				return emptyList();
			}
			return asList(container.value());
		} else {
			return singletonList(single);
		}
	}

	@Override
	public Statement apply(Statement base, Description description) {
		List<LoggerConfig> clauses = new ArrayList<>();
		description.getTestClass().getAnnotation(LoggerConfig.class);
		clauses.addAll(getAnnotations(description.getTestClass()::getAnnotation));
		clauses.addAll(getAnnotations(description::getAnnotation));
		return new LambdaStatement(() -> {
			Logger rootLogger = LogManager.getLogManager().getLogger("");
			Handler[] oldHandlers = rootLogger.getHandlers();
			for (Handler handler : oldHandlers) {
				rootLogger.removeHandler(handler);
			}
			Level[] oldLevels = new Level[clauses.size()];
			Logger[] loggers = new Logger[clauses.size()];
			rootLogger.setLevel(Level.ALL);
			for (int i = 0; i < clauses.size(); i++) {
				LoggerConfig clause = clauses.get(i);
				Logger logger = Logger.getLogger(
						clause.logger() != Void.class ?
								clause.logger().getName() :
								clause.packageOf() != Void.class ?
										clause.packageOf().getPackage().getName() :
										"");
				oldLevels[i] = logger.getLevel();
				loggers[i] = logger;
				logger.setLevel(clause.value().getLevel());
			}
			rootLogger.addHandler(new SLF4JBridgeHandler());

			try {
				base.evaluate();
			} finally {
				for (Handler handler : rootLogger.getHandlers()) {
					rootLogger.removeHandler(handler);
				}
				for (int i = 0; i < loggers.length; i++) {
					loggers[i].setLevel(oldLevels[i]);
				}
				for (Handler handler : oldHandlers) {
					rootLogger.addHandler(handler);
				}
			}
		});
	}

}
