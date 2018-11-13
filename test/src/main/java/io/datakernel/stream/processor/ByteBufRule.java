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

import io.datakernel.bytebuf.ByteBufPool;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.Assert.assertEquals;

/**
 * {@link TestRule} that fails if not all byte buffers requested from the {@link ByteBufPool} were recycled properly.
 * <p>
 * Annotation {@link IgnoreLeaks} can be put on a test that wants this rule disabled.
 */
public final class ByteBufRule implements TestRule {
	static {
		System.setProperty("ByteBufPool.minSize", "1");
		System.setProperty("ByteBufPool.maxSize", "0");
	}

	@Override
	public Statement apply(Statement base, Description description) {
		return new LambdaStatement(() -> {
			ByteBufPool.clear();
			base.evaluate();
			if (description.getAnnotation(IgnoreLeaks.class) == null) {
				assertEquals(ByteBufPool.getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
			}
		});
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	public @interface IgnoreLeaks {

		/**
		 * An optional description for why the test needs to ignore leaks
		 */
		String value() default "";
	}
}
