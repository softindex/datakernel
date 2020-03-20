/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for determining whether {@link Preconditions} should be enabled or not.
 * It is sometimes useful to disable preconditions checks at runtime environment
 * to squeeze out more performance in a tight loop.
 * <p>
 * The common pattern is having a
 * <pre>
 * {@code private static final boolean CHECK = Check.isEnabled(MyClass.class);}
 * </pre>
 * constant in your class. When using {@link Preconditions} just wrap the call in if-statement like so:
 * <pre>
 * {@code if (CHECK) Preconditions.checkNotNull(value);}
 * </pre>
 * <p>
 * By default, all checks are <b>enabled</b> (unlike java asserts).
 * To disable all checks you can run application with system property {@code -Dchk=off}
 * You can enable or disable checks for the whole package (with its subpackages) or for individual classes
 * like so: {@code -Dchk:io.datakernel.eventloop=on -Dchk:io.datakernel.promise.Promise=off}.
 */
public final class Check {
	private static final Logger logger = LoggerFactory.getLogger(Check.class);

	private static final boolean ENABLED_BY_DEFAULT;
	private static final String ENV_PREFIX = "chk:";

	static {
		String enabled = System.getProperty("chk");

		if (enabled == null || enabled.equals("on")) ENABLED_BY_DEFAULT = true;
		else if (enabled.equals("off")) ENABLED_BY_DEFAULT = false;
		else throw new RuntimeException(getErrorMessage(enabled));

		logger.trace("All checks are " + (ENABLED_BY_DEFAULT ? "enabled" : "disabled") + " by default");
	}

	/**
	 * Indicates whether checks are enabled or disabled for the specified class
	 *
	 * @param cls class to be checked
	 * @return {@code true} if checks are enabled for the given class, {@code false} otherwise
	 */
	public static boolean isEnabled(Class<?> cls) {
		String property;
		String path = cls.getName();
		if ((property = System.getProperty(ENV_PREFIX + path)) == null) {
			property = System.getProperty(ENV_PREFIX + cls.getSimpleName());
			while (property == null) {
				int idx = path.lastIndexOf(".");
				if (idx == -1) break;
				path = path.substring(0, idx);
				property = System.getProperty(ENV_PREFIX + path);
			}
		}

		boolean enabled = ENABLED_BY_DEFAULT;
		if (property != null) {
			if (property.equals("on")) enabled = true;
			else if (property.equals("off")) enabled = false;
			else throw new RuntimeException(getErrorMessage(property));
		}
		logger.trace("Check is " + (enabled ? "ON" : "OFF") + " for class {}", cls);
		return enabled;
	}

	private static String getErrorMessage(String value) {
		return "Only 'on' and 'off' values are allowed for 'chk' system properties, was '" + value + '\'';
	}
}
