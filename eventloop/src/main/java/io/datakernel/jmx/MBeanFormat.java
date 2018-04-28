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

package io.datakernel.jmx;

import io.datakernel.annotation.Nullable;
import io.datakernel.util.StringFormatUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;

import static java.lang.System.currentTimeMillis;

public final class MBeanFormat {
	private MBeanFormat() {
	}

	public static String formatExceptionLine(Throwable exception) {
		if (exception == null)
			return "";
		StringWriter stringWriter = new StringWriter();
		exception.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}

	public static String[] formatException(Throwable exception) {
		return formatExceptionLine(exception).split("\n");
	}

	public static String formatInstant(@Nullable Instant instant) {
		if (instant == null) return "null";
		Duration ago = Duration.between(instant, Instant.ofEpochMilli(currentTimeMillis())).withNanos(0);
		return StringFormatUtils.formatInstant(instant) +
				" (" + StringFormatUtils.formatDuration(ago) + " ago)";
	}

	public static String formatTimestamp(long timestamp) {
		return formatInstant(timestamp != 0L ? Instant.ofEpochMilli(timestamp) : null);
	}
}
