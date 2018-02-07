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

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class MBeanFormat {
	private static final char SPLITTER_LN = '\n';

	private MBeanFormat() {
	}

	public static ObjectName name(Class<?> type) {
		return name(type.getPackage().getName(), type.getSimpleName());
	}

	public static ObjectName name(String domain, String type, Class<?> valueType) {
		return name(domain, type, valueType.getSimpleName());
	}

	public static ObjectName name(String domain, String type, String value) {
		value = value.replace(':', '_').replace(',', '_').replace('=', '_');
		return name(domain + ":type=" + type + ",value=" + value);
	}

	public static ObjectName name(String domain, String type) {
		return name(domain + ":type=" + type);
	}

	private static ObjectName name(String name) {
		try {
			return new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		}
	}

	public static String formatExceptionLine(Throwable exception) {
		if (exception == null)
			return "";
		StringWriter stringWriter = new StringWriter();
		exception.printStackTrace(new PrintWriter(stringWriter));
		return stringWriter.toString();
	}

	public static String[] formatException(Throwable exception) {
		return formatMultilines(formatExceptionLine(exception));
	}

	private static String formatHours(long period) {
		long milliseconds = period % 1000;
		long seconds = (period / 1000) % 60;
		long minutes = (period / (60 * 1000)) % 60;
		long hours = period / (60 * 60 * 1000);
		return String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", seconds) + "." + String.format("%03d", milliseconds);
	}

	public static String formatDuration(long period) {
		if (period == 0)
			return "";
		return formatHours(period);
	}

	private static Function<Long, String> dateTimeFormatter = timestamp ->
			LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC)
					.format(DateTimeFormatter.ISO_DATE_TIME)
					.replace('T', ' ');

	public static void setTimestampFormat(Function<Long, String> dateTimeFormatter) {
		MBeanFormat.dateTimeFormatter = dateTimeFormatter;
	}

	public static String formatPeriodAgo(long timestamp) {
		if (timestamp == 0)
			return "";
		return dateTimeFormatter.apply(timestamp) +
				" (" + formatHours(System.currentTimeMillis() - timestamp) + " ago)";
	}

	public static String formatTimestamp(long timestamp) {
		if (timestamp == 0)
			return "";
		return dateTimeFormatter.apply(timestamp);
	}

	public static String[] formatMultilines(String s) {
		if (s == null || s.isEmpty())
			return null;

		return split(s);
	}

	private static String[] split(String s) {
		List<String> list = new ArrayList<>();
		int position = 0;

		int indexOfSplitter = s.indexOf(SPLITTER_LN, position);
		while (s.indexOf(SPLITTER_LN, position) != -1) {

			list.add(s.substring(position, indexOfSplitter));
			position = indexOfSplitter + 1;

			indexOfSplitter = s.indexOf(SPLITTER_LN, position);
		}
		if (position != s.length()) {
			list.add(s.substring(position, s.length()));
		}
		if (s.charAt(s.length() - 1) == SPLITTER_LN) {
			list.add("");
		}

		return list.toArray(new String[list.size()]);
	}
}
