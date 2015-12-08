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

package io.datakernel.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.util.Preconditions.checkArgument;

public final class MemSize {
	public static final long KB = 1024;
	public static final long MB = 1024 * KB;
	public static final long GB = 1024 * MB;
	public static final long TB = 1024 * GB;

	private static final Pattern PATTERN = Pattern.compile("([0-9]+([\\.][0-9]+)?)\\s*(|K|M|G|T)B?", Pattern.CASE_INSENSITIVE);

	private final long bytes;

	private MemSize(long bytes) {
		checkArgument(bytes >= 0, "Bytes must be positive or zero");
		this.bytes = bytes;
	}

	public static MemSize of(long bytes) {
		checkArgument(bytes >= 0, "Bytes must be positive or zero");
		return new MemSize(bytes);
	}

	public long getBytes() {
		return bytes;
	}

	public static MemSize valueOf(String string) {
		Matcher matcher = PATTERN.matcher(string);
		if (!matcher.matches())
			throw new IllegalArgumentException("Illegal format: " + string);
		try {
			double value = Double.valueOf(matcher.group(1));
			String units = matcher.group(3).toLowerCase();

			long unit;
			switch (units) {
				case "":
					unit = 1;
					break;
				case "k":
					unit = KB;
					break;
				case "m":
					unit = MB;
					break;
				case "g":
					unit = GB;
					break;
				case "t":
					unit = TB;
					break;
				default:
					throw new IllegalArgumentException("Illegal units: " + string);
			}
			long bytes = (long) (value * unit);
			return new MemSize(bytes);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Illegal number format: " + string, e);
		}
	}

	public String format() {
		long bytes = getBytes();
		if (bytes == 0) {
			return "0b";
		}

		if ((bytes % TB) == 0) {
			return bytes / TB + "Tb";
		}

		if ((bytes % GB) == 0) {
			return bytes / GB + "Gb";
		}

		if ((bytes % MB) == 0) {
			return bytes / MB + "Mb";
		}

		if ((bytes % KB) == 0) {
			return bytes / KB + "Kb";
		}

		return bytes + "b";
	}

	@Override
	public String toString() {
		return format();
	}
}
