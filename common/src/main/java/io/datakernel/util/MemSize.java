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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.multiplyExact;

public final class MemSize {
	public static final long KB = 1024;
	public static final long MB = 1024 * KB;
	public static final long GB = 1024 * MB;
	public static final long TB = 1024 * GB;

	private static final Pattern PATTERN = Pattern.compile("(?<size>\\d+)([\\.](?<floating>\\d+))?\\s*(?<unit>(|g|m|k|t)b)?(\\s+|$)", Pattern.CASE_INSENSITIVE);

	private final long bytes;

	private MemSize(long bytes) {
		this.bytes = bytes;
	}

	public static MemSize of(long bytes) {
		return new MemSize(bytes);
	}

	public static MemSize bytes(long bytes) {
		return MemSize.of(bytes);
	}

	public static MemSize kilobytes(long kilobytes) {
		return of(kilobytes * KB);
	}

	public static MemSize megabytes(long megabytes) {
		return of(megabytes * MB);
	}

	public static MemSize gigabytes(long gigabytes) {
		return of(gigabytes * GB);
	}

	public static MemSize terabytes(long terabytes) {
		return of(terabytes * TB);
	}

	public long toLong() {
		return bytes;
	}

	public int toInt() {
		if (bytes > Integer.MAX_VALUE) throw new IllegalStateException("MemSize exceeds Integer.MAX_VALUE: " + bytes);
		return (int) bytes;
	}

	public static MemSize valueOf(String string) {
		Set<String> units = new HashSet<>();
		Matcher matcher = PATTERN.matcher(string.trim().toLowerCase());
		long result = 0;

		int lastEnd = 0;
		while (!matcher.hitEnd()) {
			if (!matcher.find() || matcher.start() != lastEnd) {
				throw new IllegalArgumentException("Invalid MemSize: " + string);
			}
			lastEnd = matcher.end();
			String unit = matcher.group("unit");

			if (unit == null) {
				unit = "";
			}

			if (!units.add(unit)) {
				throw new IllegalArgumentException("Memory unit " + unit + " occurs more than once in: " + string);
			}

			long memsize = Long.parseLong(matcher.group("size"));
			int floating = 0;
			int denominator = 1;
			String floatingPoint = matcher.group("floating");
			if (floatingPoint != null) {
				if (unit.equals("") || unit.equals("b")) {
					throw new IllegalArgumentException("MemSize unit bytes cannot be fractional");
				}
				floating = Integer.parseInt(floatingPoint);
				for (int i = 0; i < floatingPoint.length(); i++) {
					denominator *= 10;
				}
			}

			long temp;
			switch (unit) {
				case "tb":
					result += memsize * TB;
					temp = multiplyExact(floating, TB) / denominator;
					result += temp;
					break;
				case "gb":
					result += memsize * GB;
					temp = multiplyExact(floating, GB) / denominator;
					result += temp;
					break;
				case "mb":
					result += memsize * MB;
					temp = multiplyExact(floating, MB) / denominator;
					result += temp;
					break;
				case "kb":
					result += memsize * KB;
					temp = multiplyExact(floating, KB) / denominator;
					result += temp;
					break;
				case "b":
				case "":
					result += memsize;
					break;
			}
		}
		return MemSize.of(result);
	}

	private static String getUnit(long unit) {
		if (unit == TB) {
			return "Tb";
		} else {
			switch ((int) unit) {
				case (int) GB:
					return "Gb";
				case (int) MB:
					return "Mb";
				case (int) KB:
					return "Kb";
				case 1:
					return "b";
				default:
					throw new IllegalArgumentException("Wrong unit");
			}
		}
	}

	public String format() {
		long bytes = toLong();
		if (bytes == 0) {
			return "0b";
		}

		StringBuilder result = new StringBuilder();
		long divideResult, remainder, unit = TB;
		do {
			divideResult = bytes / unit;
			remainder = bytes % unit;

			if (divideResult != 0) {
				result.append(divideResult).append(getUnit(unit)).append(remainder == 0 ? "" : " ");
			}

			bytes -= divideResult * unit;
			unit /= 1024L;
		} while (remainder != 0);

		System.out.println("");
		return result.toString();
	}

	@Override
	public String toString() {
		return "" + toLong() + "b";
	}
}
