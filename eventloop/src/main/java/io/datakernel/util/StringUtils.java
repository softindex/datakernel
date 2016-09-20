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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class StringUtils {

	private StringUtils() {}

	public static String join(String separator, Iterable<?> inputs) {
		checkNotNull(separator);
		checkNotNull(inputs);

		StringBuilder stringBuilder = new StringBuilder();
		Iterator<?> iterator = inputs.iterator();

		if (iterator.hasNext()) {
			Object firstElement = checkNotNull(iterator.next(), "Elements cannot be null");
			String firstString = checkNotNull(firstElement.toString(), "element.toString() cannot be null");
			stringBuilder.append(firstString);
		} else {
			return "";
		}

		while (iterator.hasNext()) {
			Object currentElement = checkNotNull(iterator.next(), "Elements cannot be null");
			String currentString = checkNotNull(currentElement.toString(), "element.toString() cannot be null");
			stringBuilder.append(separator);
			stringBuilder.append(currentString);
		}

		return stringBuilder.toString();
	}

	public static String join(String separator, Object[] inputs) {
		return join(separator, Arrays.asList(inputs));
	}

	public static String join(char separator, Iterable<?> inputs) {
		return join(Character.toString(separator), inputs);
	}

	public static String join(char separator, Object[] inputs) {
		return join(Character.toString(separator), inputs);
	}

	public static List<String> splitToList(CharSequence separators, String input) {
		List<String> substrings = new ArrayList<>();

		int currentSubstringBeginningIndex = -1;  // -1 means that we are looking for substring beginning now

		for (int currentIndex = 0; currentIndex < input.length(); currentIndex++) {
			boolean isCurrentCharSeparator = charSequenceContainsChar(separators, input.charAt(currentIndex));

			if (currentSubstringBeginningIndex == -1 && !isCurrentCharSeparator) {
				currentSubstringBeginningIndex = currentIndex;
			} else if (currentSubstringBeginningIndex != -1 && isCurrentCharSeparator) {
				substrings.add(input.substring(currentSubstringBeginningIndex, currentIndex));
				currentSubstringBeginningIndex = -1;
			} else {
				// skip
			}
		}

		// add last substring
		if (currentSubstringBeginningIndex != -1) {
			substrings.add(input.substring(currentSubstringBeginningIndex, input.length()));
		}

		return substrings;
	}

	private static boolean charSequenceContainsChar(CharSequence sequence, char c) {
		for (int i = 0; i < sequence.length(); i++) {
			if (sequence.charAt(i) == c) {
				return true;
			}
		}
		return false;
	}

	public static List<String> splitToList(char separator, String input) {
		return splitToList(Character.toString(separator), input);
	}

	public static String[] split(CharSequence separators, String input) {
		List<String> output = splitToList(separators, input);
		return output.toArray(new String[output.size()]);
	}

	public static String[] split(char separator, String input) {
		return split(Character.toString(separator), input);
	}
}
