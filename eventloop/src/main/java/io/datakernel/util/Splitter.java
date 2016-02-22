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
import java.util.Collections;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class Splitter {
	private final CharSequenceWrapper separators;
	private boolean shallTrim;
	private boolean omitEmptyStrings;

	private Splitter(CharSequence separators) {
		checkArgument(separators != null && separators.length() > 0);
		this.separators = new CharSequenceWrapper(separators);
	}

	public static Splitter on(char separator) {
		return new Splitter(Character.toString(separator));
	}

	public static Splitter onAnyOf(CharSequence separators) {
		return new Splitter(separators);
	}

	// TODO (vmykhalko): replace with static methods with appropriate parameters and move to Utils class
	public String[] split(String input) {
		// TODO (vmykhalko): inefficient implementation, make sure no garbage is created at all
		List<String> list = splitToList(input);
		return list.toArray(new String[list.size()]);
	}

	public List<String> splitToList(String input) {
		if (input.isEmpty()) {
			return Collections.emptyList();
		}
		List<Integer> separatorsPositions = new ArrayList<>(input.length());
		for (int i = 0; i < input.length(); i++) {
			char currentChar = input.charAt(i);
			if (separators.contains(currentChar)) {
				separatorsPositions.add(i);
			}
		}



		List<String> splitSubStrings = new ArrayList<>();
		int currentSubStringStart = 0;
		for (Integer separatorsPosition : separatorsPositions) {
			int currentSeparatorPosition = separatorsPosition;
			String result = input.substring(currentSubStringStart, currentSeparatorPosition);
			if (shallTrim) {
				result = result.trim();
			}
			if (!omitEmptyStrings || !result.isEmpty()) {
				splitSubStrings.add(result);
			}
			currentSubStringStart = currentSeparatorPosition + 1;
		}
		String last = input.substring(currentSubStringStart, input.length());
		if (shallTrim) {
			last = last.trim();
		}
		if (!omitEmptyStrings || !last.isEmpty()) {
			splitSubStrings.add(last);
		}
		return splitSubStrings;
	}

	public Splitter trimResults() {
		this.shallTrim = true;
		return this;
	}

	public Splitter omitEmptyStrings() {
		this.omitEmptyStrings = true;
		return this;
	}

	private static final class CharSequenceWrapper {
		private final CharSequence sequence;

		public CharSequenceWrapper(CharSequence sequence) {
			this.sequence = checkNotNull(sequence);
		}

		public boolean contains(char c) {
			for (int i = 0; i < sequence.length(); i++) {
				if (sequence.charAt(i) == c) {
					return true;
				}
			}
			return false;
		}
	}
}
