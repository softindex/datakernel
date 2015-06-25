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

public final class RateCounter {
	private int numerator;
	private int denominator;

	public void reset() {
		numerator = 0;
		denominator = 0;
	}

	public void incNumerator() {
		++numerator;
	}

	public void incDenominator() {
		++denominator;
	}

	public int getNumerator() {
		return numerator;
	}

	public int getDenominator() {
		return denominator;
	}

	public float getPercent() {
		int d = denominator;
		if (d <= 0)
			return 0;
		return ((float) numerator) / d * 100;
	}

	@Override
	public String toString() {
		return String.format("%.2f%% (%d of %d)", getPercent(), numerator, denominator);
	}
}
