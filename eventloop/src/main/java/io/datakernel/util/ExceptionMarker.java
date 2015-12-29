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

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Arrays;

public final class ExceptionMarker {
	private final Class<?> clazz;
	private final Marker marker;

	public ExceptionMarker(Class<?> clazz, String name) {
		this.clazz = clazz;
		this.marker = MarkerFactory.getMarker(name);
	}

	public Marker getMarker() {
		return marker;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ExceptionMarker that = (ExceptionMarker) o;
		return equal(this.clazz, that.clazz) &&
				equal(this.marker, that.marker);
	}

	private static boolean equal(Object a, Object b) {
		return a == b || (a != null && a.equals(b));
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(new Object[]{clazz, marker});
	}

	@Override
	public String toString() {
		return clazz.getName() + "." + marker.getName();
	}
}