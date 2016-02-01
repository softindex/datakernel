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

package io.datakernel.jmx.helper;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public final class CustomMatchers {

	private CustomMatchers() {}

	public static Matcher<ObjectName> objectname(final String name) {
		return new BaseMatcher<ObjectName>() {
			@Override
			public boolean matches(Object item) {
				try {
					return new ObjectName(name).equals((ObjectName) item);
				} catch (MalformedObjectNameException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("ObjectName: " + name);
			}
		};
	}
}
