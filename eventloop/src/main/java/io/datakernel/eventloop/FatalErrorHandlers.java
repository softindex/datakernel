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

package io.datakernel.eventloop;

import java.io.IOError;
import java.util.List;
import java.util.zip.ZipError;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class FatalErrorHandlers {
	private FatalErrorHandlers() {}

	public static FatalErrorHandler ignoreAllErrors() {
		return new FatalErrorHandler() {
			@Override
			public void handle(Throwable error, Object context) {

			}
		};
	}

	public static FatalErrorHandler exitOnAnyError() {
		return new FatalErrorHandler() {
			@Override
			public void handle(Throwable error, Object context) {
				shutdownForcibly();
			}
		};
	}

	public static FatalErrorHandler exitOnMatchedError(final List<Class> whiteList, final List<Class> blackList) {
		return new FatalErrorHandler() {
			@Override
			public void handle(Throwable error, Object context) {
				if (matchesAny(error.getClass(), whiteList) && !matchesAny(error.getClass(), blackList))
					shutdownForcibly();
			}

			@SuppressWarnings("unchecked")
			private boolean matchesAny(Class c, List<Class> list) {
				for (Class cl : list) {
					if (cl.isAssignableFrom(c)) return true;
				}
				return false;
			}
		};
	}

	public static FatalErrorHandler exitOnJvmError() {
		return exitOnMatchedError(singletonList((Class) Error.class),
				asList((Class) AssertionError.class, StackOverflowError.class, IOError.class, ZipError.class));
	}

	private static void shutdownForcibly() {
		Runtime.getRuntime().halt(1);
	}
}
