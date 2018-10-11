/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.file;

import java.util.regex.Pattern;

public final class FileUtils {

	/** This pattern matches glob metacharacters. It can be used to test if glob matches a signle file, for escaping, and so on */
	public static final Pattern GLOB_META = Pattern.compile("(?<!\\\\)[*?{}\\[\\]]");
	//                                                       ^-------^^----------^ match any of chars *?{}[]
	//                                                            negative lookbehind - ensure that next char is not preceeded by \


	private FileUtils() {
		throw new AssertionError("nope.");
	}

	/**
	 * Escapes any glob metacharacters so that given path string can ever only match one file.
	 *
	 * @param path path that potentially can contain glob metachars
	 * @return escaped glob which matches only a file with that name
	 */
	public static String escapeGlob(String path) {
		return GLOB_META.matcher(path).replaceAll("\\\\$1");
	}

	/**
	 * Checks if given glob can match more than one file.
	 *
	 * @param glob the glob to check.
	 * @return <code>true</code> if given glob can match more than one file.
	 */
	public static boolean isWildcard(String glob) {
		return GLOB_META.matcher(glob).find();
	}
}
