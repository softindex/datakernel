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

package io.global.fs.api;

import io.global.common.Hash;
import io.global.common.PubKey;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class LocalPath {
	private final String fs;
	private final String path;

	// region creators
	LocalPath(String fs, String path) {
		this.fs = fs;
		this.path = path;
	}

	public static LocalPath of(String fs, String path) {
		return new LocalPath(fs, path);
	}
	// endregion

	public GlobalPath complete(PubKey owner) {
		return new GlobalPath(owner, fs, path);
	}

	public String getFs() {
		return fs;
	}

	public String getPath() {
		return path;
	}

	public Hash hash() {
		// NUL is not allowed un Unix filenames so it is a good
		// unambiguous delimiter to hash these two values together
		return Hash.of((fs + '\0' + path).getBytes(UTF_8));
	}

	@Override
	public int hashCode() {
		return 31 * fs.hashCode() + path.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LocalPath localPath = (LocalPath) o;

		return fs.equals(localPath.fs) && path.equals(localPath.path);
	}

	@Override
	public String toString() {
		return "LocalPath{fs='" + fs + "', path='" + path + "'}";
	}
}
