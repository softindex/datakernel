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

import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;

public final class GlobalFsPath {
	private final GlobalFsSpace space;
	private final String path;

	// region creators
	GlobalFsPath(GlobalFsSpace space, String path) {
		this.space = space;
		this.path = path;
	}

	public static GlobalFsPath of(GlobalFsSpace space, String path) {
		return new GlobalFsPath(space, path);
	}

	public static GlobalFsPath of(PubKey pubKey, String fsName, String path) {
		return new GlobalFsPath(GlobalFsSpace.of(pubKey, fsName), path);
	}

	public static GlobalFsPath of(KeyPair keys, String fsName, String path) {
		return of(keys.getPubKey(), fsName, path);
	}

	public static GlobalFsPath of(PrivKey key, String fsName, String path) {
		return of(key.computePubKey(), fsName, path);
	}
	// endregion

	public GlobalFsSpace getSpace() {
		return space;
	}

	public String getPath() {
		return path;
	}

	public PubKey getPubKey() {
		return space.getPubKey();
	}

	public String getFs() {
		return space.getFs();
	}

	public String getFullPath() {
		return space.getFs() + "::" + path;
	}

	@Override
	public int hashCode() {
		return 31 * space.hashCode() + path.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GlobalFsPath that = (GlobalFsPath) o;
		return space.equals(that.space) && path.equals(that.path);
	}

	@Override
	public String toString() {
		return "GlobalFsPath{space=" + space + ", path='" + path + "'}";
	}
}
