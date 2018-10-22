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
import io.global.common.RepoID;

public final class GlobalPath {
	private final PubKey owner;
	private final String fs;
	private final String path;

	// region creators
	GlobalPath(PubKey owner, String fs, String path) {
		this.owner = owner;
		this.fs = fs;
		this.path = path;
	}

	public static GlobalPath of(RepoID ns, String path) {
		return new GlobalPath(ns.getOwner(), ns.getName(), path);
	}

	public static GlobalPath of(PubKey owner, String fs, String path) {
		return new GlobalPath(owner, fs, path);
	}

	public static GlobalPath of(KeyPair keys, String fs, String path) {
		return of(keys.getPubKey(), fs, path);
	}

	public static GlobalPath of(PrivKey key, String fs, String path) {
		return of(key.computePubKey(), fs, path);
	}
	// endregion

	public RepoID toRepoID() {
		return RepoID.of(owner, fs);
	}

	public LocalPath toLocalPath() {
		return LocalPath.of(fs, path);
	}

	public PubKey getOwner() {
		return owner;
	}

	public String getFs() {
		return fs;
	}

	public String getPath() {
		return path;
	}

	@Override
	public int hashCode() {
		return 31 * (31 * owner.hashCode() + fs.hashCode()) + path.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		GlobalPath that = (GlobalPath) o;

		return owner.equals(that.owner) && fs.equals(that.fs) && path.equals(that.path);
	}

	@Override
	public String toString() {
		return "GlobalPath{owner=" + owner + ", fs='" + fs + '\'' + ", path='" + path + '\'' + '}';
	}
}
