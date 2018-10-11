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

public final class GlobalFsSpace {
	private final PubKey pubKey;
	private final String fsName;

	// region creators
	private GlobalFsSpace(PubKey pubKey, String fsName) {
		this.pubKey = pubKey;
		this.fsName = fsName;
	}

	public static GlobalFsSpace of(PubKey pubKey, String fsName) {
		return new GlobalFsSpace(pubKey, fsName);
	}

	public static GlobalFsSpace of(KeyPair keys, String fsName) {
		return of(keys.getPubKey(), fsName);
	}

	public static GlobalFsSpace of(PrivKey key, String fsName) {
		return of(key.computePubKey(), fsName);
	}
	// endregion

	public GlobalFsPath pathFor(String file) {
		return new GlobalFsPath(this, file);
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public String getFs() {
		return fsName;
	}

	@Override
	public int hashCode() {
		return 31 * pubKey.hashCode() + fsName.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GlobalFsSpace name = (GlobalFsSpace) o;
		return pubKey.equals(name.pubKey) && fsName.equals(name.fsName);
	}

	@Override
	public String toString() {
		return "GlobalFsSpace{pubKey=" + pubKey + ", fsName='" + fsName + "'}";
	}
}
