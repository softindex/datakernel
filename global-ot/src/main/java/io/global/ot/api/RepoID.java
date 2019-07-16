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

package io.global.ot.api;

import io.datakernel.exception.ParseException;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;

public final class RepoID {
	private final PubKey owner;
	private final String name;

	private RepoID(PubKey owner, String name) {
		this.owner = owner;
		this.name = name;
	}

	public static RepoID of(PubKey owner, String name) {
		return new RepoID(owner, name);
	}

	public static RepoID of(PrivKey privKey, String name) {
		return new RepoID(privKey.computePubKey(), name);
	}

	public static RepoID of(KeyPair keys, String name) {
		return of(keys.getPubKey(), name);
	}

	public static RepoID fromString(String string) throws ParseException {
		String[] parts = string.split("/", 2);
		if (parts.length != 2) {
			throw new ParseException(PubKey.class, "No '/' delimiters in repo id string");
		}
		return new RepoID(PubKey.fromString(parts[0]), parts[1]);
	}

	public PubKey getOwner() {
		return owner;
	}

	public String getName() {
		return name;
	}

	public String asString() {
		return owner.asString() + '/' + name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RepoID that = (RepoID) o;
		return owner.equals(that.owner) && name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return 31 * owner.hashCode() + name.hashCode();
	}

	@Override
	public String toString() {
		return "RepoID{owner=" + owner + ", name='" + name + "'}";
	}
}
