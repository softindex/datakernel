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

package io.global.db.api;

import io.datakernel.exception.ParseException;
import io.global.common.KeyPair;
import io.global.common.PubKey;

public final class TableID {
	private final PubKey space;
	private final String name;

	private TableID(PubKey space, String name) {
		this.space = space;
		this.name = name;
	}

	public static TableID of(PubKey space, String name) {
		return new TableID(space, name);
	}

	public static TableID of(KeyPair keys, String name) {
		return of(keys.getPubKey(), name);
	}

	public static TableID fromString(String string) throws ParseException {
		String[] parts = string.split("/", 2);
		if (parts.length != 2) {
			throw new ParseException(PubKey.class, "No '/' delimiters in repo id string");
		}
		return new TableID(PubKey.fromString(parts[0]), parts[1]);
	}

	public PubKey getSpace() {
		return space;
	}

	public String getName() {
		return name;
	}

	public String asString() {
		return space.asString() + '/' + name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TableID that = (TableID) o;
		return space.equals(that.space) && name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return 31 * space.hashCode() + name.hashCode();
	}

	@Override
	public String toString() {
		return "TableID{space=" + space + ", name='" + name + "'}";
	}
}
