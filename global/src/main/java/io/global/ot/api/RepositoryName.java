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

import io.global.common.PubKey;

public final class RepositoryName {
	private final PubKey pubKey;
	private final String repositoryName;

	public RepositoryName(PubKey pubKey, String repositoryName) {
		this.pubKey = pubKey;
		this.repositoryName = repositoryName;
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public String getRepositoryName() {
		return repositoryName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RepositoryName that = (RepositoryName) o;
		if (!pubKey.equals(that.pubKey)) return false;
		return repositoryName.equals(that.repositoryName);
	}

	@Override
	public int hashCode() {
		int result = pubKey.hashCode();
		result = 31 * result + repositoryName.hashCode();
		return result;
	}
}
