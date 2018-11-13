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

public final class RawPullRequest {
	public final RepoID repository;
	public final RepoID forkRepository;

	private RawPullRequest(RepoID repository, RepoID forkRepository) {
		this.repository = repository;
		this.forkRepository = forkRepository;
	}

	public static RawPullRequest of(RepoID repository, RepoID forkRepository) {
		return new RawPullRequest(repository, forkRepository);
	}

	public static RawPullRequest parse(RepoID repository, RepoID forkRepository) throws ParseException {
		return new RawPullRequest(repository, forkRepository);
	}

	public RepoID getRepository() {
		return repository;
	}

	public RepoID getForkRepository() {
		return forkRepository;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawPullRequest that = (RawPullRequest) o;
		if (!repository.equals(that.repository)) return false;
		return forkRepository.equals(that.forkRepository);
	}

	@Override
	public int hashCode() {
		int result = repository.hashCode();
		result = 31 * result + forkRepository.hashCode();
		return result;
	}
}
