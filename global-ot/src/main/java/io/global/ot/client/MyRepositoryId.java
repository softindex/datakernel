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

package io.global.ot.client;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.common.PrivKey;
import io.global.ot.api.RepoID;

import java.util.List;

public class MyRepositoryId<D> {
	private final RepoID repositoryId;
	private final PrivKey privKey;
	private final StructuredCodec<D> diffCodec;
	private final StructuredCodec<List<D>> diffsCodec;

	public MyRepositoryId(RepoID repositoryId, PrivKey privKey, StructuredCodec<D> diffCodec) {
		this.repositoryId = repositoryId;
		this.privKey = privKey;
		this.diffCodec = diffCodec;
		this.diffsCodec = StructuredCodecs.ofList(diffCodec);
	}

	public RepoID getRepositoryId() {
		return repositoryId;
	}

	public PrivKey getPrivKey() {
		return privKey;
	}

	public StructuredCodec<D> getDiffCodec() {
		return diffCodec;
	}

	public StructuredCodec<List<D>> getDiffsCodec() {
		return diffsCodec;
	}
}
