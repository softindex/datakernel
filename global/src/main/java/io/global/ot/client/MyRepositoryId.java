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

import io.datakernel.util.ParserFunction;
import io.global.common.PrivKey;
import io.global.ot.api.RepositoryName;

import java.util.List;
import java.util.function.Function;

public class MyRepositoryId<D> {
	private final RepositoryName repositoryId;
	private final PrivKey privKey;
	private final Function<List<D>, byte[]> diffsSerializer;
	private final ParserFunction<byte[], List<D>> diffsDeserializer;

	public MyRepositoryId(RepositoryName repositoryId, PrivKey privKey,
			Function<List<D>, byte[]> diffsSerializer, ParserFunction<byte[], List<D>> diffsDeserializer) {
		this.repositoryId = repositoryId;
		this.privKey = privKey;
		this.diffsSerializer = diffsSerializer;
		this.diffsDeserializer = diffsDeserializer;
	}

	public RepositoryName getRepositoryId() {
		return repositoryId;
	}

	public PrivKey getPrivKey() {
		return privKey;
	}

	public Function<List<D>, byte[]> getDiffsSerializer() {
		return diffsSerializer;
	}

	public ParserFunction<byte[], List<D>> getDiffsDeserializer() {
		return diffsDeserializer;
	}
}
