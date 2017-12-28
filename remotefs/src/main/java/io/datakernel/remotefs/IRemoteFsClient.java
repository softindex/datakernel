/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.remotefs;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.StreamProducers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface IRemoteFsClient {
	CompletionStage<StreamConsumerWithResult<ByteBuf, Void>> upload(String fileName);

	default StreamConsumerWithResult<ByteBuf, Void> uploadStream(String fileName) {
		return StreamConsumers.ofStageWithResult(upload(fileName));
	}

	CompletionStage<StreamProducerWithResult<ByteBuf, Void>> download(String fileName, long startPosition);

	default StreamProducerWithResult<ByteBuf, Void> downloadStream(String fileName, long startPosition) {
		return StreamProducers.ofStageWithResult(download(fileName, startPosition));
	}

	CompletionStage<Void> move(Map<String, String> changes);

	CompletionStage<Void> delete(String fileName);

	CompletionStage<List<String>> list();
}
