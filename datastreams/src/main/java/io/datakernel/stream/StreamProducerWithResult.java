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

package io.datakernel.stream;

import io.datakernel.async.SettableStage;

import java.util.concurrent.CompletionStage;

public interface StreamProducerWithResult<T, X> extends StreamProducer<T> {
	CompletionStage<X> getResult();

	static <T, X> StreamProducerWithResult<T, X> ofStage(CompletionStage<StreamProducerWithResult<T, X>> producerStage) {
		SettableStage<X> result = SettableStage.create();
		return new StreamProducerDecorator<T>() {
			{
				producerStage.whenCompleteAsync((producer1, throwable) -> {
					if (throwable == null) {
						setActualProducer(producer1);
						producer1.getResult().whenComplete(result::set);
					} else {
						setActualProducer(StreamProducer.closingWithError(throwable));
						result.setException(throwable);
					}
				});
			}
		}.withResult(result);
	}

}