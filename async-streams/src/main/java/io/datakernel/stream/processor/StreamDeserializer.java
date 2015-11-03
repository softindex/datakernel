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

package io.datakernel.stream.processor;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamDataReceiver;

/**
 * Represent deserializer which deserializes data from ByteBuffer to some type. Is a {@link AbstractStreamConsumer} which
 * receives ByteBuffers and {@link AbstractStreamProducer} which streams specified type.
 *
 * @param <T> original type of data
 */
public interface StreamDeserializer<T> extends StreamTransformer<ByteBuf, T> {
	void drainBuffersTo(StreamDataReceiver<ByteBuf> dataReceiver);
}
