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

package io.datakernel.eventloop;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSuppliers;

public interface AsyncTcpSocket extends Cancellable {
	Stage<ByteBuf> read();

	Stage<Void> write(@Nullable ByteBuf buf);

	default SerialSupplier<ByteBuf> reader() {
		return SerialSuppliers.prefetch(SerialSupplier.of(this::read, this));
	}

	default SerialConsumer<ByteBuf> writer() {
		return SerialConsumer.of(this::write, this)
				.withAcknowledgement(ack -> ack
						.thenCompose($ -> write(null)));
	}
}
