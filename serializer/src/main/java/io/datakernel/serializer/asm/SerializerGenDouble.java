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

package io.datakernel.serializer.asm;

import org.objectweb.asm.MethodVisitor;

public final class SerializerGenDouble extends SerializerGenPrimitive {

	public SerializerGenDouble() {
		super(double.class);
	}

	@Override
	protected void doSerialize(MethodVisitor mv, SerializerBackend backend) {
		backend.writeDoubleGen(mv);
	}

	@Override
	protected void doDeserialize(MethodVisitor mv, SerializerBackend backend) {
		backend.readDoubleGen(mv);
	}
}

