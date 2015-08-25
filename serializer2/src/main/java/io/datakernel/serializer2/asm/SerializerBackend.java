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

package io.datakernel.serializer2.asm;

import org.objectweb.asm.MethodVisitor;

public interface SerializerBackend {
	void writeBytesGen(MethodVisitor mv);

	void writeByteGen(MethodVisitor mv);

	void writeShortGen(MethodVisitor mv);

	void writeIntGen(MethodVisitor mv);

	void writeVarIntGen(MethodVisitor mv);

	void writeLongGen(MethodVisitor mv);

	void writeVarLongGen(MethodVisitor mv);

	void writeFloatGen(MethodVisitor mv);

	void writeDoubleGen(MethodVisitor mv);

	void writeCharGen(MethodVisitor mv);

	void writeUTF8Gen(MethodVisitor mv);

	void writeNullableUTF8Gen(MethodVisitor mv);

	void writeUTF16Gen(MethodVisitor mv);

	void writeNullableUTF16Gen(MethodVisitor mv);

	void writeAscii(MethodVisitor mv);

	void writeNullableAscii(MethodVisitor mv);

	void readBytesGen(MethodVisitor mv);

	void readByteGen(MethodVisitor mv);

	void readShortGen(MethodVisitor mv);

	void readIntGen(MethodVisitor mv);

	void readVarIntGen(MethodVisitor mv);

	void readLongGen(MethodVisitor mv);

	void readVarLongGen(MethodVisitor mv);

	void readFloatGen(MethodVisitor mv);

	void readDoubleGen(MethodVisitor mv);

	void readCharGen(MethodVisitor mv);

	void writeBooleanGen(MethodVisitor mv);

	void readBooleanGen(MethodVisitor mv);

	void readUTF8Gen(MethodVisitor mv);

	void readNullableUTF8Gen(MethodVisitor mv);

	void readUTF16Gen(MethodVisitor mv);

	void readNullableUTF16Gen(MethodVisitor mv);

	void readAscii(MethodVisitor mv);

	void readNullableAscii(MethodVisitor mv);
}
