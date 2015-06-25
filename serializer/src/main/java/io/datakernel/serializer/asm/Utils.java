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

import io.datakernel.serializer.asm.SerializerGen.VersionsCollector;
import org.objectweb.asm.MethodVisitor;

import java.util.Collections;
import java.util.Set;

import static com.google.common.primitives.Primitives.wrap;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

public final class Utils {
	private Utils() {
	}

	public static Class<?> castSourceType(MethodVisitor mv, Class<?> actualSourceType, Class<?> requiredSourceType) {
		if (requiredSourceType.isAssignableFrom(actualSourceType)) {
			return actualSourceType;
		}
		if (actualSourceType.isAssignableFrom(requiredSourceType)) {
			mv.visitTypeInsn(CHECKCAST, getInternalName(requiredSourceType));
			return requiredSourceType;
		} else
			throw new IllegalArgumentException("Unrelated type '" + actualSourceType + "'");
	}

	public static void unbox(MethodVisitor mv, Class<?> primitiveType) {
		Class<?> boxedType = wrap(primitiveType);
		mv.visitMethodInsn(INVOKEVIRTUAL,
				getType(boxedType).getInternalName(),
				primitiveType + "Value",
				getMethodType(getType(primitiveType)).getDescriptor());
	}

	public static void box(MethodVisitor mv, Class<?> primitiveType) {
		Class<?> boxedType = wrap(primitiveType);
		mv.visitMethodInsn(INVOKESTATIC, getType(boxedType).getInternalName(),
				"valueOf",
				getMethodType(getType(boxedType), getType(primitiveType)).getDescriptor());
	}

	public static int getMaxSupportedVersion(SerializerGen serializerGen) {
		Set<Integer> versions = VersionsCollector.versions(serializerGen);
		if (versions.isEmpty())
			return 0;
		return Collections.max(versions);
	}

}
