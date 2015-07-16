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

package io.datakernel.codegen.utils;

import com.google.common.collect.Maps;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getDescriptor;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

public final class StaticValueHolder {
	private static int count = 0;
	private final static Map<Integer, Object> registry = Maps.newHashMap();

	private static final AtomicInteger COUNTER = new AtomicInteger();

	public static final String VALUE_FIELD = "value";

	public synchronized static int add(Object object) {
		registry.put(count, object);
		return count++;
	}

	public synchronized static Object get(int key) {
		return registry.remove(key);
	}

	public static Class<?> createValueHolderClass(DefiningClassLoader classLoader, Object value) {
		DefiningClassWriter cw = new DefiningClassWriter(classLoader);

		String className = "asm.Holder" + COUNTER.incrementAndGet();
		Type classType = getType('L' + className.replace('.', '/') + ';');

		cw.visit(V1_1, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
				classType.getInternalName(),
				null,
				"java/lang/Object",
				null);

		{
			GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC,
					getMethod("void <init> ()"), null, null, cw);
			g.loadThis();
			g.invokeConstructor(getType(Object.class), getMethod("void <init> ()"));
			g.returnValue();
			g.endMethod();
		}

		{
			FieldVisitor fv = cw.visitField(ACC_PUBLIC + ACC_STATIC + ACC_FINAL,
					VALUE_FIELD, getDescriptor(value.getClass()),
					null, null);
			fv.visitEnd();
		}

		{
			GeneratorAdapter g = new GeneratorAdapter(ACC_PRIVATE + ACC_STATIC,
					getMethod("void <clinit> ()"), null, null, cw);
			g.push(StaticValueHolder.add(value));
			g.invokeStatic(getType(StaticValueHolder.class), getMethod("Object get(int)"));
			g.checkCast(getType(value.getClass()));
			g.putStatic(classType, VALUE_FIELD, getType(value.getClass()));
			g.returnValue();
			g.endMethod();
		}

		cw.visitEnd();

		return classLoader.defineClass(className, cw.toByteArray());
	}

	public static void pushStaticValue(DefiningClassLoader classLoader, GeneratorAdapter g, Object value) {
		g.getStatic(getType(createValueHolderClass(classLoader, value)), VALUE_FIELD, getType(value.getClass()));
	}

}
