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

package io.datakernel.serializer;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.datakernel.serializer.asm.SerializerBackend;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.utils.DefiningClassLoader;
import io.datakernel.serializer.utils.DefiningClassWriter;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.*;
import static io.datakernel.serializer.asm.Utils.castSourceType;
import static java.lang.ClassLoader.getSystemClassLoader;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;

/**
 * Contains static factory methods for various serializers.
 */
@SuppressWarnings("unchecked")
public class SerializerFactory {
	public static final String ASM_SERIALIZER_LIBRARY = "asm.SerializerLibrary";
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private final ClassLoader rootClassLoader;
	private final DefiningClassLoader libraryClassLoader;
	private final Class<?> serializerClass;
	private final Class<?> outputClass;
	private final Class<?> inputClass;
	private final String outputTypeName;
	private final String inputTypeName;
	private final int invokeOpCode;
	private final String[] outputExceptions;
	private final String[] inputExceptions;

	private final boolean serialize;
	private final boolean deserialize;

	private final SerializerBackend backend = new SerializerBackend() {
		@Override
		public void writeBytesGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "write", "([BII)V");
		}

		@Override
		public void writeByteGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeByte", "(B)V");
		}

		@Override
		public void writeShortGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeShort", "(S)V");
		}

		@Override
		public void writeIntGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeInt", "(I)V");
		}

		@Override
		public void writeVarIntGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeVarInt", "(I)V");
		}

		@Override
		public void writeLongGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeLong", "(J)V");
		}

		@Override
		public void writeVarLongGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeVarLong", "(J)V");
		}

		@Override
		public void writeFloatGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeFloat", "(F)V");
		}

		@Override
		public void writeDoubleGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeDouble", "(D)V");
		}

		@Override
		public void writeBooleanGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeBoolean", "(Z)V");
		}

		@Override
		public void writeCharGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeChar", "(C)V");
		}

		@Override
		public void writeUTF8Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeUTF8", "(Ljava/lang/String;)V");
		}

		@Override
		public void writeNullableUTF8Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeNullableUTF8", "(Ljava/lang/String;)V");
		}

		@Override
		public void writeUTF16Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeUTF16", "(Ljava/lang/String;)V");
		}

		@Override
		public void writeNullableUTF16Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, outputTypeName, "writeNullableUTF16", "(Ljava/lang/String;)V");
		}

		@Override
		public void readBytesGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "read", "([BII)I");
			mv.visitInsn(POP);
		}

		@Override
		public void readByteGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readByte", "()B");
		}

		@Override
		public void readShortGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readShort", "()S");
		}

		@Override
		public void readIntGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readInt", "()I");
		}

		@Override
		public void readVarIntGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readVarInt", "()I");
		}

		@Override
		public void readLongGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readLong", "()J");
		}

		@Override
		public void readVarLongGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readVarLong", "()J");
		}

		@Override
		public void readFloatGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readFloat", "()F");
		}

		@Override
		public void readDoubleGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readDouble", "()D");
		}

		@Override
		public void readBooleanGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readBoolean", "()Z");
		}

		@Override
		public void readCharGen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readChar", "()C");
		}

		@Override
		public void readUTF8Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readUTF8", "()Ljava/lang/String;");
		}

		@Override
		public void readNullableUTF8Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readNullableUTF8", "()Ljava/lang/String;");
		}

		@Override
		public void readUTF16Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readUTF16", "()Ljava/lang/String;");
		}

		@Override
		public void readNullableUTF16Gen(MethodVisitor mv) {
			mv.visitMethodInsn(invokeOpCode, inputTypeName, "readNullableUTF16", "()Ljava/lang/String;");
		}
	};

	private SerializerFactory(ClassLoader rootClassLoader, Class<?> serializerClass, Class<?> outputClass, Class<?> inputClass,
	                          Class<? extends Exception> outputException, Class<? extends Exception> inputException, boolean serialize, boolean deserialize) {
		this.rootClassLoader = rootClassLoader;
		this.serialize = serialize;
		this.deserialize = deserialize;
		this.libraryClassLoader = new DefiningClassLoader(rootClassLoader);
		this.serializerClass = serializerClass;
		this.outputClass = outputClass;
		this.inputClass = inputClass;
		this.outputTypeName = getInternalName(outputClass);
		this.inputTypeName = getInternalName(inputClass);
		assert inputClass.isInterface() == outputClass.isInterface();
		this.invokeOpCode = inputClass.isInterface() ? INVOKEINTERFACE : INVOKEVIRTUAL;
		this.outputExceptions = outputException == null ? null : new String[]{getInternalName(outputException)};
		this.inputExceptions = inputException == null ? null : new String[]{getInternalName(inputException)};
	}

	public ClassLoader getRootClassLoader() {
		return rootClassLoader;
	}

	/**
	 * Constructs a {@code SerializerFactory} that is able to instantiate serializers that work with buffers.
	 *
	 * @param rootClassLoader class loader to use for loading dynamic classes
	 * @param serialize       determines whether constructed factory can instantiate serializers
	 * @param deserialize     determines whether constructed factory can instantiate deserializers
	 * @return serializer factory that is able to instantiate serializers that work with buffers
	 */
	public static SerializerFactory createBufferSerializerFactory(ClassLoader rootClassLoader, boolean serialize, boolean deserialize) {
		return new SerializerFactory(rootClassLoader, BufferSerializer.class, SerializationOutputBuffer.class, SerializationInputBuffer.class, null, null, serialize, deserialize);
	}

	public static SerializerFactory createBufferSerializerFactory() {
		return createBufferSerializerFactory(getSystemClassLoader(), true, true);
	}

	public <T> BufferSerializer<T> createBufferSerializer(SerializerGen serializerGen, int serializeVersion) {
		checkState(serializerClass == BufferSerializer.class);
		return (BufferSerializer<T>) createSerializer(serializerGen, serializeVersion);
	}

	/**
	 * Constructs buffer serializer for type, described by the given {@code SerializerGen}.
	 *
	 * @param serializerGen {@code SerializerGen} that describes the type that is to serialize
	 * @return buffer serializer for the given {@code SerializerGen}
	 */
	public <T> BufferSerializer<T> createBufferSerializer(SerializerGen serializerGen) {
		return createBufferSerializer(serializerGen, Integer.MAX_VALUE);
	}

	private final static class MethodKey {
		private MethodKey(SerializerGen serializerGen, int version) {
			this.serializerGen = checkNotNull(serializerGen);
			this.version = version;
		}

		private final SerializerGen serializerGen;
		private final int version;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			MethodKey methodKey = (MethodKey) o;

			return (version == methodKey.version) && (serializerGen.equals(methodKey.serializerGen));
		}

		@Override
		public int hashCode() {
			int result = serializerGen.hashCode();
			result = 31 * result + version;
			return result;
		}
	}

	private final static class MethodCall {
		private final String libraryClassName;
		private final String methodName;

		private MethodCall(String libraryClassName, String methodName) {
			this.libraryClassName = libraryClassName;
			this.methodName = methodName;
		}
	}

	private final Map<MethodKey, MethodCall> staticMethods = Maps.newHashMap();

	private final class SerializerCallerInMethods implements SerializerCaller {
		private final String libraryClassName;
		private final DefiningClassWriter cw;
		private int counter = 0;

		private SerializerCallerInMethods() {
			this.libraryClassName = ASM_SERIALIZER_LIBRARY + COUNTER.incrementAndGet();
			this.cw = new DefiningClassWriter(libraryClassLoader);

			cw.visit(V1_7, ACC_PUBLIC + ACC_FINAL + ACC_SUPER, libraryClassName.replace('.', '/'), null, getInternalName(Object.class), null);

			MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
			mv.visitCode();
			mv.visitVarInsn(ALOAD, 0);
			mv.visitMethodInsn(INVOKESPECIAL, getInternalName(Object.class), "<init>", "()V");
			mv.visitInsn(RETURN);
			mv.visitMaxs(1, 1);
			mv.visitEnd();
		}

		public void createStaticClass() {
			if (counter == 0)
				return;
			byte[] bytes = cw.toByteArray();
			getDefineClass(libraryClassLoader, libraryClassName, bytes);
		}

		private MethodCall ensureMethodCall(SerializerGen serializerGen, int version, SerializerBackend backend) {
			Class<?> dataType = serializerGen.getRawType();
			MethodKey methodKey = new MethodKey(serializerGen, version);
			MethodCall methodCall = staticMethods.get(methodKey);
			if (methodCall == null) {
				String methodName = dataType.getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + (++counter);
				methodCall = new MethodCall(libraryClassName, methodName);
				staticMethods.put(methodKey, methodCall);

				if (serialize) {
					MethodVisitor mv1 = cw.visitMethod(ACC_PUBLIC + ACC_STATIC, "serialize_" + methodName,
							getMethodDescriptor(getType(Void.TYPE), getType(outputClass), getType(dataType)),
							null,
							outputExceptions);
					mv1.visitCode();
					mv1.visitVarInsn(ALOAD, 0);
					mv1.visitVarInsn(ALOAD, 1);
					serializerGen.serialize(version, mv1, backend, 0, 2, this, dataType);
					mv1.visitInsn(RETURN);
					mv1.visitMaxs(1, 1);
					mv1.visitEnd();
				}

				if (deserialize) {
					MethodVisitor mv2 = cw.visitMethod(ACC_PUBLIC + ACC_STATIC, "deserialize_" + methodName,
							getMethodDescriptor(getType(dataType), getType(inputClass)),
							null,
							inputExceptions);
					mv2.visitCode();
					mv2.visitVarInsn(ALOAD, 0);
					serializerGen.deserialize(version, mv2, backend, 0, 1, this, dataType);
					mv2.visitInsn(ARETURN);
					mv2.visitMaxs(1, 1);
					mv2.visitEnd();
				}
			}
			return methodCall;
		}

		@Override
		public void serialize(SerializerGen serializerGen, int version, MethodVisitor mv, int locals, int varContainer, Class<?> sourceType) {
			if (serializerGen.isInline()) {
				serializerGen.serialize(version, mv, backend, varContainer, locals, this, sourceType);
				return;
			}
			MethodCall methodCall = ensureMethodCall(serializerGen, version, backend);
			sourceType = castSourceType(mv, sourceType, serializerGen.getRawType());
			mv.visitMethodInsn(INVOKESTATIC, methodCall.libraryClassName.replace('.', '/'), "serialize_" + methodCall.methodName,
					getMethodDescriptor(getType(Void.TYPE), getType(outputClass), getType(sourceType)));
		}

		@Override
		public void deserialize(SerializerGen serializerGen, int version, MethodVisitor mv, int locals, int varContainer, Class<?> targetType) {
			if (serializerGen.isInline()) {
				serializerGen.deserialize(version, mv, backend, varContainer, locals, this, targetType);
				return;
			}
			MethodCall methodCall = ensureMethodCall(serializerGen, version, backend);
			checkArgument(targetType.isAssignableFrom(serializerGen.getRawType()), "%s is not assignable from %s", targetType, serializerGen.getRawType());
			mv.visitMethodInsn(INVOKESTATIC, methodCall.libraryClassName.replace('.', '/'), "deserialize_" + methodCall.methodName,
					getMethodDescriptor(getType(serializerGen.getRawType()), getType(inputClass)));
		}

		@Override
		public DefiningClassLoader getClassLoader() {
			return libraryClassLoader;
		}

	}

	synchronized private Object createSerializer(SerializerGen serializerGen, int serializeVersion) {
		checkState(serializeVersion >= 0, "serializerVersion is negative");
		Class<?> dataType = serializerGen.getRawType();

		DefiningClassLoader classLoader = new DefiningClassLoader(libraryClassLoader);
		ClassWriter cw = new DefiningClassWriter(classLoader);

		String className = (dataType.getName().startsWith("java.") ?
				"$" + dataType.getName() :
				dataType.getName()
		) + "$Serializer" + COUNTER.incrementAndGet();
		Type classType = getType('L' + className.replace('.', '/') + ';');

		cw.visit(V1_7, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
				classType.getInternalName(),
				getDescriptor(Object.class) + "L" + getInternalName(serializerClass) + "<" + getDescriptor(dataType) + ">;",
				getInternalName(Object.class),
				new String[]{getInternalName(serializerClass)});

		{
			MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
			mv.visitCode();
			mv.visitVarInsn(ALOAD, 0);
			mv.visitMethodInsn(INVOKESPECIAL, getInternalName(Object.class), "<init>", "()V");
			mv.visitInsn(RETURN);
			mv.visitMaxs(1, 1);
			mv.visitEnd();
		}
		SerializerCallerInMethods serializerCaller = new SerializerCallerInMethods();
		List<Integer> versions = new ArrayList<>();
		List<Integer> allVersions = new ArrayList<>();
		for (int v : SerializerGen.VersionsCollector.versions(serializerGen)) {
			if (v <= serializeVersion)
				versions.add(v);
			allVersions.add(v);
		}
		Collections.sort(versions);
		Collections.sort(allVersions);
		Optional<Integer> currentVersion = getLatestVersion(versions);
		if (!allVersions.isEmpty() && !currentVersion.isPresent())
			currentVersion = Optional.of(serializeVersion);

		if (serialize) {
			defineSerialize(serializerGen, dataType, cw, classType, serializerCaller, currentVersion);
		}
		if (deserialize) {
			defineDeserialize(serializerGen, dataType, cw, classType, serializerCaller, allVersions);
		}
		cw.visitEnd();

		serializerCaller.createStaticClass();

		byte[] classBytes = cw.toByteArray();
		Class<?> serializerClass = getDefineClass(classLoader, className, classBytes);

		try {
			return serializerClass.newInstance();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

	private Class<?> getDefineClass(DefiningClassLoader definingClassLoader, String className, byte[] classBytes) {
		return definingClassLoader.defineClass(className, classBytes);
	}

	private void defineSerializeVersion(SerializerGen serializerGen, Class<?> dataType, ClassWriter cw, SerializerCallerInMethods serializerCaller, Optional<Integer> version) {
		MethodVisitor mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "serialize",
				getMethodDescriptor(getType(Void.TYPE), getType(outputClass), getType(dataType)),
				null,
				outputExceptions);
		mv.visitCode();

		if (version.isPresent()) {
			mv.visitVarInsn(ALOAD, 1);
			mv.visitLdcInsn(version.get());
			backend.writeByteGen(mv);
		}

		mv.visitVarInsn(ALOAD, 1);
		mv.visitVarInsn(ALOAD, 2);
		serializerCaller.serialize(serializerGen, version.or(0), mv, 3, 1, dataType);
		mv.visitInsn(RETURN);

		mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	private void defineSerializeBridge(Class<?> dataType, ClassWriter cw, Type serializerClassType) {
		MethodVisitor mv = cw.visitMethod(ACC_PUBLIC + ACC_BRIDGE + ACC_SYNTHETIC, "serialize",
				getMethodDescriptor(getType(Void.TYPE), getType(outputClass), getType(Object.class)),
				null,
				outputExceptions);
		mv.visitCode();
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 1);
		mv.visitVarInsn(ALOAD, 2);
		mv.visitTypeInsn(CHECKCAST, getInternalName(dataType));
		mv.visitMethodInsn(INVOKEVIRTUAL, serializerClassType.getInternalName(), "serialize",
				getMethodDescriptor(getType(Void.TYPE), getType(outputClass), getType(dataType)));
		mv.visitInsn(RETURN);
		mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	private void defineSerialize(SerializerGen serializerGen, Class<?> dataType, ClassWriter cw, Type serializerClassType,
	                             SerializerCallerInMethods serializerCaller, Optional<Integer> version) {
		defineSerializeVersion(serializerGen, dataType, cw, serializerCaller, version);
		defineSerializeBridge(dataType, cw, serializerClassType);
	}

	private Optional<Integer> getLatestVersion(List<Integer> versions) {
		return versions.isEmpty() ? Optional.<Integer>absent() : Optional.of(versions.get(versions.size() - 1));
	}

	private void defineDeserializeLatest(SerializerGen serializerGen, Class<?> dataType, ClassWriter cw, Type serializerClassType,
	                                     SerializerCallerInMethods serializerCaller, Optional<Integer> latestVersion) {
		MethodVisitor mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "deserialize",
				getMethodDescriptor(getType(dataType), getType(inputClass)),
				null,
				inputExceptions);
		mv.visitCode();

		if (!latestVersion.isPresent()) {
			mv.visitVarInsn(ALOAD, 1);
			serializerCaller.deserialize(serializerGen, 0, mv, 2, 1, dataType);
			mv.visitInsn(ARETURN);
		} else {
			mv.visitVarInsn(ALOAD, 1);
			backend.readByteGen(mv);
			mv.visitInsn(DUP);
			mv.visitVarInsn(ISTORE, 2);
			mv.visitLdcInsn(latestVersion.get());
			Label fallback = new Label();
			mv.visitJumpInsn(IF_ICMPNE, fallback);

			mv.visitVarInsn(ALOAD, 1);
			serializerCaller.deserialize(serializerGen, latestVersion.get(), mv, 2, 1, dataType);
			mv.visitInsn(ARETURN);

			mv.visitLabel(fallback);
			mv.visitVarInsn(ALOAD, 0);
			mv.visitVarInsn(ALOAD, 1);
			mv.visitVarInsn(ILOAD, 2);
			mv.visitMethodInsn(INVOKEVIRTUAL, serializerClassType.getInternalName(), "deserializeEarlierVersions",
					getMethodDescriptor(getType(dataType), getType(inputClass), getType(Integer.TYPE)));
			mv.visitInsn(ARETURN);
		}

		mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	private void defineDeserializeEarlierVersions(Class<?> dataType, ClassWriter cw, Type serializerClassType, List<Integer> versions) {
		MethodVisitor mv = cw.visitMethod(ACC_PRIVATE + ACC_FINAL, "deserializeEarlierVersions",
				getMethodDescriptor(getType(dataType), getType(inputClass), getType(Integer.TYPE)),
				null,
				inputExceptions);
		mv.visitCode();

		for (int i = versions.size() - 2; i >= 0; i--) {
			int version = versions.get(i);
			mv.visitVarInsn(ILOAD, 2);
			mv.visitLdcInsn(version);
			Label next = new Label();
			mv.visitJumpInsn(IF_ICMPNE, next);

			mv.visitVarInsn(ALOAD, 0);
			mv.visitVarInsn(ALOAD, 1);
			mv.visitMethodInsn(INVOKEVIRTUAL, serializerClassType.getInternalName(), "deserializeVersion" + version,
					getMethodDescriptor(getType(dataType), getType(inputClass)));
			mv.visitInsn(ARETURN);

			mv.visitLabel(next);
		}

		mv.visitInsn(ACONST_NULL); // TODO (vsavchuk): throw exception
		mv.visitInsn(ARETURN);

		mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	private void defineDeserializeVersion(SerializerGen serializerGen, Class<?> dataType, ClassWriter cw, SerializerCallerInMethods serializerCaller, int version) {
		MethodVisitor mv = cw.visitMethod(ACC_PRIVATE + ACC_FINAL, "deserializeVersion" + version,
				getMethodDescriptor(getType(dataType), getType(inputClass)),
				null,
				inputExceptions);
		mv.visitCode();

		mv.visitVarInsn(ALOAD, 1);
		serializerCaller.deserialize(serializerGen, version, mv, 2, 1, dataType);
		mv.visitInsn(ARETURN);

		mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	private void defineDeserializeBridge(Class<?> dataType, ClassWriter cw, Type serializerClassType) {
		MethodVisitor mv = cw.visitMethod(ACC_PUBLIC + ACC_BRIDGE + ACC_SYNTHETIC, "deserialize",
				getMethodDescriptor(getType(Object.class), getType(inputClass)),
				null,
				inputExceptions);
		mv.visitCode();
		mv.visitVarInsn(ALOAD, 0);
		mv.visitVarInsn(ALOAD, 1);
		mv.visitMethodInsn(INVOKEVIRTUAL, serializerClassType.getInternalName(), "deserialize",
				getMethodDescriptor(getType(dataType), getType(inputClass)));
		mv.visitInsn(ARETURN);
		mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	private void defineDeserialize(SerializerGen serializerGen, Class<?> dataType, ClassWriter cw, Type serializerClassType, SerializerCallerInMethods serializerCaller, List<Integer> versions) {
		defineDeserializeLatest(serializerGen, dataType, cw, serializerClassType, serializerCaller, getLatestVersion(versions));
		defineDeserializeEarlierVersions(dataType, cw, serializerClassType, versions);
		for (int i = versions.size() - 2; i >= 0; i--) {
			int version = versions.get(i);
			defineDeserializeVersion(serializerGen, dataType, cw, serializerCaller, version);
		}
		defineDeserializeBridge(dataType, cw, serializerClassType);
	}

}
