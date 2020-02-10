package io.datakernel.specializer;

import io.datakernel.specializer.Utils.*;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.*;
import org.objectweb.asm.commons.AnalyzerAdapter;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.tree.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.specializer.Utils.*;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getMethodDescriptor;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.GeneratorAdapter.NE;
import static org.objectweb.asm.commons.Method.getMethod;

@SuppressWarnings("unchecked")
public final class Specializer {
	private static final AtomicInteger STATIC_VALUE_N = new AtomicInteger();
	private static final Map<Integer, Object> STATIC_VALUES = new ConcurrentHashMap<>();

	private final BytecodeClassLoader classLoader;

	private Predicate<Class<?>> predicate;

	final Map<IdentityKey<?>, Specialization> specializations = new HashMap<>();

	private Path bytecodeSaveDir;

	private Specializer(ClassLoader parent) {classLoader = new BytecodeClassLoader(parent);}

	private Specializer() {classLoader = new BytecodeClassLoader();}

	public static Specializer create(ClassLoader parent) {
		return new Specializer(parent);
	}

	public static Specializer create() {
		return new Specializer();
	}

	public Specializer withPredicate(Predicate<Class<?>> predicate) {
		this.predicate = predicate;
		return this;
	}

	public Specializer withBytecodeSaveDir(Path bytecodeSaveDir) {
		this.bytecodeSaveDir = bytecodeSaveDir;
		return this;
	}

	@SuppressWarnings("PointlessBooleanExpression")
	final class Specialization {
		public static final String THIS = "$this";

		final Object instance;
		final Class<?> instanceClass;
		final Type specializedType;
		Class<?> specializedClass;
		Object specializedInstance;

		final List<Specialization> relatedSpecializations = new ArrayList<>(singletonList(this));

		final Map<java.lang.reflect.Field, String> specializedFields = new LinkedHashMap<>();
		final Map<java.lang.reflect.Method, String> specializedMethods = new LinkedHashMap<>();

		Specialization(Object instance) {
			this.instance = instance;
			this.instanceClass = normalizeClass(instance.getClass());
			this.specializedType = Type.getType("L" + ("SpecializedClass" + classLoader.classN.incrementAndGet()) + ";");
		}

		void scanInstance() {
			for (Class<?> clazz = instance.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
				for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
					specializedFields.put(field,
							field.getDeclaringClass().getSimpleName() + "$" + field.getName());
				}
			}

			for (Class<?> clazz = instance.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
				for (java.lang.reflect.Method javaMethod : clazz.getDeclaredMethods()) {
					if (Modifier.isStatic(javaMethod.getModifiers())) continue;
					if (Modifier.isAbstract(javaMethod.getModifiers())) continue;
					specializedMethods.put(javaMethod,
							javaMethod.getDeclaringClass().getSimpleName() + "$" + javaMethod.getName());
				}
			}

			for (Field field : specializedFields.keySet()) {
				if (!Modifier.isFinal(field.getModifiers())) continue;
				if (field.getType().isPrimitive()) continue;
				if (field.getType().isArray() || field.getType().getPackage().getName().startsWith("java.lang."))
					continue;
				field.setAccessible(true);
				Object fieldInstance;
				try {
					fieldInstance = field.get(this.instance);
				} catch (IllegalAccessException e) {
					throw new IllegalArgumentException(e);
				}
				if (fieldInstance == null) continue;
				Class<?> fieldInstanceClazz = fieldInstance.getClass();
				if (fieldInstanceClazz.isSynthetic()) continue;
				if (fieldInstanceClazz.getClassLoader() instanceof BytecodeClassLoader) continue;
				if (predicate != null && !predicate.test(fieldInstance.getClass())) continue;
				relatedSpecializations.add(ensureSpecialization(fieldInstance));
			}
		}

		public Object ensureInstance() {
			if (specializedInstance != null) return specializedInstance;
			try {
				specializedInstance = ensureClass().newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return specializedInstance;
		}

		public Class<?> ensureClass() {
			if (specializedClass != null) return specializedClass;
			byte[] bytecode = defineNewClass();
			String className = specializedType.getClassName();
			classLoader.register(className, bytecode);
			try {
				specializedClass = classLoader.loadClass(className);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			return specializedClass;
		}

		byte[] defineNewClass() {
			Set<Class<?>> interfaces = new HashSet<>();
			for (Class<?> clazz = instance.getClass(); clazz != Object.class; clazz = clazz.getSuperclass()) {
				interfaces.addAll(Arrays.asList(clazz.getInterfaces()));
			}

			ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
			cw.visit(V1_8, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
					specializedType.getInternalName(),
					null,
					Type.getInternalName(Object.class),
					interfaces.stream().map(Type::getInternalName).toArray(String[]::new));

			cw.visitField(ACC_PUBLIC | ACC_STATIC | ACC_FINAL, "$this",
					Type.getType(instanceClass).getDescriptor(), null, null);

			for (Map.Entry<java.lang.reflect.Field, String> entry : specializedFields.entrySet()) {
				java.lang.reflect.Field javaField = entry.getKey();
				String name = entry.getValue();

				cw.visitField(ACC_PUBLIC | ACC_STATIC | (javaField.getModifiers() & (ACC_FINAL | ACC_VOLATILE)), name,
						Type.getType(javaField.getType()).getDescriptor(), null, null);
			}

			{
				Method m = getMethod("void <clinit> ()");
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC, m, null, null, cw);

				g.push(registerStaticValue(instance));
				g.invokeStatic(Type.getType(Specializer.class),
						new Method("takeStaticValue", getType(Object.class), new Type[]{getType(int.class)}));
				g.checkCast(getType(instanceClass));
				g.putStatic(specializedType, THIS, getType(instanceClass));

				for (Map.Entry<java.lang.reflect.Field, String> entry : specializedFields.entrySet()) {
					java.lang.reflect.Field javaField = entry.getKey();
					String fieldName = entry.getValue();

					javaField.setAccessible(true);
					Object fieldInstance = null;
					try {
						fieldInstance = javaField.get(this.instance);
					} catch (IllegalAccessException e) {
						throw new IllegalArgumentException(e);
					}
					if (fieldInstance == null) {
						g.visitInsn(ACONST_NULL);
					} else {
						g.push(registerStaticValue(fieldInstance));
						g.invokeStatic(Type.getType(Specializer.class),
								new Method("takeStaticValue", getType(Object.class), new Type[]{getType(int.class)}));
					}
					if (javaField.getType().isPrimitive()) {
						g.checkCast(getType(getBoxedType(javaField.getType())));
						g.unbox(getType(javaField.getType()));
					} else {
						g.checkCast(getType(javaField.getType()));
					}
					g.putStatic(specializedType, fieldName, getType(javaField.getType()));
				}

				g.returnValue();
				g.endMethod();
			}

			{
				Method m = getMethod("void <init> ()");
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);
				g.loadThis();
				g.invokeConstructor(getType(Object.class), m);
				g.returnValue();
				g.endMethod();
			}

			for (Map.Entry<java.lang.reflect.Method, String> entry : specializedMethods.entrySet()) {
				java.lang.reflect.Method javaMethod = entry.getKey();
				String specializedMethodName = entry.getValue();
				ClassNode classNode = ensureClassNode(javaMethod.getDeclaringClass());
				String methodDesc = getMethodDescriptor(
						getType(javaMethod.getReturnType()),
						Arrays.stream(javaMethod.getParameterTypes()).map(Type::getType).toArray(Type[]::new));
				//noinspection OptionalGetWithoutIsPresent

				transformMethod(
						classNode.methods.stream()
								.filter(methodNode -> true &&
										methodNode.name.equals(javaMethod.getName()) &&
										methodNode.desc.equals(methodDesc))
								.findFirst()
								.get(),
						new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC | ACC_FINAL,
								new Method(specializedMethodName, methodDesc), null, null, cw));
			}

			Set<Method> interfaceMethods = interfaces.stream()
					.flatMap(i -> Arrays.stream(i.getMethods())
							.map(m -> new Method(
									m.getName(),
									getMethodDescriptor(
											getType(m.getReturnType()),
											Arrays.stream(m.getParameterTypes()).map(Type::getType).toArray(Type[]::new)))))
					.collect(toSet());

			for (Method method : interfaceMethods) {
				String methodImpl = lookupMethod(instance.getClass(), method);
				if (methodImpl == null) continue;
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC | ACC_FINAL, method, null, null, cw);
				for (int i = 0; i < method.getArgumentTypes().length; i++) {
					g.loadArg(i);
				}
				g.invokeStatic(specializedType, new Method(methodImpl, method.getDescriptor()));
				g.returnValue();
				g.endMethod();
			}

			cw.visitEnd();

			if (bytecodeSaveDir != null) {
				try (FileOutputStream fos = new FileOutputStream(bytecodeSaveDir.resolve(specializedType.getClassName() + ".class").toFile())) {
					fos.write(cw.toByteArray());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			return cw.toByteArray();
		}

		@SuppressWarnings("ConstantConditions")
		void transformMethod(MethodNode methodNode, GeneratorAdapter g) {
			AnalyzerAdapter analyzerAdapter = new AnalyzerAdapter(getType(instanceClass).getInternalName(), ACC_PUBLIC | ACC_FINAL, methodNode.name, methodNode.desc, null);

			Type[] methodParameters = new Method(methodNode.name, methodNode.desc).getArgumentTypes();
			final Map<Integer, Integer> localsRemapping = new HashMap<>();
			Map<LabelNode, Map<Integer, Integer>> localRemappingsByLabel = new HashMap<>();
			AbstractInsnNode insn;
			for (int i = 0; i < methodNode.instructions.size(); i++, insn.accept(analyzerAdapter)) {
				insn = methodNode.instructions.get(i);
				int opcode = insn.getOpcode();

				if (insn instanceof JumpInsnNode) {
					JumpInsnNode insnJump = (JumpInsnNode) insn;
					localRemappingsByLabel.put(insnJump.label, new HashMap<>(localsRemapping));
				}

				if (insn instanceof LabelNode) {
					LabelNode insnLabel = (LabelNode) insn;
					methodNode.tryCatchBlocks.stream()
							.filter(block -> block.end == insnLabel)
							.findFirst()
							.ifPresent(block ->
									localRemappingsByLabel.put(block.handler, new HashMap<>(localsRemapping)));
				}

				if (insn instanceof LabelNode) {
					LabelNode insnLabel = (LabelNode) insn;
					if (localRemappingsByLabel.containsKey(insnLabel)) {
						localsRemapping.clear();
						localsRemapping.putAll(localRemappingsByLabel.get(insnLabel));
					}
					g.visitLabel(insnLabel.getLabel());
					continue;
				}

				if (insn instanceof FrameNode) {
					FrameNode insnFrame = (FrameNode) insn;
					for (Integer k : new ArrayList<>(localsRemapping.keySet())) {
						if (k >= insnFrame.local.size()) {
							localsRemapping.remove(k);
						}
					}
					continue;
				}

				switch (opcode) {
					case ACONST_NULL:
					case ICONST_M1:
					case ICONST_0:
					case ICONST_1:
					case ICONST_2:
					case ICONST_3:
					case ICONST_4:
					case ICONST_5:
					case LCONST_0:
					case LCONST_1:
					case FCONST_0:
					case FCONST_1:
					case FCONST_2:
					case DCONST_0:
					case DCONST_1:
						g.visitInsn(opcode);
						break;

					case BIPUSH:
					case SIPUSH:
						g.visitIntInsn(opcode, ((IntInsnNode) insn).operand);
						break;

					case LDC:
						g.visitLdcInsn(((LdcInsnNode) insn).cst);
						break;

					case ILOAD:
					case LLOAD:
					case FLOAD:
					case DLOAD:
					case ALOAD: {
						VarInsnNode insnVar = (VarInsnNode) insn;
						if (insnVar.var == 0) {
							g.getStatic(specializedType, THIS, getType(instanceClass));
							break;
						}
						if (insnVar.var - 1 < methodParameters.length) {
							g.loadArg(insnVar.var - 1);
							break;
						}
						g.loadLocal(localsRemapping.get(insnVar.var));
						break;
					}

					case IALOAD:
					case LALOAD:
					case FALOAD:
					case DALOAD:
					case AALOAD:
					case BALOAD:
					case CALOAD:
					case SALOAD:
						g.visitInsn(opcode);
						break;

					case ISTORE:
					case LSTORE:
					case FSTORE:
					case DSTORE:
					case ASTORE: {
						VarInsnNode insnVar = (VarInsnNode) insn;
						int var = insnVar.var;

						if (var - 1 < methodParameters.length) {
							g.storeArg(var - 1);
							break;
						}

						if (localsRemapping.containsKey(var)) {
							g.storeLocal(localsRemapping.get(var));
						} else {
							Object top = analyzerAdapter.stack.get(analyzerAdapter.stack.size() - 1);
							Type type = null;
							if (top == Opcodes.INTEGER) type = Type.INT_TYPE;
							if (top == Opcodes.FLOAT) type = Type.FLOAT_TYPE;
							if (top == Opcodes.DOUBLE) type = Type.DOUBLE_TYPE;
							if (top == Opcodes.LONG) type = Type.LONG_TYPE;
							if (top == Opcodes.NULL) type = getType(Object.class);
							if (top instanceof String) type = Type.getType(internalizeClassName((String) top));
							int newLocal = g.newLocal(type);
							localsRemapping.put(var, newLocal);
							g.storeLocal(newLocal);
						}
						break;
					}

					case IASTORE:
					case LASTORE:
					case FASTORE:
					case DASTORE:
					case AASTORE:
					case BASTORE:
					case CASTORE:
					case SASTORE:
						g.visitInsn(opcode);
						break;

					case POP:
					case POP2:
					case DUP:
					case DUP_X1:
					case DUP_X2:
					case DUP2:
					case DUP2_X1:
					case DUP2_X2:
						g.visitInsn(opcode);
						break;

					case IADD:
					case LADD:
					case FADD:
					case DADD:
					case ISUB:
					case LSUB:
					case FSUB:
					case DSUB:
					case IMUL:
					case LMUL:
					case FMUL:
					case DMUL:
					case IDIV:
					case LDIV:
					case FDIV:
					case DDIV:
					case IREM:
					case LREM:
					case FREM:
					case DREM:
					case INEG:
					case LNEG:
					case FNEG:
					case DNEG:
					case ISHL:
					case LSHL:
					case ISHR:
					case LSHR:
					case IUSHR:
					case LUSHR:
					case IAND:
					case LAND:
					case IOR:
					case LOR:
					case IXOR:
					case LXOR:
						g.visitInsn(opcode);
						break;

					case IINC: {
						IincInsnNode insnInc = (IincInsnNode) insn;
						int var = insnInc.var;
						if (var - 1 < methodParameters.length) {
							g.visitIincInsn(var, insnInc.incr);
							break;
						}
						g.iinc(localsRemapping.get(insnInc.var), insnInc.incr);
						break;
					}

					case I2L:
					case I2F:
					case I2D:
					case L2I:
					case L2F:
					case L2D:
					case F2I:
					case F2L:
					case F2D:
					case D2I:
					case D2L:
					case D2F:
					case I2B:
					case I2C:
					case I2S:
						g.visitInsn(opcode);
						break;

					case IFEQ:
					case IFNE:
					case IFLT:
					case IFGE:
					case IFGT:
					case IFLE:
					case IF_ICMPEQ:
					case IF_ICMPNE:
					case IF_ICMPLT:
					case IF_ICMPGE:
					case IF_ICMPGT:
					case IF_ICMPLE:
					case IF_ACMPEQ:
					case IF_ACMPNE:
					case GOTO:
					case IFNULL:
					case IFNONNULL:
						g.visitJumpInsn(opcode, ((JumpInsnNode) insn).label.getLabel());
						break;

					case GETSTATIC:
					case PUTSTATIC: {
						FieldInsnNode insnField = (FieldInsnNode) insn;
						g.visitFieldInsn(opcode, insnField.owner, insnField.name, insnField.desc);
						break;
					}

					case GETFIELD: {
						FieldInsnNode insnField = (FieldInsnNode) insn;
						Type ownerType = getType(internalizeClassName(insnField.owner));
						doCall(g, ownerType, new Type[]{},
								s -> Optional.ofNullable(s.lookupField(s.instance.getClass(), insnField.name))
										.map(lookupField ->
												() -> g.getStatic(s.specializedType, lookupField, getType(insnField.desc))),
								() -> g.visitFieldInsn(GETFIELD, insnField.owner, insnField.name, insnField.desc));
						break;
					}

					case PUTFIELD: {
						FieldInsnNode insnField = (FieldInsnNode) insn;
						Type ownerType = getType(internalizeClassName(insnField.owner));
						doCall(g, ownerType, new Type[]{getType(insnField.desc)},
								s -> Optional.ofNullable(s.lookupField(s.instance.getClass(), insnField.name))
										.map(lookupField ->
												() -> g.putStatic(s.specializedType, lookupField, getType(insnField.desc))),
								() -> g.visitFieldInsn(PUTFIELD, insnField.owner, insnField.name, insnField.desc));
						break;
					}

					case INVOKESTATIC: {
						MethodInsnNode insnMethod = (MethodInsnNode) insn;
						g.visitMethodInsn(INVOKESTATIC, insnMethod.owner, insnMethod.name, insnMethod.desc, false);
						break;
					}
					case INVOKEINTERFACE:
					case INVOKEVIRTUAL: {
						MethodInsnNode insnMethod = (MethodInsnNode) insn;
						Method method = new Method(insnMethod.name, insnMethod.desc);
						Type ownerType = getType(internalizeClassName(insnMethod.owner));
						doCall(g, ownerType, method.getArgumentTypes(),
								s -> Optional.ofNullable(s.lookupMethod(s.instance.getClass(), method))
										.map(lookupMethod ->
												() -> g.invokeStatic(s.specializedType, new Method(lookupMethod, method.getDescriptor()))),
								() -> {
									if (opcode == INVOKEINTERFACE) {
										g.invokeInterface(ownerType, method);
									} else if (opcode == INVOKEVIRTUAL) {
										g.invokeVirtual(ownerType, method);
									}

								});
						break;
					}
					case INVOKESPECIAL: {
						MethodInsnNode insnMethod = (MethodInsnNode) insn;
						if (insnMethod.name.equals("<init>")) {
							g.visitMethodInsn(INVOKESPECIAL, insnMethod.owner, insnMethod.name, insnMethod.desc, false);
							break;
						}
						Method method = new Method(insnMethod.name, insnMethod.desc);

						List<Integer> paramLocals = new ArrayList<>();
						for (Type type : method.getArgumentTypes()) {
							int paramLocal = g.newLocal(type);
							paramLocals.add(paramLocal);
							g.storeLocal(paramLocal);
						}
						Collections.reverse(paramLocals);

						g.pop();
						for (int paramLocal : paramLocals) {
							g.loadLocal(paramLocal);
						}

						String name = lookupMethod(
								loadClass(classLoader, getType(internalizeClassName(insnMethod.owner))),
								method);
						g.invokeStatic(specializedType,
								new Method(
										name,
										method.getDescriptor()));
						break;
					}

					case INVOKEDYNAMIC: {
						InvokeDynamicInsnNode insnInvokeDynamic = (InvokeDynamicInsnNode) insn;
						g.visitInvokeDynamicInsn(insnInvokeDynamic.name, insnInvokeDynamic.desc, insnInvokeDynamic.bsm, insnInvokeDynamic.bsmArgs);
						break;
					}

					case NEW:
					case NEWARRAY:
					case ANEWARRAY:
						g.visitTypeInsn(opcode, ((TypeInsnNode) insn).desc);
						break;

					case ARRAYLENGTH:
						g.visitInsn(opcode);
						break;

					case ATHROW:
						g.visitInsn(opcode);
						break;

					case CHECKCAST:
					case INSTANCEOF:
						g.visitTypeInsn(opcode, ((TypeInsnNode) insn).desc);
						break;

					case MONITORENTER:
					case MONITOREXIT:
						g.visitInsn(opcode);
						break;

					case ARETURN:
					case IRETURN:
					case FRETURN:
					case LRETURN:
					case DRETURN:
					case RETURN:
						g.visitInsn(opcode);
						break;

					default:
						throw new UnsupportedOperationException("" + opcode + " " + insn);
				}
			}

			for (int i = 0; i < methodNode.tryCatchBlocks.size(); i++) {
				TryCatchBlockNode tryCatchBlock = methodNode.tryCatchBlocks.get(i);
				g.visitTryCatchBlock(tryCatchBlock.start.getLabel(), tryCatchBlock.end.getLabel(), tryCatchBlock.handler.getLabel(),
						tryCatchBlock.type);
			}

			g.endMethod();
		}

		private void doCall(GeneratorAdapter g,
				Type ownerType, Type[] paramTypes,
				Function<Specialization, Optional<Runnable>> staticCallSupplier,
				Runnable defaultCall) {

			Class<?> ownerClazz = loadClass(classLoader, ownerType);

			int[] paramLocals = new int[paramTypes.length];
			for (int j = paramTypes.length - 1; j >= 0; j--) {
				Type type = paramTypes[j];
				int paramLocal = g.newLocal(type);
				paramLocals[j] = paramLocal;
				g.storeLocal(paramLocal);
			}

			Label labelExit = g.newLabel();

			for (Specialization s : relatedSpecializations) {
				if (!ownerClazz.isAssignableFrom(s.instance.getClass())) continue;
				Optional<Runnable> staticCall = staticCallSupplier.apply(s);
				if (!staticCall.isPresent()) continue;

				Label labelNext = g.newLabel();

				g.dup();
				g.getStatic(s.specializedType, THIS, getType(s.instanceClass));
				g.ifCmp(getType(Object.class), NE, labelNext);

				g.pop();

				for (int paramLocal : paramLocals) {
					g.loadLocal(paramLocal);
				}
				staticCall.get().run();
				g.goTo(labelExit);

				g.mark(labelNext);
			}

			g.checkCast(ownerType);
			for (int paramLocal : paramLocals) {
				g.loadLocal(paramLocal);
			}
			defaultCall.run();

			g.mark(labelExit);
		}

		@Nullable
		String lookupField(Class<?> owner, String field) {
			java.lang.reflect.Field result = null;
			for (java.lang.reflect.Field originalField : specializedFields.keySet()) {
				if (true &&
						Objects.equals(originalField.getName(), field) &&
						originalField.getDeclaringClass().isAssignableFrom(owner) &&
						(result == null ||
								result.getDeclaringClass().isAssignableFrom(originalField.getDeclaringClass()))) {
					result = originalField;
				}
			}
			return specializedFields.get(result);
		}

		@Nullable
		String lookupMethod(Class<?> owner, Method method) {
			java.lang.reflect.Method result = null;
			for (java.lang.reflect.Method originalMethod : specializedMethods.keySet()) {
				if (true &&
						Objects.equals(originalMethod.getName(), method.getName()) &&
						Objects.equals(
								Arrays.stream(originalMethod.getParameters()).map(p -> getType(p.getType())).collect(toList()),
								Arrays.asList(method.getArgumentTypes())) &&
						originalMethod.getDeclaringClass().isAssignableFrom(owner) &&
						(result == null ||
								result.getDeclaringClass().isAssignableFrom(originalMethod.getDeclaringClass()))) {
					result = originalMethod;
				}
			}
			return specializedMethods.get(result);
		}

	}

	synchronized static private int registerStaticValue(Object value) {
		int idx = STATIC_VALUE_N.incrementAndGet();
		STATIC_VALUES.put(idx, value);
		return idx;
	}

	synchronized static public Object takeStaticValue(int idx) {
		return STATIC_VALUES.remove(idx);
	}

	public <T> T specialize(T instance) {
		if (instance.getClass().getClassLoader() instanceof BytecodeClassLoader) return instance;
		if (predicate != null && !predicate.test(instance.getClass())) return instance;
		Specialization specialization = ensureSpecialization(instance);
		for (Specialization s : specializations.values()) {
			s.ensureClass();
		}
		return (T) specialization.ensureInstance();
	}

	private <T> Specialization ensureSpecialization(T instance) {
		IdentityKey<Object> key = new IdentityKey<>(instance);
		Specialization specialization = specializations.get(key);
		if (specialization == null) {
			specialization = new Specialization(instance);
			specializations.put(key, specialization);
			specialization.scanInstance();
		}
		return specialization;
	}

	private ClassNode ensureClassNode(Class<?> clazz) {
		ClassNode classNode = new ClassNode();
		ClassReader cr;
		try {
			cr = new ClassReader(clazz.getName());
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
		cr.accept(classNode, ClassReader.SKIP_DEBUG | ClassReader.EXPAND_FRAMES);
		return classNode;
	}

	public boolean isSpecialized(Object instance) {
		return specializations.containsKey(new IdentityKey<>(instance));
	}

	public BytecodeClassLoader getClassLoader() {
		return classLoader;
	}
}
