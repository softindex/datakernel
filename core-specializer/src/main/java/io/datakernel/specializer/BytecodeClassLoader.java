package io.datakernel.specializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class BytecodeClassLoader extends ClassLoader {
	final AtomicInteger classN = new AtomicInteger();

	private final Map<String, byte[]> extraClassDefs = new HashMap<>();

	public BytecodeClassLoader() {
	}

	public BytecodeClassLoader(ClassLoader parent) {
		super(parent);
	}

	synchronized void register(String className, byte[] bytecode) {
		extraClassDefs.put(className, bytecode);
	}

	@Override
	synchronized protected Class<?> findClass(final String name) throws ClassNotFoundException {
		byte[] classBytes = this.extraClassDefs.remove(name);
		if (classBytes != null) {
			return defineClass(name, classBytes, 0, classBytes.length);
		}
		return super.findClass(name);
	}

}
