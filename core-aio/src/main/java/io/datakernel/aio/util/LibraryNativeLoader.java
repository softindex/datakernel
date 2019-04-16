package io.datakernel.aio.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class LibraryNativeLoader {

	public static void loadLibrary(String libName) {
		String file = libName + ".so";
		try {
			Path tmp = Paths.get("tmp-" + file);
			InputStream stream = ClassLoader.getSystemClassLoader().getResourceAsStream(file);
			if (stream == null) {
				throw new UnsatisfiedLinkError(libName);
			}
			Files.copy(stream, tmp, REPLACE_EXISTING);
			System.load(tmp.toAbsolutePath().toString());
			Files.delete(tmp);
		} catch (IOException ex) {
			throw new UnsatisfiedLinkError(libName);
		}
	}
}
