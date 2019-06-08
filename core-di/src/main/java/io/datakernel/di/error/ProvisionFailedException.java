package io.datakernel.di.error;

import io.datakernel.di.core.Key;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Executable;

public final class ProvisionFailedException extends RuntimeException {
	@Nullable
	private final Key<?> requestedKey;
	private final Executable executable;

	public ProvisionFailedException(@Nullable Key<?> requestedKey, Executable executable, ReflectiveOperationException cause) {
		super("Failed to call " + executable + (requestedKey != null ? " to provide requested key " + requestedKey : ""), cause);
		this.requestedKey = requestedKey;
		this.executable = executable;
	}

	@Nullable
	public Key<?> getRequestedKey() {
		return requestedKey;
	}

	public Executable getExecutable() {
		return executable;
	}
}
