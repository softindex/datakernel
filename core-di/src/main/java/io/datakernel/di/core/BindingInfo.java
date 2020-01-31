package io.datakernel.di.core;

import io.datakernel.di.util.LocationInfo;
import io.datakernel.di.util.MarkedBinding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static io.datakernel.di.core.BindingType.EAGER;
import static io.datakernel.di.core.BindingType.TRANSIENT;

public final class BindingInfo {
	private final Set<Dependency> dependencies;
	private final BindingType type;

	@Nullable
	private final LocationInfo location;

	public BindingInfo(Set<Dependency> dependencies, BindingType type, @Nullable LocationInfo location) {
		this.dependencies = dependencies;
		this.type = type;
		this.location = location;
	}

	public static BindingInfo from(MarkedBinding<?> markedBinding) {
		Binding<?> binding = markedBinding.getBinding();
		return new BindingInfo(binding.getDependencies(), markedBinding.getType(), binding.getLocation());
	}

	@NotNull
	public Set<Dependency> getDependencies() {
		return dependencies;
	}

	public BindingType getType() {
		return type;
	}

	@Nullable
	public LocationInfo getLocation() {
		return location;
	}

	@Override
	public String toString() {
		return (type == TRANSIENT ? "*" : type == EAGER ? "!" : "") + "Binding" + dependencies;
	}
}
