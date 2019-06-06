package io.datakernel.di.error;

import java.lang.reflect.Member;

public final class InjectionFailedException extends RuntimeException {
	private final Member member;

	public InjectionFailedException(Member member, ReflectiveOperationException cause) {
		super("Failed to inject member injectable member " + member, cause);
		this.member = member;
	}

	public Member getMember() {
		return member;
	}
}
