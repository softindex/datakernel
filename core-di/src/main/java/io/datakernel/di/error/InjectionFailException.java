package io.datakernel.di.error;

import java.lang.reflect.Member;

public final class InjectionFailException extends RuntimeException {
	private final Member member;

	public InjectionFailException(Member member, ReflectiveOperationException cause) {
		super("Failed to inject member injectable member " + member, cause);
		this.member = member;
	}

	public Member getMember() {
		return member;
	}
}
