package io.global.ot.service.messaging;

import io.global.ot.shared.SharedRepo;

public final class CreateSharedRepo {
	private final SharedRepo sharedRepo;

	public CreateSharedRepo(SharedRepo sharedRepo) {
		this.sharedRepo = sharedRepo;
	}

	public SharedRepo getSharedRepo() {
		return sharedRepo;
	}
}
