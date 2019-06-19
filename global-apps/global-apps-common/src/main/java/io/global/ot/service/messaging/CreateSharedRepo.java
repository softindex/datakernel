package io.global.ot.service.messaging;

import io.global.common.PubKey;

import java.util.Set;

public final class CreateSharedRepo {
	private final String id;
	private final String name;
	private final Set<PubKey> participants;

	public CreateSharedRepo(String id, String name, Set<PubKey> participants) {
		this.id = id;
		this.name = name;
		this.participants = participants;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public Set<PubKey> getParticipants() {
		return participants;
	}
}
