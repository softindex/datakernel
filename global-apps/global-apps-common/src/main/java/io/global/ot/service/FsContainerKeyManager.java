package io.global.ot.service;

import io.datakernel.async.Promise;
import io.global.common.PrivKey;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toSet;

public final class FsContainerKeyManager implements ContainerKeyManager {
	public static final String DEFAULT_ACTUAL_KEYS_FILE = "actual.dat";
	public static final String DEFAULT_EXPECTED_KEYS_FILE = "expected.dat";

	private final Executor executor;
	private final Path incoming;
	private final Path outgoing;

	private FsContainerKeyManager(Executor executor, Path incoming, Path outgoing) {
		this.executor = executor;
		this.incoming = incoming;
		this.outgoing = outgoing;
	}

	public static FsContainerKeyManager create(Executor executor, Path containersDir, boolean forApp) {
		Path actual = containersDir.resolve(DEFAULT_ACTUAL_KEYS_FILE);
		Path expected = containersDir.resolve(DEFAULT_EXPECTED_KEYS_FILE);
		return forApp ?
				new FsContainerKeyManager(executor, expected, actual) :
				new FsContainerKeyManager(executor, actual, expected);
	}

	@Override
	public Promise<Set<PrivKey>> getKeys() {
		return Promise.ofBlockingCallable(executor, () -> {
			List<String> lines = Files.readAllLines(incoming);
			Set<PrivKey> privKeys = new HashSet<>();
			for (String line : lines) {
				privKeys.add(PrivKey.fromString(line));
			}
			return privKeys;
		});
	}

	@Override
	public Promise<Void> updateKeys(Set<PrivKey> keys) {
		return Promise.ofBlockingRunnable(executor, () -> {
			Path temp = outgoing.resolveSibling("tmp.dat");
			Files.write(temp, keys.stream().map(PrivKey::asString).collect(toSet()));
			Files.move(temp, outgoing, ATOMIC_MOVE, REPLACE_EXISTING);
		});
	}
}
