package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.global.common.PrivKey;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
	private static final Logger logger = LoggerFactory.getLogger(FsContainerKeyManager.class);

	public static final String DEFAULT_ACTUAL_KEYS_FILE = "actual.dat";
	public static final String DEFAULT_EXPECTED_KEYS_FILE = "expected.dat";

	private final Eventloop eventloop;
	private final Executor executor;
	private final Path containerDir;
	private final Path incoming;
	private final Path outgoing;

	private FsContainerKeyManager(Eventloop eventloop, Executor executor, Path containerDir, String incomingFilename, String outgoingFilename) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.containerDir = containerDir;
		this.incoming = containerDir.resolve(incomingFilename);
		this.outgoing = containerDir.resolve(outgoingFilename);
	}

	public static FsContainerKeyManager create(Eventloop eventloop, Executor executor, Path containersDir,
			String incomingFilename, String outgoingFileName) {
		return new FsContainerKeyManager(eventloop, executor, containersDir, incomingFilename, outgoingFileName);
	}

	public static FsContainerKeyManager create(Eventloop eventloop, Executor executor, Path containersDir, boolean forApp) {
		return forApp ?
				new FsContainerKeyManager(eventloop, executor, containersDir, DEFAULT_EXPECTED_KEYS_FILE, DEFAULT_ACTUAL_KEYS_FILE) :
				new FsContainerKeyManager(eventloop, executor, containersDir, DEFAULT_ACTUAL_KEYS_FILE, DEFAULT_EXPECTED_KEYS_FILE);
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		return Promise.ofBlockingRunnable(executor, () -> {
			try {
				Files.createDirectories(containerDir);
				if (!Files.exists(incoming)) {
					Files.createFile(incoming);
				}
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	@Override
	public @NotNull Promise<?> stop() {
		return Promise.complete();
	}

	@Override
	public Promise<Set<PrivKey>> getKeys() {
		return Promise.ofBlockingCallable(executor, () -> {
			try {
				Set<PrivKey> privKeys = new HashSet<>();
				List<String> lines = Files.readAllLines(incoming);
				for (String line : lines) {
					if (line.trim().isEmpty()) {
						logger.trace("Empty line found in place of private key, skipping");
					}
					try {
						privKeys.add(PrivKey.fromString(line));
					} catch (ParseException ignored) {
						logger.warn("Could not parse private key from line {}", line);
					}
				}
				return privKeys;
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	@Override
	public Promise<Void> updateKeys(Set<PrivKey> keys) {
		return Promise.ofBlockingRunnable(executor, () -> {
			try {
				Path temp = outgoing.resolveSibling("tmp.dat");
				Files.write(temp, keys.stream().map(PrivKey::asString).collect(toSet()));
				Files.move(temp, outgoing, ATOMIC_MOVE, REPLACE_EXISTING);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}
}
