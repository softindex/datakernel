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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toSet;

public final class FsKeyExchanger implements KeyExchanger {
	private static final Logger logger = LoggerFactory.getLogger(FsKeyExchanger.class);

	public static final String DEFAULT_ACTUAL_KEYS_FILE = "actual.dat";
	public static final String DEFAULT_EXPECTED_KEYS_FILE = "expected.dat";

	private final Eventloop eventloop;
	private final Executor executor;
	private final Path containerDir;
	private final Path incoming;
	private final Path outgoing;

	private FsKeyExchanger(Eventloop eventloop, Executor executor, Path containerDir, String incomingFilename, String outgoingFilename) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.containerDir = containerDir;
		this.incoming = containerDir.resolve(incomingFilename);
		this.outgoing = containerDir.resolve(outgoingFilename);
	}

	public static FsKeyExchanger create(Eventloop eventloop, Executor executor, Path containersDir,
			String incomingFilename, String outgoingFileName) {
		return new FsKeyExchanger(eventloop, executor, containersDir, incomingFilename, outgoingFileName);
	}

	public static FsKeyExchanger create(Eventloop eventloop, Executor executor, Path containersDir, boolean forApp) {
		return forApp ?
				new FsKeyExchanger(eventloop, executor, containersDir, DEFAULT_EXPECTED_KEYS_FILE, DEFAULT_ACTUAL_KEYS_FILE) :
				new FsKeyExchanger(eventloop, executor, containersDir, DEFAULT_ACTUAL_KEYS_FILE, DEFAULT_EXPECTED_KEYS_FILE);
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
	public Promise<Map<String, PrivKey>> receiveKeys() {
		return Promise.ofBlockingCallable(executor, () -> {
			try {
				Map<String, PrivKey> privKeys = new HashMap<>();
				List<String> lines = Files.readAllLines(incoming);
				for (String line : lines) {
					if (line.trim().isEmpty()) {
						logger.trace("Empty line found in place of a private key, skipping");
					}
					String[] parts = line.split(":");
					if (parts.length != 2) {
						logger.warn("Corrupted line '{}', should be formatted as 'id:privateKey'", line);
						continue;
					}
					if (privKeys.containsKey(parts[0])) {
						logger.warn("Duplicate id '{}' found", parts[0]);
						continue;
					}
					try {
						privKeys.put(parts[0], PrivKey.fromString(parts[1]));
					} catch (ParseException ignored) {
						logger.warn("Could not parse a private key from line {}", line);
					}
				}
				return privKeys;
			} catch (IOException e) {
				logger.error("Failed to receive keys", e);
				throw new UncheckedException(e);
			}
		});
	}

	@Override
	public Promise<Void> sendKeys(Map<String, PrivKey> keys) {
		return Promise.ofBlockingRunnable(executor, () -> {
			try {
				Path temp = outgoing.resolveSibling("tmp_" + outgoing.getFileName());
				Files.write(temp, keys.entrySet()
						.stream()
						.map(entry -> entry.getKey() + ":" + entry.getValue().asString())
						.collect(toSet()));
				Files.move(temp, outgoing, ATOMIC_MOVE, REPLACE_EXISTING);
			} catch (IOException e) {
				logger.error("Failed to send keys", e);
				throw new UncheckedException(e);
			}
		});
	}
}
