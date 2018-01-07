package io.datakernel.loader;

import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class FileNamesLoadingService implements EventloopService {
    private final Eventloop eventloop;
    private final ExecutorService executorService;
    private final Path path;

    private Set<String> fileNames;

    private FileNamesLoadingService(Eventloop eventloop, ExecutorService executorService, Path path) {
        this.eventloop = eventloop;
        this.executorService = executorService;
        this.path = path;
    }

    public static FileNamesLoadingService create(Eventloop eventloop, ExecutorService executorService, Path path) {
        return new FileNamesLoadingService(eventloop, executorService, path);
    }

    @Override
    public Eventloop getEventloop() {
        return eventloop;
    }

    @Override
    public CompletionStage<Void> start() {
        return eventloop.callConcurrently(executorService, () -> {
            Set<String> names = new HashSet<>();
            try (Stream<Path> pathStream = Files.walk(path)) {
                pathStream
                        .filter(Files::isRegularFile)
                        .map(path1 -> path1.toFile().getName())
                        .forEach(names::add);
                return names;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(strings -> fileNames = strings);
    }

    @Override
    public CompletionStage<Void> stop() {
        return Stages.of(null);
    }

    public Set<String> getFileNames() {
        return fileNames;
    }

    public boolean contains(String value) {
        return fileNames.contains(value);
    }
}