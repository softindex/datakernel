package io.datakernel.loader;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class ResourcesNameLoadingService implements EventloopService {
    private final Eventloop eventloop;
    private final ClassLoader loader;
    private final ExecutorService executorService;

    private Set<String> names;
    private String resourcePath;

    private ResourcesNameLoadingService(Eventloop eventloop, ExecutorService executorService, ClassLoader loader,
                                        String resourcePath) {
        this.eventloop = eventloop;
        this.executorService = executorService;
        this.loader = loader;
        this.resourcePath = resourcePath;
    }

    public static ResourcesNameLoadingService create(Eventloop eventloop, ExecutorService executorService,
                                                     ClassLoader classLoader, String resourcePath) {
        return new ResourcesNameLoadingService(eventloop, executorService, classLoader, resourcePath);
    }

    public static ResourcesNameLoadingService createRoot(Eventloop eventloop, ExecutorService executorService,
                                                         ClassLoader classLoader) {
        return new ResourcesNameLoadingService(eventloop, executorService, classLoader, "");
    }

    @Override
    public Eventloop getEventloop() {
        return eventloop;
    }

    @Override
    public CompletionStage<Void> start() {
        return eventloop.callConcurrently(executorService, () -> {
            final Set<String> fileNames = new HashSet<>();
            final InputStream in = loader.getResourceAsStream(resourcePath);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    fileNames.add(Paths.get(resourcePath).resolve(Paths.get(line)).toString());
                }
                return fileNames;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(strings -> names = strings);
    }

    @Override
    public CompletionStage<Void> stop() {
        return Stages.of(null);
    }

    public Set<String> getNames() {
        return names;
    }

    public boolean contains(String value) {
        return names.contains(value);
    }
}