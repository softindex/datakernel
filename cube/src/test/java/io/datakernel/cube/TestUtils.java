/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.cube;

import io.datakernel.cube.bean.TestPubRequest;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

public final class TestUtils {
	public static List<TestPubRequest> generatePubRequests(int numberOfTestRequests) {
		List<TestPubRequest> pubRequests = new ArrayList<>();

		for (int i = 0; i < numberOfTestRequests; ++i) {
			pubRequests.add(TestPubRequest.randomPubRequest());
		}

		return pubRequests;
	}

	public static long countAdvRequests(List<TestPubRequest> pubRequests) {
		long count = 0;

		for (TestPubRequest pubRequest : pubRequests) {
			count += pubRequest.advRequests.size();
		}

		return count;
	}

	public static void deleteRecursively(Path path) throws IOException {
		Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
					throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				if (exc == null) {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				} else {
					throw exc;
				}
			}
		});
	}

	public static void deleteRecursivelyQuietly(Path path) {
		try {
			deleteRecursively(path);
		} catch (IOException ignored) {
		}
	}
}
