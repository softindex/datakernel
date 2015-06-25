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

package io.datakernel.file;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;

import java.nio.file.CopyOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

public interface FileSystem {
	<T extends File> void open(Path path, OpenOption[] openOptions, ResultCallback<T> callback);

	void delete(final Path path, CompletionCallback callback);

	void move(final Path source, final Path target, final CopyOption[] options, CompletionCallback callback);

	void createDirectory(final Path dir, final FileAttribute<?>[] attrs, CompletionCallback callback);

	void createDirectories(final Path dir, final FileAttribute<?>[] attrs, CompletionCallback callback);

	void readFile(Path path, final ResultCallback<ByteBuf> callback);

	void createNewAndWriteFile(Path path, final ByteBuf buf, final CompletionCallback callback);
}
