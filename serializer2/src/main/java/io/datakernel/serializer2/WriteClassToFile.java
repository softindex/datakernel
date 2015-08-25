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

package io.datakernel.serializer2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class WriteClassToFile {
	private static final Logger logger = LoggerFactory.getLogger(WriteClassToFile.class);
	private static String targetPath;

	public static void setWriteClassPath(String targetWritePath) {
		if (!targetWritePath.endsWith("/"))
			targetWritePath = targetWritePath + "/";
		targetPath = targetWritePath;
	}

	public static void write(String className, byte[] bytes) {
		if (targetPath == null)
			return;
		String filename = className.replace(".", File.separator);
		int lastIndexOf = filename.lastIndexOf(File.separator);
		String parent = "";
		if (lastIndexOf != -1) {
			parent = filename.substring(0, lastIndexOf);
			filename = filename.substring(lastIndexOf + 1);
		}
		filename = filename + ".class";
		File parentDir = new File(targetPath + parent);
		if (!parentDir.exists()) {
			if (!parentDir.mkdirs()) {
				logger.error("Error make dirs of {}", parentDir.getAbsolutePath());
				return;
			}
		}

		File outputFile = new File(parentDir, filename);
		try (DataOutputStream out = new DataOutputStream(new FileOutputStream(outputFile))) {
			out.write(bytes);
			out.flush();
		} catch (IOException e) {
			logger.error("IOException while write class " + className + " to " + filename, e);
		}
	}
}
