package io.datakernel.aio.file.service;

import sun.nio.ch.FileChannelImpl;
import sun.nio.ch.IOUtil;

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;

import static io.datakernel.util.Preconditions.checkArgument;

public final class FileDescriptorResolver {
	private static final Field fdDescriptorField;

	static {
		try {
			Class<FileChannelImpl> channelClass = FileChannelImpl.class;
			fdDescriptorField = channelClass.getDeclaredField("fd");
			fdDescriptorField.setAccessible(true);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		}

	}

	public static Integer getFd(FileChannel channel) throws IllegalAccessException {
		checkArgument(channel instanceof FileChannelImpl, "Not a descriptor-backed file channel");
		FileDescriptor fileDescriptor = (FileDescriptor) fdDescriptorField.get(channel);
		return IOUtil.fdVal(fileDescriptor);
	}
}
