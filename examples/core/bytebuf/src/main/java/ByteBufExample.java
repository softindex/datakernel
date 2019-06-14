import io.datakernel.bytebuf.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ByteBufExample {
	private static void wrapForReading() {
		byte[] data = new byte[]{0, 1, 2, 3, 4, 5};
		ByteBuf byteBuf = ByteBuf.wrapForReading(data);

		while (byteBuf.canRead()) {
			System.out.println(byteBuf.readByte());
		}

		System.out.println();
	}

	private static void wrapForWriting() {
		byte[] data = new byte[6];
		ByteBuf byteBuf = ByteBuf.wrapForWriting(data);
		byte value = 0;
		while (byteBuf.canWrite()) {
			byteBuf.writeByte(value++);
		}

		System.out.println(Arrays.toString(byteBuf.getArray()));
		System.out.println();
	}

	private static void stringConversion() {
		String message = "Hello";
		ByteBuf byteBuf = ByteBuf.wrapForReading(message.getBytes(UTF_8));
		String unWrappedMessage = byteBuf.asString(UTF_8);
		System.out.println(unWrappedMessage);
		System.out.println();
	}

	private static void slicing() {
		byte[] data = new byte[]{0, 1, 2, 3, 4, 5};
		ByteBuf byteBuf = ByteBuf.wrap(data, 0, data.length);
		ByteBuf slice = byteBuf.slice(1, 3);
		System.out.println("Sliced byteBuf array: " + Arrays.toString(slice.asArray()));
		System.out.println();
	}

	private static void byteBufferConversion() {
		ByteBuf byteBuf = ByteBuf.wrap(new byte[20], 0, 0);
		ByteBuffer buffer = byteBuf.toWriteByteBuffer();
		buffer.put((byte) 1);
		buffer.put((byte) 2);
		buffer.put((byte) 3);
		byteBuf.ofWriteByteBuffer(buffer);
		System.out.println("Array of ByteBuf converted from ByteBuffer: " + Arrays.toString(byteBuf.asArray()));
		System.out.println();
	}

	public static void main(String[] args) {
		wrapForReading();
		wrapForWriting();
		stringConversion();
		slicing();
		byteBufferConversion();
	}
}
