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

public final class SerializerUtils {

	public static int encodeZigZag32(int n) {
		return (n << 1) ^ (n >> 31);
	}

	public static long encodeZigZag64(long n) {
		return (n << 1) ^ (n >> 63);
	}

	public static int decodeZigZag32(int n) {
		return (n >>> 1) ^ -(n & 1);
	}

	public static long decodeZigZag64(long n) {
		return (n >>> 1) ^ -(n & 1);
	}

	public static int varint32Size(int value) {
		if ((value & 0xffffffff << 7) == 0) return 1;
		if ((value & 0xffffffff << 14) == 0) return 2;
		if ((value & 0xffffffff << 21) == 0) return 3;
		if ((value & 0xffffffff << 28) == 0) return 4;
		return 5;
	}

	public static int varint64Size(long value) {
		if ((value & 0xffffffffffffffffL << 7) == 0) return 1;
		if ((value & 0xffffffffffffffffL << 14) == 0) return 2;
		if ((value & 0xffffffffffffffffL << 21) == 0) return 3;
		if ((value & 0xffffffffffffffffL << 28) == 0) return 4;
		if ((value & 0xffffffffffffffffL << 35) == 0) return 5;
		if ((value & 0xffffffffffffffffL << 42) == 0) return 6;
		if ((value & 0xffffffffffffffffL << 49) == 0) return 7;
		if ((value & 0xffffffffffffffffL << 56) == 0) return 8;
		if ((value & 0xffffffffffffffffL << 63) == 0) return 9;
		return 10;
	}

	private SerializerUtils() {
	}
}
