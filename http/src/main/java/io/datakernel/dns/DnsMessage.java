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

package io.datakernel.dns;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

public final class DnsMessage {
	private byte[] message;
	private int idx = 0;

	private static final int RR_TTL_SIZE = 4;
	private static final int RR_NAME_SIZE = 2;
	private static final int RR_TYPE_SIZE = 2;
	private static final int RR_CLASS_SIZE = 2;
	private static final int TRANSACTION_ID_POSITION = 0;
	private static final int BYTE_WITH_ERROR_CODE_POSITION = 3;
	private static final int FLAGS_POSITION = 2;
	private static final int QUESTION_COUNT_POSITION = 4;
	private static final int ANSWER_COUNT_POSITION = 6;
	private static final int AUTHORITY_COUNT_POSITION = 8;
	private static final int ADDITIONAL_COUNT_POSITION = 10;
	private static final int HEADER_SIZE = 12;
	private static final int MAX_SIZE = 512;

	private static final byte A_RECORD_CODE = 1;
	private static final byte AAAA_RECORD_CODE = 28;
	private static final byte IN_CLASS_CODE = 1;

	public static final short A_RECORD_TYPE = 1;
	public static final short AAAA_RECORD_TYPE = 28;

	private final static Random random = new Random();

	public enum ResponseErrorCode {
		NO_ERROR,
		NO_DATA,
		FORMAT_ERROR,
		SERVER_FAILURE,
		NAME_ERROR,
		NOT_IMPLEMENTED,
		REFUSED,
		UNKNOWN
	}

	private DnsMessage(byte[] message) {
		this.message = message;
	}

	/**
	 * Creates a new query, generates request and wraps it to ByteBuf
	 *
	 * @param domainName domain name for request
	 * @param ipv6       if it is true, it is IPv6, else IPv4
	 * @return ByteBuf with DnsMessage
	 */
	public static ByteBuf newQuery(String domainName, boolean ipv6) {
		ByteBuf byteBuf = ByteBufPool.allocate(MAX_SIZE);
		DnsMessage m = new DnsMessage(byteBuf.array());
		m.generateRequest(domainName, ipv6);
		byteBuf.tail(m.idx);
		return byteBuf;
	}

	public static short getQuestionCount(byte[] message) throws DnsResponseParseException {
		try {
			byte questionCountFirst = message[QUESTION_COUNT_POSITION];
			byte questionCountSecond = message[QUESTION_COUNT_POSITION + 1];

			return convertTwoBytesToShort(questionCountFirst, questionCountSecond);
		} catch (RuntimeException e) {
			throw new DnsResponseParseException();
		}
	}

	public static short getAnswerRRCount(byte[] message) throws DnsResponseParseException {
		try {
			byte answerRRCountFirst = message[ANSWER_COUNT_POSITION];
			byte answerRRCountSecond = message[ANSWER_COUNT_POSITION + 1];

			return convertTwoBytesToShort(answerRRCountFirst, answerRRCountSecond);
		} catch (RuntimeException e) {
			throw new DnsResponseParseException();
		}
	}

	public static short getTransactionID(byte[] message) throws DnsResponseParseException {
		try {
			byte transactionIDFirst = message[TRANSACTION_ID_POSITION];
			byte transactionIDSecond = message[TRANSACTION_ID_POSITION + 1];

			return convertTwoBytesToShort(transactionIDFirst, transactionIDSecond);
		} catch (RuntimeException e) {
			throw new DnsResponseParseException();
		}
	}

	public static DnsResourceRecord getRecords(String domainName, byte[] response) throws DnsResponseParseException {
		int i = HEADER_SIZE - 1;
		int questionCount = getQuestionCount(response);
		int answerCount = getAnswerRRCount(response);
		ArrayList<InetAddress> ips = new ArrayList<>();
		int k = 0;
		int minTtl = Integer.MAX_VALUE;

		try {
			while (true) {
				++i;
				if (response[i] == 0) {
					if (++k == questionCount) {
						break;
					} else {
						i += RR_TYPE_SIZE + RR_CLASS_SIZE + 1; // skip type and class
					}
				}
			}

			i += RR_TYPE_SIZE + RR_CLASS_SIZE + 1; // skip type and class. i = position of answer beginning

			short type = 0;

			for (int j = 0; j < answerCount; ++j) {
				i += RR_NAME_SIZE; // skip name

				type = convertTwoBytesToShort(response[i], response[i + 1]);
				boolean ignoreRecord = type != A_RECORD_TYPE && type != AAAA_RECORD_TYPE;

				i += RR_TYPE_SIZE; // skip type
				i += RR_CLASS_SIZE; // skip class

				if (!ignoreRecord) {
					int ttl = convertFourBytesToInt(response, i);
					minTtl = Math.min(ttl, minTtl);
				}

				i += RR_TTL_SIZE;

				// fetch length
				byte lengthFirst = response[i++];
				byte lengthSecond = response[i++];
				short length = convertTwoBytesToShort(lengthFirst, lengthSecond);

				if (!ignoreRecord) {
					ips.add(convertBytesToInetAddress(domainName, response, i, length));
				}

				i += length;
			}

			InetAddress[] ipsArray = ips.toArray(new InetAddress[ips.size()]);

			return DnsResourceRecord.of(ipsArray, minTtl, type);
		} catch (RuntimeException e) {
			throw new DnsResponseParseException();
		}
	}

	private static InetAddress convertBytesToInetAddress(String domainName, byte[] response, int startIdx, int length) {
		byte ipBytes[] = new byte[length];
		System.arraycopy(response, startIdx, ipBytes, 0, length);
		try {
			return InetAddress.getByAddress(domainName, ipBytes);
		} catch (UnknownHostException ignored) {
			// length is guaranteed to be legal
		}
		return null;
	}

	private void setHeader() {
		short id = (short) random.nextInt(Short.MAX_VALUE + 1);

		byte idFirstByte = (byte) (id >> 8);
		byte idSecondByte = (byte) id;

		byte flagFirst = 1;
		byte flagSecond = 0;

		// transaction id
		message[TRANSACTION_ID_POSITION] = idFirstByte;
		message[TRANSACTION_ID_POSITION + 1] = idSecondByte;

		// flags
		message[FLAGS_POSITION] = flagFirst;
		message[FLAGS_POSITION + 1] = flagSecond;

		// 1 question
		message[QUESTION_COUNT_POSITION] = 0;
		message[QUESTION_COUNT_POSITION + 1] = 1;

		// 0 answer RR
		message[ANSWER_COUNT_POSITION] = 0;
		message[ANSWER_COUNT_POSITION + 1] = 0;

		// 0 authority RR
		message[AUTHORITY_COUNT_POSITION] = 0;
		message[AUTHORITY_COUNT_POSITION + 1] = 0;

		// 0 additional RR
		message[ADDITIONAL_COUNT_POSITION] = 0;
		message[ADDITIONAL_COUNT_POSITION + 1] = 0;

		idx = HEADER_SIZE;
	}

	private void setQName(String domainName) {
		int i = 0;
		int j = idx;
		byte componentSize = 0;

		while (i != domainName.length()) {
			if (domainName.charAt(i) != '.') {
				++componentSize;
			} else {
				j = copyComponent(i, j, componentSize, domainName);
				componentSize = 0;
			}
			++i;
		}

		j = copyComponent(i, j, componentSize, domainName);

		message[j++] = (byte) 0; // terminator

		idx = j;
	}

	public static String getQueryDomainName(byte[] response) throws DnsResponseParseException {
		try {
			int i = HEADER_SIZE;
			StringBuilder sb = new StringBuilder();

			while (response[i] != 0) {
				int componentLength = response[i++];

				for (int j = 0; j < componentLength; ++j) {
					sb.append((char) response[i++]);
				}

				if (response[i + 1] != 0) {
					sb.append(".");
				}
			}

			return sb.toString();
		} catch (RuntimeException e) {
			throw new DnsResponseParseException();
		}
	}

	private static ResponseErrorCode getErrorCode(byte[] response) {
		byte bitMask = 0b00001111;
		byte byteWithErrorCode = response[BYTE_WITH_ERROR_CODE_POSITION];
		byte errorCode = (byte) (byteWithErrorCode & bitMask); // take four least significant bits

		if (errorCode > ResponseErrorCode.values().length - 2) {
			return ResponseErrorCode.UNKNOWN;
		}

		return ResponseErrorCode.values()[errorCode];
	}

	public static DnsQueryResult getQueryResult(byte[] response) throws DnsResponseParseException {
		ResponseErrorCode errorCode = getErrorCode(response);
		String domainName = getQueryDomainName(response);
		DnsQueryResult result;

		if (errorCode == ResponseErrorCode.NO_ERROR) {
			DnsResourceRecord record = getRecords(domainName, response);
			if (record.hasData())
				result = DnsQueryResult.successfulQuery(domainName, record.getIps(), record.getMinTtl(), record.getType());
			else
				result = DnsQueryResult.failedQuery(domainName, ResponseErrorCode.NO_DATA);
		} else {
			result = DnsQueryResult.failedQuery(domainName, errorCode);
		}

		return result;
	}

	private int copyComponent(int i, int j, byte componentSize, String domainName) {
		message[j++] = componentSize;

		for (int k = i - componentSize; k < i; ++k) {
			message[j++] = (byte) domainName.charAt(k);
		}

		return j;
	}

	private void setQType(boolean ipv6) {
		if (ipv6) {
			message[idx++] = (byte) 0;
			message[idx++] = AAAA_RECORD_CODE;
		} else {
			message[idx++] = (byte) 0;
			message[idx++] = A_RECORD_CODE;
		}
	}

	private void setQClass() {
		message[idx++] = (byte) 0;
		message[idx++] = IN_CLASS_CODE;
	}

	private void generateRequest(String domainName, boolean ipv6) {
		setHeader();
		setQName(domainName);
		setQType(ipv6);
		setQClass();
	}

	/* Utility methods */
	private static int convertFourBytesToInt(byte[] response, int start) {
		int num = 0;
		num |= (0xFF & response[start]) << 24;
		num |= (0xFF & response[start + 1]) << 16;
		num |= (0xFF & response[start + 2]) << 8;
		num |= 0xFF & response[start + 3];
		return num;
	}

	private static short convertTwoBytesToShort(byte a, byte b) {
		return (short) (((a & 0xFF) << 8) | (b & 0xFF));
	}
}
