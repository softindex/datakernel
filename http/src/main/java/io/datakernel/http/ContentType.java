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

package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.http.CharsetUtils.getCharset;
import static io.datakernel.http.HttpUtils.skipSpaces;
import static io.datakernel.util.ByteBufStrings.*;

@SuppressWarnings("unused")
public class ContentType {
	private static class ContentTypeExt extends ContentType {
		private double q = DEFAULT_Q;
		private Charset encoding = DEFAULT_ENCODING;

		private ContentTypeExt(ContentType ct, double q, Charset encoding) {
			super(ct.bytes);
			this.q = q;
			this.encoding = encoding;
		}

		private ContentTypeExt(byte[] name) {
			super(name);
		}

		public ContentTypeExt(ContentType ct, double q) {
			super(ct.bytes);
		}

		public ContentTypeExt(ContentType ct, Charset encoding) {
			super(ct.bytes);
			this.encoding = encoding;
		}

		@Override
		int estimateSize() {
			return super.estimateSize() + 60;
		}

		@Override
		public double getQ() {
			return q;
		}

		@Override
		public Charset getCharsetEncoding() {
			return encoding;
		}

		@Override
		public ContentType specify(double q, Charset encoding) {
			this.q = q;
			this.encoding = encoding;
			return this;
		}

		@Override
		public ContentType specify(double q) {
			this.q = q;
			return this;
		}

		@Override
		public ContentType specify(Charset encoding) {
			this.encoding = encoding;
			return this;
		}

		@Override
		public String toString() {
			return super.toString()
					+ (q != DEFAULT_Q ? ";q=" + q : "")
					+ (encoding != DEFAULT_ENCODING ? ";encoding=" + encoding : "");
		}
	}

	private static final double DEFAULT_Q = 1.0;
	private static final Charset DEFAULT_ENCODING = Charset.forName("ISO-8859-1");

	private final static int SLOTS = 1024;
	private final static int PROBINGS = 2;

	private static final ContentType[] types = new ContentType[SLOTS];
	private static final Map<String, ContentType> extensions = new HashMap<>();

	public static final ContentType ANY = register("*/*");
	public static final ContentType ANY_APPLICATION = register("application/*");
	public static final ContentType ANY_TEXT = register("text/*");
	public static final ContentType ANY_IMAGE = register("image/*");
	public static final ContentType ANY_AUDIO = register("audio/*");
	public static final ContentType ANY_VIDEO = register("video/*");

	public static final ContentType ATOM = register("application/atom+xml").addExtensions("atom");
	public static final ContentType EDI_X12 = register("application/EDI-X12");
	public static final ContentType EDI_EDIFACT = register("application/EDIFACT");
	public static final ContentType JSON = register("application/json").addExtensions("json");
	public static final ContentType JAVASCRIPT_APP = register("application/javascript");
	public static final ContentType OCTET_STREAM = register("application/octet-stream").addExtensions("com", "exe", "bin");
	public static final ContentType ZIP = register("application/zip").addExtensions("zip", "zipx");
	public static final ContentType GZIP = register("application/gzip").addExtensions("gzip", "gz");
	public static final ContentType BZIP2 = register("application/x-bzip2").addExtensions("bz2");
	public static final ContentType FLASH = register("application/x-shockwave-flash");
	public static final ContentType TEX = register("application/x-tex");
	public static final ContentType PDF = register("application/pdf").addExtensions("pdf");
	public static final ContentType OGG_APP = register("application/ogg").addExtensions("ogg");
	public static final ContentType POSTSCRIPT = register("application/postscript").addExtensions("ai");
	public static final ContentType BINARY = register("application/binary");
	public static final ContentType TAR = register("application/x-tar").addExtensions("tar");
	public static final ContentType KEY_ARCHIVE = register("application/pkcs12").addExtensions("p12", "pfx");
	public static final ContentType PROTOBUF = register("application/protobuf").addExtensions("proto");
	public static final ContentType EPUB = register("application/epub+zip").addExtensions("epub");
	public static final ContentType RDF = register("application/rdf+xml").addExtensions("rdf");
	public static final ContentType XRD = register("application/xrd+xml").addExtensions("xrd");
	public static final ContentType JAVA_ARCHIVE = register("application/java-archive").addExtensions("jar", "war", "ear");
	public static final ContentType WOFF = register("application/font-woff").addExtensions("woff");
	public static final ContentType EOT = register("application/vnd.ms-fontobject").addExtensions("eot");
	public static final ContentType SFNT = register("application/font-sfnt").addExtensions("sfnt");
	public static final ContentType XML_APP = register("application/xml");
	public static final ContentType XHTML_APP = register("application/xhtml+xml");

	public static final ContentType CSS = register("text/css").addExtensions("css");
	public static final ContentType CSV = register("text/csv").addExtensions("csv");
	public static final ContentType HTML = register("text/html").addExtensions("html", "htm");
	public static final ContentType PLAIN_TEXT = register("text/plain").addExtensions("txt");
	public static final ContentType RTF = register("text/rtf").addExtensions("rtf");
	public static final ContentType XML = register("text/xml").addExtensions("xml");
	public static final ContentType XHTML = register("text/xhtml+xml").addExtensions("xhtml");
	public static final ContentType JAVASCRIPT_TXT = register("text/javascript").addExtensions("js");

	public static final ContentType BMP = register("image/bmp").addExtensions("bmp");
	public static final ContentType ICO = register("image/vnd.microsoft.icon").addExtensions("ico");
	public static final ContentType CRW = register("image/x-canon-crw").addExtensions("crw");
	public static final ContentType PSD = register("image/vnd.adobe.photoshop").addExtensions("psd");
	public static final ContentType WEBP = register("image/webp").addExtensions("webp");
	public static final ContentType GIF = register("image/gif").addExtensions("gif");
	public static final ContentType JPEG = register("image/jpeg").addExtensions("jpeg", "jpg");
	public static final ContentType PNG = register("image/png").addExtensions("png");
	public static final ContentType SVG = register("image/svg+xml").addExtensions(").addExtensions(g");
	public static final ContentType TIFF = register("image/tiff").addExtensions("tiff", "tif");

	public static final ContentType OGG_AUDIO = register("audio/ogg").addExtensions("oga");
	public static final ContentType MP4_AUDIO = register("audio/mp4").addExtensions("mp4a");
	public static final ContentType MPEG_AUDIO = register("audio/mpeg").addExtensions("mpga");
	public static final ContentType WEBM_AUDIO = register("audio/webm").addExtensions("weba");
	public static final ContentType FLAC = register("audio/x-flac").addExtensions("flac");
	public static final ContentType MP3 = register("audio/mp3").addExtensions("mp3");
	public static final ContentType AAC = register("audio/aac").addExtensions("aac");
	public static final ContentType WMA = register("audio/x-ms-wma").addExtensions("wma");
	public static final ContentType REALAUDIO = register("audio/vnd.rn-realaudio").addExtensions("rm");
	public static final ContentType WAVE = register("audio/vnd.wave").addExtensions("wave", "wav");

	public static final ContentType MPEG = register("video/mpeg").addExtensions("mpeg", "mpg");
	public static final ContentType MP4 = register("video/mp4").addExtensions("mp4");
	public static final ContentType QUICKTIME = register("video/quicktime").addExtensions("mov", "moov", "qt", "qtvr");
	public static final ContentType OGG_VIDEO = register("video/ogg").addExtensions("ogv");
	public static final ContentType WEBM = register("video/webm").addExtensions("webm");
	public static final ContentType WMV = register("video/x-ms-wmv").addExtensions("wmv");
	public static final ContentType FLV = register("video/x-flv").addExtensions("flv");
	public static final ContentType AVI = register("video/avi").addExtensions("avi");

	private final byte[] bytes;

	// internal creators
	private static ContentType register(String name) {
		byte[] bytes = encodeAscii(name);
		ContentType type = new ContentType(bytes);
		int hash = ByteBufStrings.hashCodeLowerCaseAscii(bytes);
		for (int i = 0; i < PROBINGS; i++) {
			int slot = (hash + i) & (types.length - 1);
			if (types[slot] == null) {
				types[slot] = type;
				return type;
			}
		}
		throw new IllegalArgumentException("ContentType hash collision, try to increase TYPES size");
	}

	private ContentType addExtensions(String... extensions) {
		for (String e : extensions) {
			ContentType.extensions.put(e, this);
		}
		return this;
	}

	private ContentType(byte[] bytes) {
		this.bytes = bytes;
	}

	// creators and builders
	public static ContentType getByName(String name) {
		byte[] bytes = encodeAscii(name);
		int slot = findSlot(bytes, 0, bytes.length);
		if (slot == -1) {
			return new ContentTypeExt(bytes);
		} else {
			return types[slot];
		}
	}

	public static ContentType getByName(byte[] bytes, int offset, int size) {
		int slot = findSlot(bytes, offset, size);
		if (slot == -1) {
			// instead of new String()
			byte[] name = new byte[size];
			System.arraycopy(bytes, offset, name, 0, size);
			return new ContentTypeExt(name);
		} else {
			return types[slot];
		}
	}

	public ContentType specify(double q, Charset encoding) {
		return new ContentTypeExt(this, q, encoding);
	}

	public ContentType specify(double q) {
		return new ContentTypeExt(this, q);
	}

	public ContentType specify(Charset encoding) {
		return new ContentTypeExt(this, encoding);
	}

	private static int findSlot(byte[] bytes, int offset, int size) {
		int hash = ByteBufStrings.hashCodeLowerCaseAscii(bytes, offset, size);
		for (int i = 0; i < PROBINGS; i++) {
			int slot = (hash + i) & (types.length - 1);
			if (types[slot] != null) {
				if (equalsLowerCaseAscii(types[slot].bytes, bytes, offset, size)) {
					return slot;
				}
			}
		}
		return -1;
	}

	int estimateSize() {
		return bytes.length;
	}

	// getters
	public static ContentType getByExt(String ext) {
		return extensions.get(ext);
	}

	public double getQ() {
		return DEFAULT_Q;
	}

	public Charset getCharsetEncoding() {
		return DEFAULT_ENCODING;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes);
	}

	// parse & render tools
	static void parse(String contentTypes, List<ContentType> list) {
		byte[] bytes = encodeAscii(contentTypes);
		parse(bytes, 0, bytes.length, list);
	}

	static void parse(ByteBuf buf, List<ContentType> list) {
		parse(buf.array(), buf.position(), buf.limit(), list);
	}

	static void parse(byte[] bytes, int pos, int end, List<ContentType> list) {
		while (pos < end) {
			pos = skipSpaces(bytes, pos, end);
			int start = pos;
			while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
				pos++;
			}
			ContentType type = getByName(bytes, start, pos - start);
			double q = DEFAULT_Q;
			Charset charset = DEFAULT_ENCODING;
			while (pos < end && bytes[pos] != ',') {
				if (bytes[pos] == ';') {
					pos++;
					pos = skipSpaces(bytes, pos, end);
					start = pos;
					while (pos < end && bytes[pos] != '=') {
						pos++;
					}

					if (equalsLowerCaseAscii(encodeAscii("q"), bytes, start, pos - start)) {
						pos++;
						int qStart = pos;
						while (bytes[pos] != '.') {
							pos++;
						}
						int integer = ByteBufStrings.decodeDecimal(bytes, qStart, pos - qStart);
						pos++;
						qStart = pos;
						while (pos < end && !(bytes[pos] == ',' || bytes[pos] == ';')) {
							pos++;
						}
						double fraction = ByteBufStrings.decodeDecimal(bytes, qStart, pos - qStart);
						while (fraction > 1.0) {
							fraction /= 10.0;
						}
						q = integer + fraction;
					} else if (equalsLowerCaseAscii(encodeAscii("charset"), bytes, start, pos - start)) {
						pos++;
						start = pos;
						while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
							pos++;
						}
						int keyEnd = pos;
						charset = getCharset(bytes, start, keyEnd);
					} else {
						// skipping unknown parameter
						while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
							pos++;
						}
					}

				}
			}
			if (q != DEFAULT_Q || !charset.equals(DEFAULT_ENCODING)) {
				type = type.specify(q, charset);
			}
			pos++;
			list.add(type);
		}

	}

	static void render(List<ContentType> types, ByteBuf buf) {
		int pos = render(types, buf.array(), buf.position());
		buf.position(pos);
	}

	static int render(List<ContentType> types, byte[] bytes, int pos) {
		for (int i = 0; i < types.size(); i++) {
			byte[] type = types.get(i).bytes;
			System.arraycopy(type, 0, bytes, pos, type.length);
			pos += type.length;

			if (types.get(i).getQ() != DEFAULT_Q) {
				encodeAscii(bytes, pos, ";q=");
				pos += 3;
				double q = types.get(i).getQ();
				int n = (int) q;
				pos += encodeDecimal(bytes, pos, n);
				bytes[pos++] = '.';
				while (q - (int) q > 0.000001) {
					q *= 10;
				}
				pos += encodeDecimal(bytes, pos, (int) q);
			}

			if (types.get(i).getCharsetEncoding() != DEFAULT_ENCODING) {
				encodeAscii(bytes, pos, ";charset=");
				pos += 9;
				byte[] chName = encodeAscii(types.get(i).getCharsetEncoding().name());
				toLowerCaseAscii(chName);
				System.arraycopy(chName, 0, bytes, pos, chName.length);
				pos += chName.length;
			}

			if (i != types.size() - 1) {
				bytes[pos++] = ',';
			}
		}
		return pos;
	}
}