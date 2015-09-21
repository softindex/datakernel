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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;

import java.nio.charset.Charset;

import static com.google.common.net.MediaType.*;

public final class ContentTypes {
	public static final ImmutableMap<String, byte[]> extensionContentType;

	private static byte[] wrap(MediaType type) {
		return type.toString().getBytes(Charset.forName("UTF-8"));
	}

	/* Content types */
	private static final byte[] CT_HTML = wrap(HTML_UTF_8);
	private static final byte[] CT_XHTML = wrap(XHTML_UTF_8);
	private static final byte[] CT_CSS = wrap(CSS_UTF_8);
	private static final byte[] CT_JS = wrap(JAVASCRIPT_UTF_8);

	private static final byte[] CT_JSON = wrap(JSON_UTF_8);
	private static final byte[] CT_PROTOBUF = wrap(PROTOBUF);
	private static final byte[] CT_CSV = wrap(CSV_UTF_8);
	private static final byte[] CT_PLAIN = wrap(PLAIN_TEXT_UTF_8);
	private static final byte[] CT_XML = wrap(APPLICATION_XML_UTF_8);
	private static final byte[] CT_OCTET_STREAM = wrap(OCTET_STREAM);
	private static final byte[] CT_BINARY = wrap(APPLICATION_BINARY);
	private static final byte[] CT_ATOM = wrap(ATOM_UTF_8);

	private static final byte[] CT_BMP = wrap(BMP);
	private static final byte[] CT_JPEG = wrap(JPEG);
	private static final byte[] CT_PNG = wrap(PNG);
	private static final byte[] CT_GIF = wrap(GIF);
	private static final byte[] CT_ICO = wrap(ICO);
	private static final byte[] CT_CRW = wrap(CRW);
	private static final byte[] CT_TIFF = wrap(TIFF);
	private static final byte[] CT_PSD = wrap(PSD);
	private static final byte[] CT_WEBP = wrap(WEBP);
	private static final byte[] CT_SVG = wrap(SVG_UTF_8);
	private static final byte[] CT_PDF = wrap(PDF);
	private static final byte[] CT_POSTSCRIPT = wrap(POSTSCRIPT);
	private static final byte[] CT_EPUB = wrap(EPUB);

	private static final byte[] CT_MP4_AUDIO = wrap(MP4_AUDIO);
	private static final byte[] CT_MPEG_AUDIO = wrap(MPEG_AUDIO);
	private static final byte[] CT_OGG_AUDIO = wrap(OGG_AUDIO);
	private static final byte[] CT_WEBM_AUDIO = wrap(WEBM_AUDIO);

	private static final byte[] CT_MP4_VIDEO = wrap(MP4_VIDEO);
	private static final byte[] CT_MPEG_VIDEO = wrap(MPEG_VIDEO);
	private static final byte[] CT_OGG_VIDEO = wrap(OGG_VIDEO);
	private static final byte[] CT_QUICKTIME = wrap(QUICKTIME);
	private static final byte[] CT_WEBM_VIDEO = wrap(WEBM_VIDEO);
	private static final byte[] CT_WMV = wrap(WMV);

	private static final byte[] CT_ZIP = wrap(ZIP);
	private static final byte[] CT_GZIP = wrap(GZIP);
	private static final byte[] CT_BZIP2 = wrap(BZIP2);
	private static final byte[] CT_TAR = wrap(TAR);
	private static final byte[] CT_KEY_ARCHIVE = wrap(KEY_ARCHIVE);

	private static final byte[] CT_WOFF = wrap(WOFF);
	private static final byte[] CT_EOT = wrap(EOT);
	private static final byte[] CT_SFNT = wrap(SFNT);

	private static final byte[] CT_RDF = wrap(RDF_XML_UTF_8);
	private static final byte[] CT_XRD = wrap(XRD_UTF_8);

	/* File extensions */
	private static final String HTML_EXT = "html";
	private static final String XHTML_EXT = "xhtml";
	private static final String CSS_EXT = "css";
	private static final String JS_EXT = "js";

	private static final String JSON_EXT = "json";
	private static final String PROTOBUF_EXT = "proto";
	private static final String CSV_EXT = "csv";
	private static final String PLAIN_EXT = "txt";
	private static final String XML_EXT = "xml";
	private static final String OCTET_STREAM_EXT = "bin";
	private static final String BINARY_EXT = "";
	private static final String ATOM_EXT = "atom";

	private static final String BMP_EXT = "bmp";
	private static final String JPEG_EXT = "jpeg";
	private static final String JPG_EXT = "jpg";
	private static final String PNG_EXT = "png";
	private static final String GIF_EXT = "gif";
	private static final String ICO_EXT = "ico";
	private static final String CRW_EXT = "crw";
	private static final String TIFF_EXT = "tiff";
	private static final String TIF_EXT = "tif";
	private static final String PSD_EXT = "psd";
	private static final String WEBP_EXT = "webp";
	private static final String SVG_EXT = "svg";
	private static final String PDF_EXT = "pdf";
	private static final String POSTSCRIPT_EXT = "ai";
	private static final String EPUB_EXT = "epub";

	private static final String MP4_AUDIO_EXT = "mp4a";
	private static final String MPEG_AUDIO_EXT = "mpga";
	private static final String OGG_AUDIO_EXT = "oga";
	private static final String WEBM_AUDIO_EXT = "weba";

	private static final String MP4_VIDEO_EXT = "mp4";
	private static final String MPEG_VIDEO_EXT = "mpeg";
	private static final String OGG_VIDEO_EXT = "ogv";
	private static final String QUICKTIME_EXT = "qt";
	private static final String WEBM_VIDEO_EXT = "webm";
	private static final String WMV_EXT = "wmv";

	private static final String ZIP_EXT = "zip";
	private static final String ZIPX_EXT = "zipx";
	private static final String GZIP_EXT = "gz";
	private static final String BZIP2_EXT = "bz2";
	private static final String TAR_EXT = "tar";
	private static final String P12_EXT = "p12";
	private static final String PFX_EXT = "pfx";

	private static final String WOFF_EXT = "woff";
	private static final String EOT_EXT = "eot";
	private static final String SFNT_EXT = "sfnt";

	private static final String RDF_EXT = "rdf";
	private static final String XRD_EXT = "xrd";

	static {
		extensionContentType = ImmutableMap.<String, byte[]>builder()
				.put(HTML_EXT, CT_HTML)
				.put(XHTML_EXT, CT_XHTML)
				.put(CSS_EXT, CT_CSS)
				.put(JS_EXT, CT_JS)

				.put(JSON_EXT, CT_JSON)
				.put(PROTOBUF_EXT, CT_PROTOBUF)
				.put(CSV_EXT, CT_CSV)
				.put(PLAIN_EXT, CT_PLAIN)
				.put(XML_EXT, CT_XML)
				.put(OCTET_STREAM_EXT, CT_OCTET_STREAM)
				.put(BINARY_EXT, CT_BINARY)
				.put(ATOM_EXT, CT_ATOM)

				.put(JPEG_EXT, CT_JPEG)
				.put(BMP_EXT, CT_BMP)
				.put(PNG_EXT, CT_PNG)
				.put(JPG_EXT, CT_JPEG)
				.put(GIF_EXT, CT_GIF)
				.put(ICO_EXT, CT_ICO)
				.put(CRW_EXT, CT_CRW)
				.put(TIFF_EXT, CT_TIFF)
				.put(TIF_EXT, CT_TIFF)
				.put(PSD_EXT, CT_PSD)
				.put(WEBP_EXT, CT_WEBP)
				.put(SVG_EXT, CT_SVG)
				.put(PDF_EXT, CT_PDF)
				.put(POSTSCRIPT_EXT, CT_POSTSCRIPT)
				.put(EPUB_EXT, CT_EPUB)

				.put(MP4_AUDIO_EXT, CT_MP4_AUDIO)
				.put(MPEG_AUDIO_EXT, CT_MPEG_AUDIO)
				.put(OGG_AUDIO_EXT, CT_OGG_AUDIO)
				.put(WEBM_AUDIO_EXT, CT_WEBM_AUDIO)

				.put(MP4_VIDEO_EXT, CT_MP4_VIDEO)
				.put(MPEG_VIDEO_EXT, CT_MPEG_VIDEO)
				.put(OGG_VIDEO_EXT, CT_OGG_VIDEO)
				.put(QUICKTIME_EXT, CT_QUICKTIME)
				.put(WEBM_VIDEO_EXT, CT_WEBM_VIDEO)
				.put(WMV_EXT, CT_WMV)

				.put(ZIP_EXT, CT_ZIP)
				.put(ZIPX_EXT, CT_ZIP)
				.put(GZIP_EXT, CT_GZIP)
				.put(BZIP2_EXT, CT_BZIP2)
				.put(TAR_EXT, CT_TAR)
				.put(P12_EXT, CT_KEY_ARCHIVE)
				.put(PFX_EXT, CT_KEY_ARCHIVE)

				.put(WOFF_EXT, CT_WOFF)
				.put(EOT_EXT, CT_EOT)
				.put(SFNT_EXT, CT_SFNT)

				.put(RDF_EXT, CT_RDF)
				.put(XRD_EXT, CT_XRD)
				.build();
	}

}
