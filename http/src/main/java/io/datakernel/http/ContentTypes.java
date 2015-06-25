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
import io.datakernel.bytebuf.ByteBuf;

import static com.google.common.net.MediaType.*;
import static io.datakernel.util.ByteBufStrings.wrapUTF8;

public final class ContentTypes {
	public static final ImmutableMap<String, ByteBuf> extensionContentType;

	/* Content types */
	private static final ByteBuf CT_HTML = wrapUTF8(HTML_UTF_8.toString());
	private static final ByteBuf CT_XHTML = wrapUTF8(XHTML_UTF_8.toString());
	private static final ByteBuf CT_CSS = wrapUTF8(CSS_UTF_8.toString());
	private static final ByteBuf CT_JS = wrapUTF8(JAVASCRIPT_UTF_8.toString());

	private static final ByteBuf CT_JSON = wrapUTF8(JSON_UTF_8.toString());
	private static final ByteBuf CT_PROTOBUF = wrapUTF8(PROTOBUF.toString());
	private static final ByteBuf CT_CSV = wrapUTF8(CSV_UTF_8.toString());
	private static final ByteBuf CT_PLAIN = wrapUTF8(PLAIN_TEXT_UTF_8.toString());
	private static final ByteBuf CT_XML = wrapUTF8(APPLICATION_XML_UTF_8.toString());
	private static final ByteBuf CT_OCTET_STREAM = wrapUTF8(OCTET_STREAM.toString());
	private static final ByteBuf CT_BINARY = wrapUTF8(APPLICATION_BINARY.toString());
	private static final ByteBuf CT_ATOM = wrapUTF8(ATOM_UTF_8.toString());

	private static final ByteBuf CT_BMP = wrapUTF8(BMP.toString());
	private static final ByteBuf CT_JPEG = wrapUTF8(JPEG.toString());
	private static final ByteBuf CT_PNG = wrapUTF8(PNG.toString());
	private static final ByteBuf CT_GIF = wrapUTF8(GIF.toString());
	private static final ByteBuf CT_ICO = wrapUTF8(ICO.toString());
	private static final ByteBuf CT_CRW = wrapUTF8(CRW.toString());
	private static final ByteBuf CT_TIFF = wrapUTF8(TIFF.toString());
	private static final ByteBuf CT_PSD = wrapUTF8(PSD.toString());
	private static final ByteBuf CT_WEBP = wrapUTF8(WEBP.toString());
	private static final ByteBuf CT_SVG = wrapUTF8(SVG_UTF_8.toString());
	private static final ByteBuf CT_PDF = wrapUTF8(PDF.toString());
	private static final ByteBuf CT_POSTSCRIPT = wrapUTF8(POSTSCRIPT.toString());
	private static final ByteBuf CT_EPUB = wrapUTF8(EPUB.toString());

	private static final ByteBuf CT_MP4_AUDIO = wrapUTF8(MP4_AUDIO.toString());
	private static final ByteBuf CT_MPEG_AUDIO = wrapUTF8(MPEG_AUDIO.toString());
	private static final ByteBuf CT_OGG_AUDIO = wrapUTF8(OGG_AUDIO.toString());
	private static final ByteBuf CT_WEBM_AUDIO = wrapUTF8(WEBM_AUDIO.toString());

	private static final ByteBuf CT_MP4_VIDEO = wrapUTF8(MP4_VIDEO.toString());
	private static final ByteBuf CT_MPEG_VIDEO = wrapUTF8(MPEG_VIDEO.toString());
	private static final ByteBuf CT_OGG_VIDEO = wrapUTF8(OGG_VIDEO.toString());
	private static final ByteBuf CT_QUICKTIME = wrapUTF8(QUICKTIME.toString());
	private static final ByteBuf CT_WEBM_VIDEO = wrapUTF8(WEBM_VIDEO.toString());
	private static final ByteBuf CT_WMV = wrapUTF8(WMV.toString());

	private static final ByteBuf CT_ZIP = wrapUTF8(ZIP.toString());
	private static final ByteBuf CT_GZIP = wrapUTF8(GZIP.toString());
	private static final ByteBuf CT_BZIP2 = wrapUTF8(BZIP2.toString());
	private static final ByteBuf CT_TAR = wrapUTF8(TAR.toString());
	private static final ByteBuf CT_KEY_ARCHIVE = wrapUTF8(KEY_ARCHIVE.toString());

	private static final ByteBuf CT_WOFF = wrapUTF8(WOFF.toString());
	private static final ByteBuf CT_EOT = wrapUTF8(EOT.toString());
	private static final ByteBuf CT_SFNT = wrapUTF8(SFNT.toString());

	private static final ByteBuf CT_RDF = wrapUTF8(RDF_XML_UTF_8.toString());
	private static final ByteBuf CT_XRD = wrapUTF8(XRD_UTF_8.toString());

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
		extensionContentType = ImmutableMap.<String, ByteBuf>builder()
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
