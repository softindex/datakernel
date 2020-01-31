package io.global.launchers;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;

public final class Utils {

	public static SSLContext getSslContext(List<Path> pemFiles, Path privateKeyPemFile) throws GeneralSecurityException, IOException {
		SSLContext context = SSLContext.getInstance("TLS");

		KeyStore keystore = KeyStore.getInstance("JKS");
		keystore.load(null);

		Key key = readPrivateKey(privateKeyPemFile);
		List<Certificate> certs = new ArrayList<>();
		for (int i = 0; i < pemFiles.size(); i++) {
			Certificate cert = readCertificate(pemFiles.get(i));
			keystore.setCertificateEntry("certificate_" + (i + 1), cert);
			certs.add(cert);
		}

		char[] pass = toHexString(randomBytes(8)).toCharArray();
		keystore.setKeyEntry("key", key, pass, certs.toArray(new Certificate[0]));

		KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		kmf.init(keystore, pass);

		context.init(kmf.getKeyManagers(), null, null);

		return context;
	}

	private static byte[] parseDERFromPEM(byte[] pem, String beginDelimiter, String endDelimiter) {
		String data = new String(pem);
		String[] tokens = data.split(beginDelimiter);
		tokens = tokens[1].split(endDelimiter);
		return Base64.getDecoder().decode(tokens[0].replaceAll("\n", ""));
	}

	private static RSAPrivateKey readPrivateKey(Path path) throws GeneralSecurityException, IOException {
		byte[] data = Files.readAllBytes(path);
		byte[] der = parseDERFromPEM(data, "-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----");
		return (RSAPrivateKey) KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(der));
	}

	private static Certificate readCertificate(Path path) throws GeneralSecurityException, IOException {
		byte[] data = Files.readAllBytes(path);
		byte[] der = parseDERFromPEM(data, "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----");
		return CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(der));
	}
}
