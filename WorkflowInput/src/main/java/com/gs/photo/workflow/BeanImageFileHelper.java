package com.gs.photo.workflow;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;

@Component
public class BeanImageFileHelper implements IBeanImageFileHelper {

	protected static final Logger LOGGER = Logger.getLogger(BeanImageFileHelper.class);
	private static final int NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED = 4 * 1024 * 1024;

	@Override
	public void waitForCopyComplete(Path filePath) {
		long k = 0;
		File f = filePath.toFile();
		do {
			k = f.length();
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		} while (k != f.length());
	}

	@Override
	public String computeHashKey(Path filePath) throws IOException {
		try {
			FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
			ByteBuffer byteBuffer = ByteBuffer.allocate(NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED);
			fileChannel.read(byteBuffer);
			final String key = Hashing.sha512().newHasher().putBytes(byteBuffer.array()).hash().toString();
			byteBuffer.clear();
			byteBuffer = null;
			return key;
		} catch (IOException e) {
			LOGGER.error("unable to compute hash key for " + filePath, e);
			throw e;
		}
	}

	@Override
	public String getFullPathName(Path filePath) {
		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			List<NetworkInterface> niList = Collections.list(interfaces);
			String addresses = niList.stream().flatMap((ni) -> {
				List<InetAddress> addressesList = Collections.list(ni.getInetAddresses());
				String adressList = addressesList.stream().flatMap(
						(ia) -> !Strings.isNullOrEmpty(ia.getHostAddress()) ? Stream.of(ia.getHostAddress()) : null)
						.collect(Collectors.joining(","));
				return adressList.length() > 0 ? Stream.of(adressList) : null;
			}).collect(Collectors.joining(","));
			return "[" + addresses + "]@" + filePath.toAbsolutePath();
		} catch (SocketException e) {
			LOGGER.error("unable to getFullPathName for " + filePath, e);
			throw new RuntimeException("unable to getFullPathName for " + filePath, e);
		}
	}

}
