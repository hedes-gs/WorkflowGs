package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;
import com.google.common.base.Strings;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.gs.photo.workflow.IBeanImageFileHelper;

@Component
public class BeanImageFileHelper implements IBeanImageFileHelper {

    protected static final Logger LOGGER                               = LoggerFactory
        .getLogger(BeanImageFileHelper.class);
    private static final int      NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED = 4 * 1024 * 1024;

    @Override
    public byte[] readFirstBytesOfFile(String filePath, String coordinates) throws IOException {
        BeanImageFileHelper.LOGGER.debug(" readFirstBytesOfFile of {}, {}", filePath, coordinates);
        byte[] retValue = new byte[BeanImageFileHelper.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED];
        Nfs3 nfs3 = new Nfs3(coordinates, new CredentialUnix(0, 0, null), 3);
        Nfs3File file = new Nfs3File(nfs3, filePath);
        try (
            NfsFileInputStream inputStream = new NfsFileInputStream(file,
                BeanImageFileHelper.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED)) {
            inputStream.read(retValue);
        } catch (IOException e) {
            BeanImageFileHelper.LOGGER.error("unable to compute hash key for " + filePath, e);
            throw e;
        }
        return retValue;
    }

    @Override
    public String computeHashKey(byte[] byteBuffer) {
        HashFunction hf = Hashing.murmur3_128();
        hf.hashBytes(byteBuffer)
            .toString();
        final String key = hf.hashBytes(byteBuffer)
            .toString();
        return key;
    }

    @Override
    public String getFullPathName(Path filePath) {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            List<NetworkInterface> niList = Collections.list(interfaces);
            String addresses = niList.stream()
                .flatMap((ni) -> {
                    List<InetAddress> addressesList = Collections.list(ni.getInetAddresses());
                    String adressList = addressesList.stream()
                        .flatMap(
                            (ia) -> !Strings.isNullOrEmpty(ia.getHostAddress()) ? Stream.of(ia.getHostAddress()) : null)
                        .collect(Collectors.joining(","));
                    return adressList.length() > 0 ? Stream.of(adressList) : null;
                })
                .collect(Collectors.joining(","));
            return "[" + addresses + "]@" + filePath.toAbsolutePath();
        } catch (SocketException e) {
            BeanImageFileHelper.LOGGER.error("unable to getFullPathName for " + filePath, e);
            throw new RuntimeException("unable to getFullPathName for " + filePath, e);
        }
    }

}
