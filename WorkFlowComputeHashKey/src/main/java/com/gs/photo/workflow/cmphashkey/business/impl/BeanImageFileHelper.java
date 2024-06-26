package com.gs.photo.workflow.cmphashkey.business.impl;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.gs.photo.workflow.cmphashkey.business.IBeanImageFileHelper;
import com.gs.photo.workflow.cmphashkey.ports.IFileUtils;

@Component
public class BeanImageFileHelper implements IBeanImageFileHelper {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BeanImageFileHelper.class);

    @Autowired
    protected IFileUtils          fileUtils;

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
