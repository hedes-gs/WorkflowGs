package com.gs.photo.workflow.cmphashkey.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
import com.gs.photo.workflow.cmphashkey.IBeanImageFileHelper;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanImageFileHelper implements IBeanImageFileHelper {

    protected static final Logger LOGGER                               = LoggerFactory
        .getLogger(BeanImageFileHelper.class);
    private static final int      NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED = (int) (2.5 * 1024 * 1024);

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

    @Override
    public byte[] readFirstBytesOfFile(FileToProcess file) throws IOException {

        BeanImageFileHelper.LOGGER.info(" readFirstBytesOfFile of {}", file);
        byte[] retValue = new byte[BeanImageFileHelper.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED];
        String root = file.getRootForNfs();
        Nfs3 nfs3 = this.createNFS3client(file, root);
        Nfs3File nfs3File = new Nfs3File(nfs3, file.getPath());
        if (!file.isCompressedFile()) {
            try (
                NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File,
                    BeanImageFileHelper.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED)) {
                inputStream.read(retValue);

            } catch (IOException e) {
                BeanImageFileHelper.LOGGER.error(
                    "ERROR : unable to compute hash key for {} error is {} ",
                    file,
                    ExceptionUtils.getStackTrace(e));
                throw e;
            }
        } else {
            try (
                NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File,
                    BeanImageFileHelper.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED);
                ZipInputStream zis = new ZipInputStream(inputStream)) {
                ZipEntry entry = zis.getNextEntry();
                do {
                    if (FilenameUtils.isExtension(entry.getName(), "ARW")) {
                        zis.read(retValue);
                        break;
                    }
                } while ((entry = zis.getNextEntry()) != null);

            } catch (IOException e) {
                BeanImageFileHelper.LOGGER.error(
                    "ERROR : unable to compute hash key for {} error is {} ",
                    file,
                    ExceptionUtils.getStackTrace(e));
                throw e;
            }
        }
        return retValue;
    }

    protected Map<String, Nfs3> mapOfNfs3client = new ConcurrentHashMap<>();

    protected Nfs3 createNFS3client(FileToProcess file, String root) throws IOException {
        return this.mapOfNfs3client.computeIfAbsent(file.getHost() + "/root", (e) -> {
            try {
                BeanImageFileHelper.LOGGER.info("Creating a Nfs3 client {} , {}", file.getHost(), root);
                return new Nfs3(file.getHost(), root, new CredentialUnix(0, 0, null), 3);
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        });
    }

}
