package com.gs.photo.workflow.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.stereotype.Service;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Streams;
import com.google.common.io.Files;
import com.workflow.model.files.FileToProcess;

@Service
public class FileUtils {

    static String LOCAL_ADDRESSES;
    static {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            List<NetworkInterface> niList = Collections.list(interfaces);
            FileUtils.LOCAL_ADDRESSES = niList.stream()
                .flatMap((ni) -> {
                    List<InetAddress> addressesList = Collections.list(ni.getInetAddresses());
                    String adressList = addressesList.stream()
                        .flatMap(
                            (ia) -> !Strings.isNullOrEmpty(ia.getHostAddress()) ? Stream.of(ia.getHostAddress()) : null)
                        .collect(Collectors.joining(","));
                    return adressList.length() > 0 ? Stream.of(adressList) : null;
                })
                .collect(Collectors.joining(","));
        } catch (SocketException e) {
            throw new RuntimeException("unable to local network addresses ", e);
        }
    }

    public byte[] readFirstBytesOfFile(String filePath, String coordinates, int nbOfBytesToRead) throws IOException {
        byte[] retValue = new byte[nbOfBytesToRead];
        Nfs3 nfs3 = new Nfs3(coordinates, new CredentialUnix(0, 0, null), 3);
        Nfs3File file = new Nfs3File(nfs3, filePath);
        try (
            NfsFileInputStream inputStream = new NfsFileInputStream(file, nbOfBytesToRead)) {
            inputStream.read(retValue);
        } catch (IOException e) {
            throw e;
        }
        return retValue;
    }

    public long copyRemoteToLocal(String coordinates, String filePath, Path localPath, int bufferSize)
        throws IOException {
        String[] tokens = coordinates.split("\\:");
        String remoteHostName = tokens[0];
        String remoteFolder = tokens[1];
        long retValue = 0;

        InetAddress ip = InetAddress.getLocalHost();
        String localHostname = ip.getHostName();

        if (Objects.equal(remoteHostName.toLowerCase(), localHostname.toLowerCase())) {
            final File sourceFile = new File(remoteFolder + File.pathSeparator + filePath);
            Files.copy(sourceFile, localPath.toFile());
            retValue = sourceFile.length();
        } else {

            byte[] buffer = new byte[bufferSize];
            Nfs3 nfs3 = new Nfs3(coordinates, new CredentialUnix(0, 0, null), 3);
            Nfs3File file = new Nfs3File(nfs3, filePath);
            try (
                OutputStream os = new FileOutputStream(localPath.toFile());
                NfsFileInputStream inputStream = new NfsFileInputStream(file, bufferSize)) {
                int nbOfBytes = inputStream.read(buffer);
                while (nbOfBytes >= 0) {
                    retValue = retValue + nbOfBytes;
                    os.write(buffer, 0, nbOfBytes);
                    nbOfBytes = inputStream.read(buffer);
                }
            } catch (IOException e) {
                throw e;
            }
        }
        return retValue;
    }

    public byte[] readFirstBytesOfFile(FileToProcess file, int bufferSize) throws IOException {
        byte[] retValue = new byte[bufferSize];
        Nfs3 nfs3 = new Nfs3(file.getHost(), file.getPath(), new CredentialUnix(0, 0, null), 3);
        Nfs3File nfs3File = new Nfs3File(nfs3, file.getName());
        try (
            NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File, bufferSize)) {
            inputStream.read(retValue);
        } catch (IOException e) {
            throw e;
        }
        return retValue;
    }

    public long copyRemoteToLocal(FileToProcess file, OutputStream os, Integer bufferSize) throws IOException {
        long retValue = 0;
        String remoteHostName = file.getHost();
        String remoteFolder = file.getPath();
        InetAddress ip = InetAddress.getLocalHost();
        String localHostname = ip.getHostName();

        if (Objects.equal(remoteHostName.toLowerCase(), localHostname.toLowerCase())) {
            final File sourceFile = new File(remoteFolder);
            byte[] buffer = new byte[bufferSize];
            try (
                FileInputStream inputStream = new FileInputStream(sourceFile);) {
                int nbOfBytes = inputStream.read(buffer);
                while (nbOfBytes >= 0) {
                    retValue = retValue + nbOfBytes;
                    os.write(buffer, 0, nbOfBytes);
                    nbOfBytes = inputStream.read(buffer);
                }
            }
        } else {
            byte[] buffer = new byte[bufferSize];
            String mountPoint = remoteFolder.substring(0, remoteFolder.indexOf("/", 1));
            Nfs3 nfs3 = new Nfs3(remoteHostName, mountPoint, new CredentialUnix(0, 0, null), 3);
            Nfs3File nfs3File = new Nfs3File(nfs3,
                file.getPath()
                    .substring(mountPoint.length()));
            try (
                NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File, bufferSize)) {
                int nbOfBytes = inputStream.read(buffer);
                while (nbOfBytes >= 0) {
                    retValue = retValue + nbOfBytes;
                    os.write(buffer, 0, nbOfBytes);
                    nbOfBytes = inputStream.read(buffer);
                }
            } catch (IOException e) {
                throw e;
            }
        }

        return retValue;
    }

    public long copyRemoteToLocal(String coordinates, String filePath, OutputStream os, int bufferSize)
        throws IOException {
        String[] tokens = coordinates.split("\\:");
        String remoteHostName = tokens[0];
        String remoteFolder = tokens[1];
        long retValue = 0;

        InetAddress ip = InetAddress.getLocalHost();
        String localHostname = ip.getHostName();

        if (Objects.equal(remoteHostName.toLowerCase(), localHostname.toLowerCase())) {
            final File sourceFile = new File(remoteFolder + File.pathSeparator + filePath);
            Files.copy(sourceFile, os);
            retValue = sourceFile.length();
        } else {

            byte[] buffer = new byte[bufferSize];
            Nfs3 nfs3 = new Nfs3(coordinates, new CredentialUnix(0, 0, null), 3);
            Nfs3File file = new Nfs3File(nfs3, filePath);
            try (
                NfsFileInputStream inputStream = new NfsFileInputStream(file, bufferSize)) {
                int nbOfBytes = inputStream.read(buffer);
                while (nbOfBytes >= 0) {
                    retValue = retValue + nbOfBytes;
                    os.write(buffer, 0, nbOfBytes);
                    nbOfBytes = inputStream.read(buffer);
                }
            } catch (IOException e) {
                throw e;
            }
        }
        return retValue;
    }

    public String getFullPathName(Path filePath) {
        String retValue = new StringBuilder().append("[")
            .append(FileUtils.LOCAL_ADDRESSES)
            .append("]@")
            .append(filePath.toAbsolutePath())
            .toString();
        return retValue;
    }

    public String getHostPathName(Path filePath) {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            String localHostname = ip.getHostName();
            String retValue = new StringBuilder().append(localHostname)
                .append(":")
                .append(filePath.toAbsolutePath())
                .toString();
            return retValue;
        } catch (UnknownHostException e) {
            throw new RuntimeException("unable to local hostname ", e);
        }
    }

    public Stream<File> toStreamWithIterator(final Path folder, final String... extensions) {

        Iterator<File> retValue = new Iterator<File>() {

            @Override
            public boolean hasNext() { return false; }

            @Override
            public File next() { return null; }
        };

        return Streams.stream(retValue);
    }

    public Stream<File> toStream(final Path folder, final String... extensions) throws IOException {
        Stream<File> stream = java.nio.file.Files.walk(folder)
            .filter((f) -> {
                if (!java.nio.file.Files.isDirectory(f)) {
                    for (String e : extensions) {
                        if (f.getFileName()
                            .toString()
                            .endsWith(e)) { return true; }
                    }
                }
                return false;
            })
            .map((p) -> p.toFile());
        return stream;
    }

}
