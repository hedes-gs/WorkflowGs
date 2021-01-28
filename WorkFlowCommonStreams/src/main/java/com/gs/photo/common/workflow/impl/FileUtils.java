package com.gs.photo.common.workflow.impl;

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
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
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
@ConditionalOnClass(value = com.emc.ecs.nfsclient.nfs.io.Nfs3File.class)
public class FileUtils {

    private static final int    NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED = 4 * 1024 * 1024;

    protected static Logger     LOGGER                               = LoggerFactory.getLogger(FileUtils.class);
    protected Map<String, Nfs3> mapOfNfs3client                      = new ConcurrentHashMap<>();

    static String               LOCAL_ADDRESSES;
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
        Nfs3 nfs3 = new Nfs3(file.getHost(), file.getRootForNfs(), new CredentialUnix(0, 0, null), 3);
        Nfs3File nfs3File = new Nfs3File(nfs3, file.getPath());
        try (
            NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File, bufferSize)) {
            inputStream.read(retValue);
        } catch (IOException e) {
            throw e;
        }
        return retValue;
    }

    public byte[] readFirstBytesOfFile(FileToProcess file) throws IOException {
        FileUtils.LOGGER.debug(" readFirstBytesOfFile of {}", file);
        byte[] retValue = new byte[FileUtils.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED];
        String root = file.getRootForNfs();
        Nfs3 nfs3 = new Nfs3(file.getHost(), root, new CredentialUnix(0, 0, null), 3);
        Nfs3File nfs3File = new Nfs3File(nfs3, file.getPath());
        if (!file.isCompressedFile()) {
            try (
                NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File,
                    FileUtils.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED)) {
                inputStream.read(retValue);
            } catch (IOException e) {
                FileUtils.LOGGER.error(
                    "ERROR : unable to compute hash key for {} error is {} ",
                    file,
                    ExceptionUtils.getStackTrace(e));
                throw e;
            }
        } else {
            try (
                NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File,
                    FileUtils.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED);
                ZipInputStream zis = new ZipInputStream(inputStream)) {
                ZipEntry entry = zis.getNextEntry();
                do {
                    if (FilenameUtils.isExtension(entry.getName(), "ARW")) {
                        zis.read(retValue);
                        break;
                    }
                } while ((entry = zis.getNextEntry()) != null);

            } catch (IOException e) {
                FileUtils.LOGGER.error(
                    "ERROR : unable to compute hash key for {} error is {} ",
                    file,
                    ExceptionUtils.getStackTrace(e));
                throw e;
            }
        }
        return retValue;
    }

    public boolean deleteIfLocal(FileToProcess file, String root) throws IOException {
        InetAddress ip = InetAddress.getLocalHost();
        String remoteHostName = file.getHost();
        String remoteFolder = file.getPath();
        String localHostname = ip.getHostName();
        if (Objects.equal(remoteHostName.toLowerCase(), localHostname.toLowerCase())) {
            final File sourceFile = new File(root + "/" + remoteFolder);
            if (sourceFile.exists()) {
                return sourceFile.delete();
            } else {
                FileUtils.LOGGER.warn("[EVENT][{}] unable to delete local file, it does not exist", file.getImageId());
            }
        }
        return false;
    }

    public long copyRemoteToLocal(FileToProcess file, OutputStream os, Integer bufferSize, String root)
        throws IOException, MissingFileException {
        long retValue = 0;
        String remoteHostName = file.getHost();
        String remoteFolder = file.getPath();
        InetAddress ip = InetAddress.getLocalHost();
        String localHostname = ip.getHostName();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            if (Objects.equal(remoteHostName.toLowerCase(), localHostname.toLowerCase())) {
                final File sourceFile = new File(root + "/" + remoteFolder);
                if (sourceFile.exists()) {
                    FileUtils.LOGGER.info(
                        "[EVENT][{}] detected local host, using normal file copy - source file is {}",
                        file.getImageId(),
                        sourceFile);
                    byte[] buffer = new byte[bufferSize];
                    try (
                        FileInputStream inputStream = new FileInputStream(sourceFile);) {
                        int nbOfBytes = inputStream.read(buffer);
                        while (nbOfBytes >= 0) {
                            retValue = retValue + nbOfBytes;
                            os.write(buffer, 0, nbOfBytes);
                            nbOfBytes = inputStream.read(buffer);
                        }
                    } catch (IOException e) {
                        FileUtils.LOGGER.error(
                            "[EVENT][{}] Unexptected error when copy remote to local : {}",
                            file.getImageId(),
                            ExceptionUtils.getStackTrace(e));
                        throw e;
                    }
                } else {
                    FileUtils.LOGGER.error(
                        "[EVENT][{}] detected local host, using normal file copy - source file  {} does not exist. Maybe already processed",
                        file.getImageId(),
                        sourceFile);
                    throw new MissingFileException(file);
                }
            } else {
                FileUtils.LOGGER.info(
                    "[EVENT][{}] detected remote host, using ftp file copy - source file {}",
                    file.getImageId(),
                    file.getPath());
                byte[] buffer = new byte[bufferSize];
                String mountPoint = file.getRootForNfs();
                Nfs3 nfs3 = this.createNFS3client(file, mountPoint);
                Nfs3File nfs3File = new Nfs3File(nfs3, file.getPath());
                try (
                    NfsFileInputStream inputStream = new NfsFileInputStream(nfs3File, bufferSize)) {
                    int nbOfBytes = inputStream.read(buffer);
                    while (nbOfBytes >= 0) {
                        retValue = retValue + nbOfBytes;
                        os.write(buffer, 0, nbOfBytes);
                        nbOfBytes = inputStream.read(buffer);
                    }
                } catch (IOException e) {
                    FileUtils.LOGGER.error(
                        "[EVENT][{}] Unexptected error when copy remote to local - {}",
                        file.getImageId(),
                        ExceptionUtils.getStackTrace(e));
                    throw e;
                }
            }

            return retValue;
        } finally {
            stopWatch.stop();
            FileUtils.LOGGER.info(
                "[EVENT][{}] Terminated copy, duration is {}, nb of byte {} Mb",
                file.getImageId(),
                stopWatch.formatTime(),
                FileUtils.humanReadableByteCountBin(retValue));
        }
    }

    public static String humanReadableByteCountBin(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) { return bytes + " B"; }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; (i >= 0) && (absB > (0xfffccccccccccccL >> i)); i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
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

    public Stream<File> toStream(String coordinates, String filePath, final String... extensions) throws IOException {
        Nfs3 nfs3 = new Nfs3(coordinates, new CredentialUnix(0, 0, null), 3);
        Nfs3File file = new Nfs3File(nfs3, filePath);
        FileUtils.LOGGER.info("NFS STREAM coordinates are : {}, filePath is {}", coordinates, filePath);
        return file.listFiles()
            .stream()
            .peek((e) -> FileUtils.LOGGER.info("Processing sub file {}", e))
            .filter((f) -> this.filteNFSFile(f, extensions))
            .flatMap((f) -> this.toNFSStream(f, extensions))
            .map((s) -> this.toFile(s, nfs3));

    }

    private Stream<Nfs3File> toNFSStream(Nfs3File file, final String... extensions) {
        try {
            if (file.isDirectory()) {
                try {
                    return file.listFiles()
                        .stream()
                        .filter((f) -> this.filteNFSFile(f, extensions))
                        .flatMap((f) -> this.toNFSStream(f, extensions));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Stream.of(file);
    }

    protected boolean filteNFSFile(Nfs3File f, final String... extensions) {
        try {
            if (!f.isDirectory()) {
                for (String e : extensions) {
                    if (f.getName()
                        .toLowerCase()
                        .endsWith(e.toLowerCase())) { return true; }
                }
                return false;
            }
            return true;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private File toFile(final Nfs3File s, final Nfs3 nfs3) {
        return new File(s.getPath()) {

            @Override
            public String getName() { return s.getName(); }

            @Override
            public String getAbsolutePath() { return s.getPath(); }

            @Override
            public boolean isDirectory() {
                try {
                    return s.isDirectory();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        };

    }

    protected Nfs3 createNFS3client(FileToProcess file, String root) throws IOException {
        return this.mapOfNfs3client.computeIfAbsent(file.getHost() + "/root", (e) -> {
            try {
                FileUtils.LOGGER.info("Creating a Nfs3 client {} , {}", file.getHost(), root);
                return new Nfs3(file.getHost(), root, new CredentialUnix(0, 0, null), 3);
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        });
    }

}
