package com.gs.photo.common.workflow.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
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

import jcifs.CIFSContext;
import jcifs.CIFSException;
import jcifs.config.PropertyConfiguration;
import jcifs.context.BaseContext;
import jcifs.smb.NtlmPasswordAuthenticator;
import jcifs.smb.SmbFile;

@Service
@ConditionalOnClass(value = com.emc.ecs.nfsclient.nfs.io.Nfs3File.class)
public class FileUtils {

    protected static Logger LOGGER                               = LoggerFactory.getLogger(FileUtils.class);
    public static final int NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED = (int) (2.0 * 1024 * 1024);

    private static URLStreamHandler getProtocol(String protocol) {
        switch (protocol) {
            case "smb": {
                String password = System.getProperty("smb.password");
                String user = System.getProperty("smb.user");
                try {
                    BaseContext bc = new BaseContext(new PropertyConfiguration(System.getProperties()));
                    final CIFSContext ct = bc.withCredentials(new NtlmPasswordAuthenticator(user, password));
                    return new URLStreamHandler() {

                        @Override
                        public URLConnection openConnection(URL u) throws IOException {
                            if (FileUtils.LOGGER.isDebugEnabled()) {
                                FileUtils.LOGGER.debug("Opening file " + u);
                            }
                            return URLConnectionWrapper.of(new SmbFile(u, ct));
                        }

                    };
                } catch (CIFSException e) {
                }
                break;
            }
            case "nfs": {
                return new NFSHandler();
            }
            case "localfile": {
                return new LocalFileHandler();
            }
        }
        return null;
    }

    static {
        URL.setURLStreamHandlerFactory(protocol -> FileUtils.getProtocol(protocol));
    }

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

    public byte[] readFirstBytesOfFile(FileToProcess file) throws IOException {
        return this.readFirstBytesOfFile(file, FileUtils.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED);
    }

    public byte[] readFirstBytesOfFile(FileToProcess file, int bufferSize) throws IOException {

        if (!file.isCompressedFile()) {
            return this.readFirstBytesOfFile(new URL(file.getUrl()), bufferSize);
        } else {
            return this.readFirstBytesOfCompressedFile(new URL(file.getUrl()), bufferSize);
        }
    }

    public byte[] readFirstBytesOfFileRetry(FileToProcess file) {
        byte[] retValue = null;
        int nbOfRetries = 0;
        try {
            do {
                try {
                    nbOfRetries++;
                    FileUtils.LOGGER.info(
                        "[EVENT][{}] readFirstBytesOfFile of {} - try : {}",
                        file.getImageId(),
                        file,
                        nbOfRetries);
                    retValue = this.readFirstBytesOfFile(file);
                    if ((retValue == null)) {
                        FileUtils.LOGGER.warn(
                            " Error when readFirstBytesOfFile of {} : unexpected read bytes value {}",
                            file,
                            retValue == null ? "<null>" : retValue.length);
                        throw new IOException("unable to read file " + file);
                    }
                } catch (Exception e) {
                    if (nbOfRetries < 5) {
                        FileUtils.LOGGER.error(
                            "[EVENT][{}] Unable to get first bytes for {} , retrying in 1 seconds  ",
                            file.getImageId(),
                            file);
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        FileUtils.LOGGER.error(
                            "[EVENT][{}] Error when readFirstBytesOfFile of {} : unable to read first byte : {}",
                            file.getImageId(),
                            file,
                            ExceptionUtils.getStackTrace(e));
                    }
                }
            } while ((retValue == null) && (nbOfRetries < 5));
        } catch (Exception e) {
            FileUtils.LOGGER.error(
                " Error when readFirstBytesOfFile of {} : unexpected read bytes value {}",
                file,
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        return retValue;
    }

    public byte[] readFirstBytesOfFileRetryWithbufferIncreased(FileToProcess file) {
        byte[] retValue = null;
        int nbOfRetries = 0;
        try {
            do {
                try {
                    nbOfRetries++;
                    FileUtils.LOGGER.info(
                        "[EVENT][{}] readFirstBytesOfFile of {} - try : {}",
                        file.getImageId(),
                        file,
                        nbOfRetries);
                    retValue = this.readFirstBytesOfFile(file, 10 * FileUtils.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED);
                    if ((retValue == null)) {
                        FileUtils.LOGGER.warn(
                            " Error when readFirstBytesOfFile of {} : unexpected read bytes value {}",
                            file,
                            retValue == null ? "<null>" : retValue.length);
                        throw new IOException("unable to read file " + file);
                    }
                } catch (Exception e) {
                    if (nbOfRetries < 5) {
                        FileUtils.LOGGER.error(
                            "[EVENT][{}] Unable to get first bytes for {} , retrying in 1 seconds  ",
                            file.getImageId(),
                            file);
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        FileUtils.LOGGER.error(
                            "[EVENT][{}] Error when readFirstBytesOfFile of {} : unable to read first byte : {}",
                            file.getImageId(),
                            file,
                            ExceptionUtils.getStackTrace(e));
                    }
                }
            } while ((retValue == null) && (nbOfRetries < 5));
        } catch (Exception e) {
            FileUtils.LOGGER.error(
                " Error when readFirstBytesOfFile of {} : unexpected read bytes value {}",
                file,
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        return retValue;
    }

    public boolean deleteIfLocal(FileToProcess file, String root) throws IOException {
        InetAddress ip = InetAddress.getLocalHost();
        URL url = new URL(file.getUrl());
        String remoteHostName = url.getHost();
        String remoteFolder = url.getPath();
        String localHostname = ip.getHostName();
        if (Objects.equal(remoteHostName.toLowerCase(), localHostname.toLowerCase())) {
            final File sourceFile = new File(root + "/" + remoteFolder);
            if (sourceFile.exists()) {
                return sourceFile.delete();
            } else {
                FileUtils.LOGGER.warn(
                    "[EVENT][{}] unable to delete local file : {} , it does not exist",
                    file.getImageId(),
                    sourceFile);
            }
        } else if ((file.getIsLocal() != null) && file.getIsLocal()) {

        } else {
            FileUtils.LOGGER
                .warn("[EVENT][{}] unable to delete local file : {} as it is remote ", file.getImageId(), file);
        }
        return false;
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

    public Stream<AbstractRemoteFile> toStream(URL url, final String... extensions) throws IOException {
        return ((URLConnectionWrapper) url.openConnection()).listFiles(extensions)
            .flatMap((x) -> this.findChildrenIfDirectoryFound(x, extensions));
    }

    public byte[] readFirstBytesOfFile(URL url, int bufferSize) throws IOException {
        byte[] retValue = new byte[bufferSize];
        int offset = 0;
        int nbOfBytes = 0;
        try (
            InputStream is = url.openConnection()
                .getInputStream()) {
            do {
                nbOfBytes = is.read(retValue, offset, retValue.length - offset);
                if (nbOfBytes >= 0) {
                    offset = offset + nbOfBytes;
                }
            } while ((nbOfBytes != -1) && (offset != retValue.length));
        }
        if (offset < retValue.length) {
            byte[] truncated = new byte[offset];
            System.arraycopy(retValue, 0, truncated, 0, offset);
            return truncated;
        }

        return retValue;
    }

    public byte[] readFirstBytesOfCompressedFile(URL url, int bufferSize) throws IOException {
        byte[] retValue = new byte[bufferSize];
        int offset = 0;
        try (
            InputStream is = url.openConnection()
                .getInputStream();
            ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry entry = zis.getNextEntry();
            do {
                if (FilenameUtils.isExtension(entry.getName(), "ARW")) {
                    zis.read(retValue);
                    break;
                }
            } while ((entry = zis.getNextEntry()) != null);
        }
        return retValue;
    }

    public long copyRemoteToLocal(URL url, Path localPath) throws IOException {
        final File file = localPath.toFile();
        org.apache.commons.io.FileUtils.copyURLToFile(url, file);
        return file.length();
    }

    public long copyRemoteToLocal(URL url, OutputStream os) throws IOException {
        try (
            InputStream is = url.openStream()) {
            return IOUtils.copyLarge(is, os);
        }
    }

    public long copyRemoteToLocal(FileToProcess fileToProcess, Path localPath) throws IOException {
        long retValue = -1;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            retValue = this.copyRemoteToLocal(new URL(fileToProcess.getUrl()), localPath);
        } finally {
            stopWatch.stop();
            FileUtils.LOGGER.info(
                "[EVENT][{}] Terminated copy, duration is {}, nb of byte {} Mb",
                fileToProcess.getImageId(),
                stopWatch.formatTime(),
                FileUtils.humanReadableByteCountBin(retValue));
        }
        return retValue;
    }

    public long copyRemoteToLocal(FileToProcess fileToProcess, OutputStream localPath) throws IOException {
        long retValue = -1;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            retValue = this.copyRemoteToLocal(new URL(fileToProcess.getUrl()), localPath);
        } finally {
            stopWatch.stop();
            FileUtils.LOGGER.info(
                "[EVENT][{}] Terminated copy, duration is {}, nb of byte {} Mb",
                fileToProcess.getImageId(),
                stopWatch.formatTime(),
                FileUtils.humanReadableByteCountBin(retValue));
        }
        return retValue;
    }

    private Stream<AbstractRemoteFile> findChildrenIfDirectoryFound(AbstractRemoteFile x, final String... extensions) {
        if (x.isDirectory()) {
            FileUtils.LOGGER.info("List file {} ", x);
            return Stream.of(x.listFiles((y) -> this.filterFile(y, extensions)))
                .flatMap((t) -> this.findChildrenIfDirectoryFound(t, extensions));
        }
        return Stream.of(x);
    }

    protected boolean filterFile(File f, final String... extensions) {
        if (!f.isDirectory()) {
            for (String e : extensions) {
                if (f.getName()
                    .toLowerCase()
                    .endsWith(e.toLowerCase())) { return true; }
            }
            return false;
        }
        return true;
    }

    protected boolean filterFile(AbstractRemoteFile f, final String... extensions) {
        if (!f.isDirectory()) {
            for (String e : extensions) {
                if (f.getName()
                    .toLowerCase()
                    .endsWith(e.toLowerCase())) { return true; }
            }
            return false;
        }
        return true;
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

    public static String getSimpleNameFromUrl(String url) {
        final Path urlPath = Paths.get(url);
        final Path lastSegment = urlPath.getName(urlPath.getNameCount() - 1);
        return lastSegment.toString();
    }

}
