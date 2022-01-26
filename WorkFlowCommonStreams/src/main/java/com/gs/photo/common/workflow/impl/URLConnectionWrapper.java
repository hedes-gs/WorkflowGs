package com.gs.photo.common.workflow.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.stream.Stream;

import jcifs.smb.SmbFile;

public abstract class URLConnectionWrapper extends URLConnection {
    private URLConnection urlConnection;

    public abstract Stream<AbstractRemoteFile> listFiles(String... extensions) throws IOException;

    public URLConnection getUrlConnection() { return this.urlConnection; }

    public URLConnectionWrapper(
        URL url,
        URLConnection urlConnection
    ) {
        super(url);
        this.urlConnection = urlConnection;
    }

    private static class NFSURLConnectionWrapper extends URLConnectionWrapper {

        private NFSURLConnection nFSURLConnection;

        @Override
        public OutputStream getOutputStream() throws IOException { return this.nFSURLConnection.getOutputStream(); }

        public NFSURLConnectionWrapper(NFSURLConnection nFSURLConnection) {
            super(nFSURLConnection.getURL(),
                nFSURLConnection);
            this.nFSURLConnection = nFSURLConnection;
        }

        @Override
        public InputStream getInputStream() throws IOException { return this.nFSURLConnection.getInputStream(); }

        @Override
        public Stream<AbstractRemoteFile> listFiles(String... extensions) throws IOException {
            return this.nFSURLConnection.listFiles(extensions);
        }

        @Override
        public void connect() throws IOException { this.nFSURLConnection.connect(); }

    }

    private static class SMBURLConnectionWrapper extends URLConnectionWrapper {

        private SmbResourceFile smbFile;

        @Override
        public InputStream getInputStream() throws IOException { return this.smbFile.openInputStream(); }

        public SMBURLConnectionWrapper(SmbFile smbFile) {
            super(smbFile.getURL(),
                smbFile);
            this.smbFile = (SmbResourceFile) SmbResourceFile.of(smbFile);
        }

        @Override
        public Stream<AbstractRemoteFile> listFiles(String... extensions) throws IOException {
            return Stream.of(this.smbFile.listFiles((f) -> {
                if (!f.isDirectory()) {
                    for (String e : extensions) {
                        if (f.getName()
                            .toLowerCase()
                            .endsWith(e.toLowerCase())) { return true; }
                    }
                    return false;
                }
                return true;
            }));
        }

        @Override
        public void connect() throws IOException { this.smbFile.listFiles(); }

    }

    private static class FileURLConnectionWrapper extends URLConnectionWrapper {

        private LocalFileURLConnection localFileURLConnection;

        @Override
        public InputStream getInputStream() throws IOException { return this.localFileURLConnection.getInputStream(); }

        public FileURLConnectionWrapper(LocalFileURLConnection localFileConnection) {
            super(localFileConnection.getURL(),
                localFileConnection);
            this.localFileURLConnection = localFileConnection;
        }

        @Override
        public Stream<AbstractRemoteFile> listFiles(String... extensions) throws IOException {
            return this.localFileURLConnection.listFiles(extensions);
        }

        @Override
        public void connect() throws IOException { this.localFileURLConnection.connect(); }

    }

    public static URLConnectionWrapper of(URLConnection connection) {
        switch (connection.getURL()
            .getProtocol()) {
            case "nfs":
                return new NFSURLConnectionWrapper((NFSURLConnection) connection);
            case "smb":
                return new SMBURLConnectionWrapper((SmbFile) connection);
            case "localfile":
                return new FileURLConnectionWrapper((LocalFileURLConnection) connection);
        }
        throw new RuntimeException("Unsupported protocol " + connection.getURL()
            .getProtocol());
    }

}
