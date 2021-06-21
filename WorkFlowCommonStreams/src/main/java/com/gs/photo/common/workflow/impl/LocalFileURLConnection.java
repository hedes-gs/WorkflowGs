package com.gs.photo.common.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.stream.Stream;

public class LocalFileURLConnection extends URLConnection {

    protected final AbstractRemoteFile file;

    public Stream<AbstractRemoteFile> listFiles(String... extensions) throws IOException {
        return Stream.of(this.file.listFiles((f) -> {
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
    public InputStream getInputStream() throws IOException { return this.file.openInputStream(); }

    protected LocalFileURLConnection(URL u) throws IOException {
        super(u);
        try {
            this.file = new LocalFile(new File(u.toURI()));
        } catch (IOException e) {
            throw e;
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void connect() throws IOException {}

}
