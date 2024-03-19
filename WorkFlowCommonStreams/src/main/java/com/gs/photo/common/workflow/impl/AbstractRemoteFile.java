package com.gs.photo.common.workflow.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

public abstract class AbstractRemoteFile {

    public interface FileFilter { boolean accept(AbstractRemoteFile pathname); }

    private final URL url;

    public AbstractRemoteFile(URL url) { this.url = url; }

    public abstract boolean canRead();

    public abstract boolean canWrite();

    public abstract boolean isDirectory();

    public abstract boolean isFile();

    public abstract boolean isHidden();

    public abstract boolean exists();

    public abstract AbstractRemoteFile getParentFile() throws MalformedURLException, IOException;

    public abstract AbstractRemoteFile[] listFiles();

    public abstract AbstractRemoteFile[] listFiles(FileFilter filter);

    public abstract boolean mkdir();

    public URL getUrl() { return this.url; }

    public abstract String getName();

    public abstract AbstractRemoteFile getChild(String string);

    public abstract InputStream openInputStream() throws IOException;

    public abstract URL toExternalURL();

    public abstract void delete();

}
