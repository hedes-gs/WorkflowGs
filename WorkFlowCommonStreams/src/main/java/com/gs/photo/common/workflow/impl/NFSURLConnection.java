package com.gs.photo.common.workflow.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.commons.httpclient.util.URIUtil;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;

public class NFSURLConnection extends URLConnection {

    protected final Nfs3               nfs3;
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

    protected NFSURLConnection(URL u) throws IOException {
        super(u);
        try {
            Path p = Path.of(URIUtil.decode(u.getPath()));
            String nfsRoot = u.getHost() + ":/" + p.getName(0) + "/";
            String subPath = '/' + p.subpath(1, p.getNameCount())
                .toString()
                .replace("\\", "/");
            if ("nfs".equals(
                p.getName(0)
                    .toString())) {
                nfsRoot = u.getHost() + ":/nfs/" + p.getName(1) + "/";
                subPath = '/' + p.subpath(2, p.getNameCount())
                    .toString()
                    .replace("\\", "/");
            }

            this.nfs3 = new Nfs3(nfsRoot, new CredentialUnix(0, 0, null), 3);
            this.file = new NFSFile(new Nfs3File(this.nfs3, subPath));
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    public void connect() throws IOException {}

}
