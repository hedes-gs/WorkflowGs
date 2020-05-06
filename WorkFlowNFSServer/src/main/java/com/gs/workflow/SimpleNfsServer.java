package com.gs.workflow;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v3.xdr.mount_prot;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v4.MDSOperationExecutor;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;

public class SimpleNfsServer implements Closeable {
    private final OncRpcSvc nfsSvc;
    private final Path      root;
    private final int       port;
    private final String    name;

    public SimpleNfsServer(Path root) { this(2049,
        root,
        null,
        null); }

    public SimpleNfsServer(
        int port,
        Path root,
        ExportFile exportFile,
        String name
    ) {
        try {
            if (exportFile == null) {
                exportFile = new ExportFile(new InputStreamReader(SimpleNfsServer.class.getClassLoader()
                    .getResourceAsStream("exports")));
            }

            this.port = port;

            if (root == null) {
                root = Files.createTempDirectory(null);
            }
            this.root = root;

            if (name == null) {
                name = "nfs@" + this.port;
            }
            this.name = name;

            VirtualFileSystem vfs = new LocalFileSystem(this.root,
                exportFile.exports()
                    .collect(Collectors.toList()));

            this.nfsSvc = new OncRpcSvcBuilder().withPort(this.port)
                .withTCP()
                .withAutoPublish()
                .withWorkerThreadIoStrategy()
                .withServiceName(this.name)
                .build();

            NFSServerV41 nfs4 = new NFSServerV41.Builder().withVfs(vfs)
                .withOperationExecutor(new MDSOperationExecutor())
                .withExportTable(exportFile)
                .build();

            NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
            MountServer mountd = new MountServer(exportFile, vfs);

            this.nfsSvc.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), mountd);
            this.nfsSvc.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3);
            this.nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
            this.nfsSvc.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws IOException { this.nfsSvc.stop(); }

    public Path getRoot() { return this.root; }

    public int getPort() { return this.port; }
}
