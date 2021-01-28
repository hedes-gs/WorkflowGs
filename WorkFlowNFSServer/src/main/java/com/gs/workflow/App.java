package com.gs.workflow;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;

import org.dcache.nfs.ExportFile;
import org.dcache.oncrpc4j.portmap.OncRpcEmbeddedPortmap;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class App {

    protected static Logger LOGGER  = LoggerFactory.getLogger(App.class);

    @Option(name = "-root", usage = "root of the file system to export", metaVar = "<path>")
    private Path            root;
    @Option(name = "-exports", usage = "path to file with export tables", metaVar = "<file>")
    private Path            exportsFile;
    @Option(name = "-port", usage = "TCP port to use", metaVar = "<port>")
    private int             rpcPort = 2049;
    @Option(name = "-with-portmap", usage = "start embedded portmap")
    private boolean         withPortmap;

    public static void main(String[] args) throws Exception { new App().run(args); }

    public void run(String[] args) throws CmdLineException, IOException {

        App.LOGGER.info(" Starting app...");
        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println();
            System.err.println(e.getMessage());
            System.err.println("Usage:");
            System.err.println("    App [options...]");
            System.err.println();
            parser.printUsage(System.err);
            System.exit(1);
        }

        ExportFile exportFile = null;
        if (this.exportsFile != null) {
            exportFile = new ExportFile(this.exportsFile.toFile());
        } else {
            try (
                InputStream in = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("exports");
                Reader reader = new InputStreamReader(in)) {
                exportFile = new ExportFile(reader);
            }
        }
        if (this.root == null) {
            this.root = Path.of("/");
        }

        if (this.withPortmap) {
            new OncRpcEmbeddedPortmap();
        }

        try (
            SimpleNfsServer ignored = new SimpleNfsServer(this.rpcPort, this.root, exportFile, null)) {
            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                }
            }
        }
    }
}
