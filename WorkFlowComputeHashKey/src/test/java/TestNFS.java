import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;

public class TestNFS {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @Test
    public void testGetRootHandle() throws Exception {
        Nfs3 nfs3 = new Nfs3("ipc3:/localcache", new CredentialUnix(0, 0, null), 3);
        StringBuilder resultStringBuilder = new StringBuilder();

        Nfs3File test = new Nfs3File(nfs3, "/toto.txt");
        Assert.assertTrue(test.canModify());
        NfsFileInputStream inputStream = new NfsFileInputStream(test);
        try (
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line)
                    .append("\n");
            }
        }
        Assert.assertEquals("toto\n", resultStringBuilder.toString());
    }

    @Test
    @Ignore
    public void testGetArwOnUSBReader() throws Exception {
        Nfs3 nfs3 = new Nfs3("ipc3:/usb-input-sdcard", new CredentialUnix(1000, 1000, null), 3);

        Nfs3File test = new Nfs3File(nfs3, "/DCIM/10000307/_HDE3429.ARW");
        Assert.assertTrue(test.canModify());
        byte[] buffer = new byte[4 * 1024 * 1024];
        int count = 0;
        try (
            NfsFileInputStream inputStream = new NfsFileInputStream(test)) {
            int readBytes;
            do {
                readBytes = inputStream.read(buffer);
                if (readBytes != -1) {
                    count = count + readBytes;
                }
            } while (readBytes != -1);
        }
        System.out.println(".... count " + count);
        Assert.assertTrue(count > 0);
    }

}
