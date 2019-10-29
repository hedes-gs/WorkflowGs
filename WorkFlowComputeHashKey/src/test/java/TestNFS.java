import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;

public class TestNFS {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetRootHandle() throws Exception {
		Nfs3 nfs3 = new Nfs3("ipc0:/cache_1", new CredentialUnix(0, 0, null), 3);
		StringBuilder resultStringBuilder = new StringBuilder();

		Nfs3File test = new Nfs3File(nfs3, "/toto.txt");
		Assert.assertTrue(test.canModify());
		NfsFileInputStream inputStream = new NfsFileInputStream(test);
		try (
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
			String line;
			while ((line = br.readLine()) != null) {
				resultStringBuilder.append(line).append("\n");
			}
		}
		Assert.assertEquals("toto\n",
				resultStringBuilder.toString());
	}

}
