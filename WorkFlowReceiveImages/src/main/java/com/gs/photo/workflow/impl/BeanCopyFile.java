package com.gs.photo.workflow.impl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFSv4StateHandler;
import org.dcache.nfs.v4.xdr.verifier4;
import org.dcache.oncrpc4j.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.ICopyImageFile;

public class BeanCopyFile implements ICopyImageFile {

	@Value("${topic.uniqueImageFoundTopic}")
	protected String uniqueImageFoundTopic;

	@Autowired
	protected Consumer<String, String> consumerForTopicWithStringKey;

	@Autowired
	protected IBeanTaskExecutor beanTaskExecutor;

	@Scheduled
	public void processInputFile() {
		consumerForTopicWithStringKey.subscribe(Arrays.asList(uniqueImageFoundTopic));
		while (true) {
			try {
				ConsumerRecords<String, String> consumerRecords = consumerForTopicWithStringKey.poll(500);
				if (consumerRecords.count() > 0) {
					List<Future<String>> tasks = new ArrayList<Future<String>>();

					consumerRecords.forEach((cr) -> {
						FutureTask<String> future = new FutureTask<String>(() -> {
							copyFile(cr.value());
							return cr.key();
						});
						tasks.add(future);
						beanTaskExecutor.execute(future);
					}

					);
					tasks.forEach((t) -> {
						try {
							t.get();
						} catch (InterruptedException | ExecutionException e) {
							e.printStackTrace();
						}
					});
					consumerForTopicWithStringKey.commitSync();
				}
			} catch (Exception e) {

			} finally {

			}
		}
	}

	@Override
	public void copyFile(String from) {
		Nfs3 nfs3 = new Nfs3(server, exportedPath, credential, maximumRetries);
	}

	static NFS4Client createClient(NFSv4StateHandler stateHandler, int minor) throws UnknownHostException {
		InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(null), 123);
		byte[] owner = new byte[8];
		byte[] bootTime = new byte[8];
		RANDOM.nextBytes(owner);
		Bytes.putLong(bootTime, 0, System.currentTimeMillis());
		return stateHandler.createClient(address, address, minor, owner, new verifier4(bootTime), null, false);
	}

	private final static Random RANDOM = new Random();

}
