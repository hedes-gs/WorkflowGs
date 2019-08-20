package com.gs.photo.workflow;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class BeanPollFolder {

	protected static final Logger LOGGER = Logger.getLogger(BeanPollFolder.class);
	@Value("${polledFolders}")
	protected String polledFolders;

	@Autowired
	protected IBeanProcessImage beanProcessImage;
	@Autowired
	protected IBeanTaskExecutor beanTaskExecutor;

	@PostConstruct
	public void init() {
		beanTaskExecutor.execute(() -> {
			poll();
		});
	}

	public void poll() {
		try {
			LOGGER.info("Starting to poll " + this.polledFolders);

			File dirFile = new File(polledFolders);
			Set<String> setOfProcessedFiles = new HashSet<>();
			for (;;) {
				String[] listDir = dirFile.list();
				for (int k = 0; k < listDir.length; k++) {
					String fileToProcess = this.polledFolders + "/" + listDir[k];
					if (!setOfProcessedFiles.contains(fileToProcess)) {
						process(fileToProcess);
						setOfProcessedFiles.add(fileToProcess);
					}
				}
				Thread.sleep(250);
			}
		} catch (InterruptedException e) {
			LOGGER.warn("Warning", e);
		}

	}

	private void process(final String child) {
		beanTaskExecutor.execute(() -> {
			beanProcessImage.process(child);
		});
	}
}
