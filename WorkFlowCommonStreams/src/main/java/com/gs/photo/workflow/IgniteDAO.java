package com.gs.photo.workflow;

import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "ignite.is.used", havingValue = "true")
public class IgniteDAO implements IIgniteDAO {

	protected final Logger                LOGGER = LoggerFactory.getLogger(IgniteDAO.class);

	@Autowired
	protected IgniteCache<String, byte[]> igniteCache;

	@Override
	public void save(String key, byte[] rawFile) {
		boolean saved = this.igniteCache.putIfAbsent(key,
				rawFile);
		if (!saved) {
			this.LOGGER.warn("Warning : file with key {} already exist",
					key);
		}

	}

	@Override
	public byte[] get(String key) {
		return this.igniteCache.get(key);
	}

}
