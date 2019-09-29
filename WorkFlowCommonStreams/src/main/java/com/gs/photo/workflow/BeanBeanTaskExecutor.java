package com.gs.photo.workflow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class BeanBeanTaskExecutor implements IBeanTaskExecutor {

	@Autowired
	protected ThreadPoolTaskExecutor threadPoolTaskExecutor;

	@Override
	public void execute(Runnable r) {
		threadPoolTaskExecutor.execute(
			r);
	}

	@Override
	public <V> Collection<Future<V>> execute(Collection<Callable<V>> tasks) {
		final List<Future<V>> retValue = new ArrayList<>();
		tasks.forEach(
			(t) -> retValue.add(
				threadPoolTaskExecutor.submit(
					t)));
		return retValue;
	}

	@Override
	public void executeRunnables(Collection<Runnable> rs) {
		rs.forEach(
			(r) -> {
				execute(
					r);
			});
	}
}
