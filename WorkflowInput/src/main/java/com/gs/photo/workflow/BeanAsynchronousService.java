package com.gs.photo.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.workflow.IBeanTaskExecutor;

@Service
public class BeanAsynchronousService {

	@Autowired
	protected IBeanTaskExecutor taskExecutor;
}
