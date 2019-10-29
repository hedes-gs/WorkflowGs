package com.gs.photos.workflow.metadata;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import com.gs.photo.workflow.IIgniteDAO;

@Profile("test")
@Configuration
public class NameServiceTestConfiguration {
	@Bean
	@Primary
	public IIgniteDAO iIgniteDAO() {
		return Mockito.mock(IIgniteDAO.class);
	}
}