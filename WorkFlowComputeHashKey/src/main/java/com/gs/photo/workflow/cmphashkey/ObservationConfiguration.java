package com.gs.photo.workflow.cmphashkey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.gs.instrumentation.MicrometerConfiguration;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.ObservationTextPublisher;
import io.micrometer.observation.aop.ObservedAspect;

@Configuration
@MicrometerConfiguration
public class ObservationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ObservationConfiguration.class);

    @Bean
    public ObservationHandler<Observation.Context> observationTextPublisher() {
        return new ObservationTextPublisher(ObservationConfiguration.log::info);
    }

    @Bean
    public ObservedAspect observedAspect(ObservationRegistry observationRegistry) {
        return new ObservedAspect(observationRegistry);
    }

    @Bean
    public ObservationRegistry observationRegistry(ObservationHandler<Observation.Context> observationTextPublisher) {
        final ObservationRegistry observationRegistry = ObservationRegistry.create();
        observationRegistry.observationConfig()
            .observationHandler(observationTextPublisher);
        return observationRegistry;
    }

    @Bean
    public MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }

    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) { return new TimedAspect(registry); }

}