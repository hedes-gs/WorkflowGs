package com.gs.photos.ws;

import java.lang.reflect.Field;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.web.ReactivePageableHandlerMethodArgumentResolver;
import org.springframework.data.web.ReactiveSortHandlerMethodArgumentResolver;
import org.springframework.format.datetime.standard.DateTimeFormatterRegistrar;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.hateoas.mediatype.MessageResolver;
import org.springframework.hateoas.mediatype.hal.CurieProvider;
import org.springframework.hateoas.mediatype.hal.Jackson2HalModule;
import org.springframework.hateoas.server.LinkRelationProvider;
import org.springframework.hateoas.server.core.DefaultLinkRelationProvider;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.util.MimeType;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.result.method.annotation.ArgumentResolverConfigurer;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.JettyRequestUpgradeStrategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.gs.photo.common.workflow.DateTimeHelper;

@Configuration
public class WebFluxConfig implements WebFluxConfigurer {

    @Autowired
    private ObjectMapper                               objectMapper;

    @Autowired
    private WebFluxWebSocketComponentEventHandler      handler;

    @Autowired
    private WebFluxWebSocketFullyProcessedImageHandler fullyProcessedImagehandler;

    @Bean
    public HandlerMapping handlerMapping() {
        Map<String, WebSocketHandler> handlerMap = Map
            .of("/ws/componentStatus", this.handler, "/ws/fullyImagesProcessed", this.fullyProcessedImagehandler

            );
        return new SimpleUrlHandlerMapping(handlerMap, -1);
    }

    @Bean
    public WebSocketHandlerAdapter handlerAdapter() { return new WebSocketHandlerAdapter(this.webSocketService()); }

    @Bean
    public WebSocketService webSocketService() {
        this.configureDate();
        JettyRequestUpgradeStrategy strategy = new JettyRequestUpgradeStrategy();
        return new HandshakeWebSocketService(strategy);
    }

    private void configureDate() {
        DateTimeFormatterRegistrar registrar = new DateTimeFormatterRegistrar();
        registrar.setDateFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_PATTERN));
        registrar.setDateTimeFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_TIME_PATTERN));

        // Hook for hateoas: it is using the field CONVERSION_SERVICE and not a bean
        // spring
        // this is the only way to overload the serialization of the OffsetDateTime
        // value
        try {
            Class<?> cl = this.getClass()
                .getClassLoader()
                .loadClass("org.springframework.hateoas.server.core.WebHandler$PathVariableParameter");
            Field f = cl.getSuperclass()
                .getDeclaredField("CONVERSION_SERVICE");
            f.setAccessible(true);
            DefaultFormattingConversionService df = (DefaultFormattingConversionService) f.get(null);
            registrar.registerFormatters(df);
        } catch (
            ClassNotFoundException |
            NoSuchFieldException |
            SecurityException |
            IllegalArgumentException |
            IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void configureHttpMessageCodecs(ServerCodecConfigurer configurer) {

        com.fasterxml.jackson.databind.Module module = new Jackson2HalModule();
        LinkRelationProvider provider = new DefaultLinkRelationProvider();
        WebFluxConfig.this.configureDate();
        Jackson2HalModule.HalHandlerInstantiator instantiator = new Jackson2HalModule.HalHandlerInstantiator(provider,
            CurieProvider.NONE,
            MessageResolver.DEFAULTS_ONLY);
        WebFluxConfig.this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        WebFluxConfig.this.objectMapper.registerModule(module);
        WebFluxConfig.this.objectMapper.setHandlerInstantiator(instantiator);

        configurer.defaultCodecs()
            .jackson2JsonEncoder(
                new Jackson2JsonEncoder(WebFluxConfig.this.objectMapper,
                    new MimeType("application", "json"),
                    new MimeType("application", "stream+json")

                ));

        configurer.defaultCodecs()
            .jackson2JsonDecoder(new Jackson2JsonDecoder(WebFluxConfig.this.objectMapper));
    }

    @Override
    public void configureArgumentResolvers(ArgumentResolverConfigurer configurer) {
        configurer.addCustomResolver(
            new ReactiveSortHandlerMethodArgumentResolver(),
            new ReactivePageableHandlerMethodArgumentResolver());
    }

}