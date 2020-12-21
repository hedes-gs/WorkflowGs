package com.gs.photos;

import org.springframework.hateoas.RepresentationModel;
import org.springframework.hateoas.mediatype.MessageResolver;
import org.springframework.hateoas.mediatype.hal.CurieProvider;
import org.springframework.hateoas.mediatype.hal.Jackson2HalModule;
import org.springframework.hateoas.server.LinkRelationProvider;
import org.springframework.hateoas.server.core.DefaultLinkRelationProvider;
import org.springframework.hateoas.server.mvc.TypeConstrainedMappingJackson2HttpMessageConverter;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.MimeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class HalMessageConverter extends AbstractMessageConverter {
    public HalMessageConverter() {
        MimeType hal = new MimeType("application", "hal+json");
        this.addSupportedMimeTypes(hal);
    }

    @Override
    protected boolean supports(Class<?> clazz) { return RepresentationModel.class.isAssignableFrom(clazz); }

    @Override
    protected Object convertToInternal(Object payload, MessageHeaders headers, Object conversionHint) {

        TypeConstrainedMappingJackson2HttpMessageConverter converter = new TypeConstrainedMappingJackson2HttpMessageConverter(
            RepresentationModel.class);

        com.fasterxml.jackson.databind.Module module = new Jackson2HalModule();
        LinkRelationProvider provider = new DefaultLinkRelationProvider();

        Jackson2HalModule.HalHandlerInstantiator instantiator = new Jackson2HalModule.HalHandlerInstantiator(provider,
            CurieProvider.NONE,
            MessageResolver.DEFAULTS_ONLY);

        ObjectMapper mapper = converter.getObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.registerModule(module);
        mapper.setHandlerInstantiator(instantiator);

        try {
            String payloadJson = mapper.writeValueAsString(payload);
            byte[] bytes = payloadJson.getBytes();
            return bytes;
        } catch (JsonProcessingException exception) {
            return null;
        }
    }
}