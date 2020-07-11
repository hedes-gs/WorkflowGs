package com.gs.photos;

import java.lang.reflect.Field;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.MethodParameter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.HateoasPageableHandlerMethodArgumentResolver;
import org.springframework.data.web.HateoasSortHandlerMethodArgumentResolver;
import org.springframework.format.datetime.standard.DateTimeFormatterRegistrar;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import com.gs.photo.workflow.DateTimeHelper;

@Configuration(proxyBeanMethods = false)
public class GsWebMvcConfiguration extends WebMvcConfigurationSupport {

    @Override
    protected void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
        super.addArgumentResolvers(argumentResolvers);
        argumentResolvers.add(new HateoasSortHandlerMethodArgumentResolver());
    }

    @Bean
    @Primary
    @Override
    public FormattingConversionService mvcConversionService() {
        FormattingConversionService conversionService = super.mvcConversionService();
        DateTimeFormatterRegistrar registrar = new DateTimeFormatterRegistrar();
        registrar.setDateFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_PATTERN));
        registrar.setDateTimeFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_TIME_PATTERN));
        registrar.registerFormatters(conversionService);

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

        return conversionService;
    }

    @Configuration(proxyBeanMethods = false)
    public static class GsWebWebMvcAutoConfiguration extends WebMvcAutoConfiguration {

    }

    @Bean
    public HateoasPageableHandlerMethodArgumentResolver pageableResolver() {

        HateoasPageableHandlerMethodArgumentResolver pageableResolver = new HateoasPageableHandlerMethodArgumentResolver() {
            @Override
            public Pageable resolveArgument(
                MethodParameter methodParameter,
                @Nullable ModelAndViewContainer mavContainer,
                NativeWebRequest webRequest,
                @Nullable WebDataBinderFactory binderFactory
            ) {
                return super.resolveArgument(methodParameter, mavContainer, webRequest, binderFactory);
            }
        };
        return pageableResolver;
    }

}