package com.gs.photos.ws;
/*
 * package com.gs.photos;
 *
 * import org.springframework.beans.factory.ObjectFactory; import
 * org.springframework.context.ApplicationContext; import
 * org.springframework.context.annotation.Bean; import
 * org.springframework.context.annotation.Configuration; import
 * org.springframework.context.annotation.Primary; import
 * org.springframework.core.MethodParameter; import
 * org.springframework.core.convert.ConversionService; import
 * org.springframework.data.domain.Pageable; import
 * org.springframework.data.web.HateoasPageableHandlerMethodArgumentResolver;
 * import
 * org.springframework.data.web.config.HateoasAwareSpringDataWebConfiguration;
 * import org.springframework.lang.Nullable; import
 * org.springframework.web.bind.support.WebDataBinderFactory; import
 * org.springframework.web.context.request.NativeWebRequest; import
 * org.springframework.web.method.support.ModelAndViewContainer;
 *
 * import com.gs.photos.controllers.GsPageRequest;
 *
 * @Configuration(proxyBeanMethods = false) public class
 * GsWebDataHateoasAwareSpringDataWebConfiguration extends
 * HateoasAwareSpringDataWebConfiguration { private
 * HateoasPageableHandlerMethodArgumentResolver pageableResolver = new
 * HateoasPageableHandlerMethodArgumentResolver() {
 *
 * @Override public boolean supportsParameter(MethodParameter parameter) { //
 * TODO Auto-generated method stub return super.supportsParameter(parameter) ||
 * GsPageRequest.class.equals(parameter.getParameterType()); }
 *
 * @Override public Pageable resolveArgument( MethodParameter methodParameter,
 *
 * @Nullable ModelAndViewContainer mavContainer, NativeWebRequest webRequest,
 *
 * @Nullable WebDataBinderFactory binderFactory ) { Pageable pageable =
 * super.resolveArgument(methodParameter, mavContainer, webRequest,
 * binderFactory); return pageable; } };
 *
 * public GsWebDataHateoasAwareSpringDataWebConfiguration( ApplicationContext
 * context, ObjectFactory<ConversionService> conversionService ) {
 *
 * }
 *
 * @Bean("GsHateoasPageableResolver")
 *
 * @Primary public HateoasPageableHandlerMethodArgumentResolver
 * pageableResolver() { return this.pageableResolver; }
 *
 * }
 */