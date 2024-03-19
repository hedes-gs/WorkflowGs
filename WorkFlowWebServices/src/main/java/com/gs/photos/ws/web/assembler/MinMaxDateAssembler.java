package com.gs.photos.ws.web.assembler;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.HateoasPageableHandlerMethodArgumentResolver;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.LinkRelation;
import org.springframework.hateoas.PagedModel.PageMetadata;
import org.springframework.hateoas.UriTemplate;
import org.springframework.hateoas.server.reactive.SimpleReactiveRepresentationModelAssembler;
import org.springframework.hateoas.server.reactive.WebFluxLinkBuilder;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.gs.photos.ws.controllers.ImageController;
import com.workflow.model.dtos.MinMaxDatesDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@Component
public class MinMaxDateAssembler implements SimpleReactiveRepresentationModelAssembler<MinMaxDatesDto> {

    protected static Logger                                    LOGGER = LoggerFactory
        .getLogger(MinMaxDateAssembler.class);
    private final HateoasPageableHandlerMethodArgumentResolver pageableResolver;
    private final Optional<UriComponents>                      baseUri;

    @Override
    public Mono<EntityModel<MinMaxDatesDto>> toModel(MinMaxDatesDto entity, ServerWebExchange exchange) {
        EntityModel<MinMaxDatesDto> resource = EntityModel.of(entity);
        return Mono.just(this.addLinks(resource, exchange));
    }

    public MinMaxDateAssembler() {
        this.pageableResolver = new HateoasPageableHandlerMethodArgumentResolver();
        this.baseUri = Optional.ofNullable(
            UriComponentsBuilder.fromOriginHeader("/api")
                .build());
    }

    public EntityModel<MinMaxDatesDto> toEntityModel(MinMaxDatesDto entity) {
        EntityModel<MinMaxDatesDto> entityModel = EntityModel.of(entity);
        entityModel = this.addLinks(entityModel, null);
        return entityModel;
    }

    public Mono<EntityModel<MinMaxDatesDto>> toReactiveEntityModel(MinMaxDatesDto entity, ServerWebExchange exchange) {
        String intervalType = null;
        switch (entity.getIntervallType()) {
            case "year":
                intervalType = "month";
                break;
            case "month":
                intervalType = "day";
                break;
            case "day":
                intervalType = "hour";
                break;
            case "hour":
                intervalType = "minute";
                break;
            case "minute":
                intervalType = "second";
                break;
            case "second":
                intervalType = "second";
                break;
        }
        try {
            return WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .getLimits(intervalType, entity.getMinDate(), entity.getMaxDate()))
                .withRel(LinkRelation.of("_subinterval"))
                .toMono()
                .map(
                    (t) -> EntityModel.of(entity)
                        .add(t))
                .zipWhen((t) -> this.getLinkForImagesForDateIntervall(t))
                .map((t) -> this.update(t));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Mono<Link> getLinkForImagesForDateIntervall(EntityModel<MinMaxDatesDto> t) {
        try {
            return WebFluxLinkBuilder.linkTo(
                WebFluxLinkBuilder.methodOn(ImageController.class)
                    .get(
                        null,
                        t.getContent()
                            .getIntervallType(),
                        t.getContent()
                            .getMinDate(),
                        t.getContent()
                            .getMaxDate()))
                .withRel(LinkRelation.of("_imgs"))
                .toMono();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private EntityModel<MinMaxDatesDto> update(Tuple2<EntityModel<MinMaxDatesDto>, Link> t) {
        return t.getT1()
            .add(t.getT2());
    }

    public Flux<EntityModel<MinMaxDatesDto>> toFlux(Flux<MinMaxDatesDto> entities, ServerWebExchange exchange) {
        return entities.flatMap(entity -> this.toReactiveEntityModel(entity, exchange));
    }

    public Flux<EntityModel<MinMaxDatesDto>> toFlux(
        Flux<MinMaxDatesDto> entities,
        Page<?> page,
        Mono<Link> baseLink,
        ServerWebExchange exchange
    ) {
        return entities.flatMap((entity) -> this.toReactiveEntityModel(entity, exchange));
    }

    /**
     * Returns a default URI string either from the one configured on assembler
     * creatino or by looking it up from the current request.
     *
     * @return
     */
    private UriTemplate getUriTemplate(Link baseLink) { return UriTemplate.of(baseLink.getHref()); }

    /**
     * Creates a {@link Link} with the given {@link LinkRelation} that will be based
     * on the given {@link UriTemplate} but enriched with the values of the given
     * {@link Pageable} (if not {@literal null}).
     *
     * @param base
     *            must not be {@literal null}.
     * @param pageable
     *            can be {@literal null}
     * @param relation
     *            must not be {@literal null}.
     * @return
     */
    private Link createLink(UriTemplate base, Pageable pageable, LinkRelation relation) {

        UriComponentsBuilder builder = UriComponentsBuilder.fromUri(base.expand());
        this.pageableResolver.enhance(builder, this.getMethodParameter(), pageable);

        return Link.of(
            UriTemplate.of(
                builder.build()
                    .toString()),
            relation);
    }

    /**
     * Return the {@link MethodParameter} to be used to potentially qualify the
     * paging and sorting request parameters to. Default implementations returns
     * {@literal null}, which means the parameters will not be qualified.
     *
     * @return
     * @since 1.7
     */
    @Nullable
    protected MethodParameter getMethodParameter() { return null; }

    /**
     * Creates a new {@link PageMetadata} instance from the given {@link Page}.
     *
     * @param page
     *            must not be {@literal null}.
     * @return
     */
    private PageMetadata asPageMetadata(Page<?> page) {

        Assert.notNull(page, "Page must not be null!");
        int number = page.getNumber();
        return new PageMetadata(page.getSize(), number, page.getTotalElements(), page.getTotalPages());
    }

}
