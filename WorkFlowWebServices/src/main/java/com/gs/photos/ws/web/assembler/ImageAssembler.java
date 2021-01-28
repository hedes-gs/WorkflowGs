package com.gs.photos.ws.web.assembler;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.HateoasPageableHandlerMethodArgumentResolver;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.IanaLinkRelations;
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
import com.gs.photos.ws.controllers.ImageExifController;
import com.workflow.model.dtos.ImageDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@Component
public class ImageAssembler implements SimpleReactiveRepresentationModelAssembler<ImageDto> {

    protected static Logger                                    LOGGER = LoggerFactory.getLogger(ImageAssembler.class);
    private final HateoasPageableHandlerMethodArgumentResolver pageableResolver;
    private final Optional<UriComponents>                      baseUri;

    @Override
    public Mono<EntityModel<ImageDto>> toModel(ImageDto entity, ServerWebExchange exchange) {
        EntityModel<ImageDto> resource = EntityModel.of(entity);
        return Mono.just(this.addLinks(resource, exchange));
    }

    public ImageAssembler() {
        this.pageableResolver = new HateoasPageableHandlerMethodArgumentResolver();
        this.baseUri = Optional.ofNullable(
            UriComponentsBuilder.fromOriginHeader("/api")
                .build());
    }

    public EntityModel<ImageDto> toEntityModel(ImageDto entity) {
        EntityModel<ImageDto> entityModel = EntityModel.of(entity);
        entityModel = this.addLinks(entityModel, null);
        return entityModel;
    }

    public Mono<EntityModel<ImageDto>> toReactiveEntityModel(ImageDto entity) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageController.class)
                .checkout(
                    entity.getData()
                        .getSalt(),
                    entity.getData()
                        .getImageId(),
                    entity.getData()
                        .getCreationDate(),
                    entity.getData()
                        .getVersion()))
            .withRel(LinkRelation.of("_checkout"))
            .toMono()
            .map(
                (t) -> EntityModel.of(entity)
                    .add(t))
            .zipWhen((t) -> this.getLinkForImg(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForSelfRel(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForUpd(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForExif(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForNext(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForPrev(t))
            .map((t) -> this.update(t));
    }

    public Mono<EntityModel<ImageDto>> toEntityModel(ImageDto entity, Page<?> page, Mono<Link> baseLink) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageController.class)
                .checkout(
                    entity.getData()
                        .getSalt(),
                    entity.getData()
                        .getImageId(),
                    entity.getData()
                        .getCreationDate(),
                    entity.getData()
                        .getVersion()))
            .withRel(LinkRelation.of("_checkout"))
            .toMono()
            .map(
                (t) -> EntityModel.of(entity)
                    .add(t))
            .zipWhen((t) -> this.getLinkForImg(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForSelfRel(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForUpd(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForExif(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForNext(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> this.getLinkForPrev(t))
            .map((t) -> this.update(t))
            .zipWhen((t) -> baseLink)
            .map((t) -> this.updatePaginationLinks(t, page));
    }

    private EntityModel<ImageDto> updatePaginationLinks(Tuple2<EntityModel<ImageDto>, Link> t, Page<?> page) {
        return this.addPaginationLinks(t.getT1(), page, t.getT2());
    }

    private EntityModel<ImageDto> update(Tuple2<EntityModel<ImageDto>, Link> t) { return t.getT1()
        .add(t.getT2()); }

    public Flux<EntityModel<ImageDto>> toFlux(Flux<ImageDto> entities, ServerWebExchange exchange) {
        return entities.flatMap(entity -> this.toModel(entity, exchange));
    }

    public Flux<EntityModel<ImageDto>> toFlux(
        Flux<ImageDto> entities,
        Page<?> page,
        Mono<Link> baseLink,
        ServerWebExchange exchange
    ) {
        return entities.flatMap((entity) -> this.toEntityModel(entity, page, baseLink));
    }

    private Mono<Link> getLinkForImg(EntityModel<ImageDto> entity) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageController.class)
                .getImageWithMediaType(
                    entity.getContent()
                        .getData()
                        .getSalt(),
                    entity.getContent()
                        .getData()
                        .getImageId(),
                    entity.getContent()
                        .getData()
                        .getCreationDate(),
                    entity.getContent()
                        .getData()
                        .getVersion()))
            .withRel(LinkRelation.of("_img"))
            .toMono();
    }

    private Mono<Link> getLinkForSelfRel(EntityModel<ImageDto> entity) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageController.class)
                .getImageById(
                    entity.getContent()
                        .getData()
                        .getSalt(),
                    entity.getContent()
                        .getData()
                        .getImageId(),
                    entity.getContent()
                        .getData()
                        .getCreationDate(),
                    entity.getContent()
                        .getData()
                        .getVersion()))
            .withSelfRel()
            .toMono();
    }

    private Mono<Link> getLinkForUpd(EntityModel<ImageDto> entity) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageController.class)
                .updateRating(entity.getContent()))
            .withRel(LinkRelation.of("_upd"))
            .toMono();
    }

    private Mono<Link> getLinkForExif(EntityModel<ImageDto> entity) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageExifController.class)
                .getExifsByImageId(
                    entity.getContent()
                        .getData()
                        .getSalt(),
                    entity.getContent()
                        .getData()
                        .getImageId(),
                    entity.getContent()
                        .getData()
                        .getCreationDate(),
                    entity.getContent()
                        .getData()
                        .getVersion()))
            .withRel(LinkRelation.of("_exif"))
            .toMono();
    }

    private Mono<Link> getLinkForNext(EntityModel<ImageDto> entity) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageController.class)
                .getNextImageById(
                    entity.getContent()
                        .getData()
                        .getSalt(),
                    entity.getContent()
                        .getData()
                        .getImageId(),
                    entity.getContent()
                        .getData()
                        .getCreationDate(),
                    entity.getContent()
                        .getData()
                        .getVersion()))
            .withRel(LinkRelation.of("_next"))
            .toMono();
    }

    private Mono<Link> getLinkForPrev(EntityModel<ImageDto> entity) {
        return WebFluxLinkBuilder.linkTo(
            WebFluxLinkBuilder.methodOn(ImageController.class)
                .getPreviousImageById(
                    entity.getContent()
                        .getData()
                        .getSalt(),
                    entity.getContent()
                        .getData()
                        .getImageId(),
                    entity.getContent()
                        .getData()
                        .getCreationDate(),
                    entity.getContent()
                        .getData()
                        .getVersion()))
            .withRel(LinkRelation.of("_prev"))
            .toMono();
    }

    protected EntityModel<ImageDto> addPaginationLinks(EntityModel<ImageDto> resources, Page<?> page, Link link) {
        UriTemplate base = this.getUriTemplate(link);
        boolean isNavigable = page.hasPrevious() || page.hasNext();
        if (isNavigable) {
            resources
                .add(this.createLink(base, PageRequest.of(0, page.getSize(), page.getSort()), IanaLinkRelations.FIRST));
        }
        if (page.hasPrevious()) {
            resources.add(this.createLink(base, page.previousPageable(), IanaLinkRelations.PREV));
        }
        resources.add(link);

        if (page.hasNext()) {
            resources.add(this.createLink(base, page.nextPageable(), IanaLinkRelations.NEXT));
        }
        if (isNavigable) {
            int lastIndex = page.getTotalPages() == 0 ? 0 : page.getTotalPages() - 1;
            resources.add(
                this.createLink(
                    base,
                    PageRequest.of(lastIndex, page.getSize(), page.getSort()),
                    IanaLinkRelations.LAST));
        }
        return resources;
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
