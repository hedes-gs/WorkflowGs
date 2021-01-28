package com.gs.photos.ws.web.assembler;

import java.io.IOException;
import java.util.Arrays;

import org.springframework.data.domain.Sort;
import org.springframework.data.web.HateoasPageableHandlerMethodArgumentResolver;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.LinkRelation;
import org.springframework.hateoas.server.mvc.RepresentationModelAssemblerSupport;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilderFactory;
import org.springframework.stereotype.Component;

import com.gs.photos.ws.controllers.GsPageRequest;
import com.gs.photos.ws.controllers.ImageController;;

@Component
public class PersonsAssembler extends RepresentationModelAssemblerSupport<String, EntityModel<String>> {
    static Class<EntityModel<String>> getClassForConstructor() {
        EntityModel<String> retValue = new EntityModel<String>(new String(), new Link[] {});
        Class<EntityModel<String>> classA = (Class<EntityModel<String>>) retValue.getClass();
        return classA;
    }

    private static final WebMvcLinkBuilderFactory          FACTORY = new WebMvcLinkBuilderFactory();

    // @Autowired
    // @Qualifier("GsHateoasPageableResolver")
    protected HateoasPageableHandlerMethodArgumentResolver pageableResolver;

    public PersonsAssembler() { super(ImageController.class,
        PersonsAssembler.getClassForConstructor()); }

    @Override
    public EntityModel<String> toModel(String entity) {
        PersonsAssembler.FACTORY.setUriComponentsContributors(Arrays.asList(this.pageableResolver));
        GsPageRequest p = GsPageRequest.builder()
            .withPage(1)
            .withSort(Sort.by("creationTime"))
            .withPageSize(100)
            .build();
        try {
            return new EntityModel<String>(entity,
                PersonsAssembler.FACTORY.linkTo(
                    WebMvcLinkBuilder.methodOn(ImageController.class)
                        .getImagesByPerson(p, entity))
                    .withRel(LinkRelation.of("_page")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}