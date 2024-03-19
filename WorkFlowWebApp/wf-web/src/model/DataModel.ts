/* tslint:disable */
/* eslint-disable */
// Generated using typescript-generator version 2.23.603 on 2020-05-29 18:45:41.
import { ObjectMapper, JsonProperty, CacheKey, Deserializer, Serializer } from 'json-object-mapper'
import 'reflect-metadata';
import { array } from 'prop-types';
import MomentTimezone from 'moment-timezone';


import { Moment } from 'moment-timezone';




class MapDeserializer implements Deserializer {
    deserialize = (value: any): any => {
        let mapToReturn: Map<String, number> = new Map<String, number>();
        if (value) {
            Object.keys(value).forEach((key: String) => {
                mapToReturn.set(key, value['' + key]);
            });
        }
        return mapToReturn;
    }
}

@CacheKey("ArrayImageLinksSerializerDeserializer")
class ArrayImageLinksSerializerDeserializer implements Deserializer, Serializer {

    deserialize = (json: string): ImageLinks[] => {
        return ObjectMapper.deserializeArray(ImageLinks, json);
    }
    serialize = (json: ImageLinks[]): string => {
        return ObjectMapper.serialize(json).toString();
    }
}

@CacheKey("ArrayExchangedImageDTOLinksSerializerDeserializer")
class ArrayExchangedImageDTOLinksSerializerDeserializer implements Deserializer, Serializer {

    deserialize = (json: string): ExchangedImageDTO[] => {
        return ObjectMapper.deserializeArray(ExchangedImageDTO, json);
    }
    serialize = (json: ExchangedImageDTO[]): string => {
        return ObjectMapper.serialize(json).toString();
    }
}

@CacheKey("ArrayMetadataSerializerDeserializer")
class ArrayMetadataSerializerDeserializer implements Deserializer, Serializer {

    deserialize = (json: string): Metadata[] => {
        return ObjectMapper.deserializeArray(Metadata, json);
    }
    serialize = (json: Metadata[]): string => {
        return ObjectMapper.serialize(json).toString();
    }
}


@CacheKey("MomentSerializerDeserializer")
class MomentSerializerDeserializer implements Deserializer, Serializer {

    deserialize = (json: string): Moment => {
        return MomentTimezone(json, "YYYY-MM-DD HH:mm:ss +01");
    }
    serialize = (json: Moment): string => {
        return '"' + json.format("YYYY-MM-DD HH:mm:ss +01") + '"';
    }
}

@CacheKey("ArrayExifDTOSerializerDeserializer")
class ArrayExifDTOSerializerDeserializer implements Deserializer, Serializer {

    deserialize = (json: string): ExifDTO[] => {
        return ObjectMapper.deserializeArray(ExifDTO, json);
    }
    serialize = (json: ExifDTO[]): string => {
        return ObjectMapper.serialize(json).toString();
    }
}



export class PageLink {
    href: string = '';
}


export class MomentType {

}

export class ImageKeyDto {
    @JsonProperty({
        type: MomentType,
        deserializer: MomentSerializerDeserializer,
        serializer: MomentSerializerDeserializer
    })
    creationDate?: Moment ;
    version: number = 0;
    imageId: string = '';
}




export class DefaultLink {
    self?: PageLink ;
}

export class ExifsLink extends DefaultLink {
    _img?: PageLink ;
    _prev?: PageLink ;
    _next?: PageLink ;
}

export class MinMaxDatesLinks {
    _subinterval?: PageLink ;
    _imgs?: PageLink ;
}

export class ImageLinks extends DefaultLink {
    _exif?: PageLink ;
    _img?: PageLink ;
    _lowRes?: PageLink ;
    _upd?: PageLink ;
    _prev?: PageLink ;
    _next?: PageLink ;
    next?: PageLink ;
    prev?: PageLink ;
    first?: PageLink ;
    last?: PageLink ;
    self?: PageLink ;
    _checkout?: PageLink ;
    _del?: PageLink ;
}

export class MetadataLinks {
    _page?: PageLink ;
}

export class Metadata {
    content: string = '';
    _links?: MetadataLinks ;
}

export class StringsOfMetadata {
    @JsonProperty({
        type: Metadata,
        deserializer: ArrayMetadataSerializerDeserializer,
        serializer: ArrayMetadataSerializerDeserializer
    })
    strings: Metadata[] = [];
}

export class MetadataWrapper {
    _embedded?: StringsOfMetadata ;
}

export class MinMaxDatesDto {
    @JsonProperty({
        type: MomentType,
        deserializer: MomentSerializerDeserializer,
        serializer: MomentSerializerDeserializer
    })
    minDate?: Moment ;
    @JsonProperty({
        type: MomentType,
        deserializer: MomentSerializerDeserializer,
        serializer: MomentSerializerDeserializer
    })
    maxDate?: Moment ;
    countNumber: number = 0;
    intervallType: string = '';
    @JsonProperty({ type: MinMaxDatesLinks })
    _links?: MinMaxDatesLinks ;

}
export class ImageDto {

    @JsonProperty({ type: ImageKeyDto })
    data?: ImageKeyDto ;
    imageName: string = '';
    creationDateAsString: string = '';
    importDate?: Date ;
    thumbnailWidth: number = 0;
    thumbnailHeight: number = 0;
    originalWidth: number = 0;
    originalHeight: number = 0;
    caption: string = '';
    aperture: string = '';
    speed: string = '';
    iso: string = '';
    keywords: string[] = [];
    persons: string[] = [];
    albums: string[] = [];
    camera: string = '';
    lens: string = '';
    creationDate?: Date ;
    imageId: string = '';
    ratings: number = 0;
    orientation: number = 0;
}

export class ExchangedImageDTO {
    @JsonProperty({ type: ImageDto })
    image?: ImageDto ;
    currentPage: number = 0;
    pageSize: number  = 0;
    totalNbOfElements : number  = 0;
    @JsonProperty({
        type: MomentType,
        deserializer: MomentSerializerDeserializer,
        serializer: MomentSerializerDeserializer
    })
    minDate?: Moment ;
    @JsonProperty({
        type: MomentType,
        deserializer: MomentSerializerDeserializer,
        serializer: MomentSerializerDeserializer
    })
    maxDate?: Moment ;
    @JsonProperty({ type: ImageLinks })
    _links?: ImageLinks ;
}


export class ExifDTO {
    @JsonProperty({ type: ImageKeyDto })
    imageOwner?: ImageKeyDto ;
    path: number[] = [];
    displayableName: string = '';
    description: string = '';
    tagValue: number = 0;
    displayableValue: string = '';
    _links?: DefaultLink ;
}

export class ExifDToes {
    @JsonProperty({ type: ExifDTO, deserializer: ArrayExifDTOSerializerDeserializer, serializer: ArrayExifDTOSerializerDeserializer })
    exifDTOList: ExifDTO[] = [];
}

export class ExifOfImages {
    @JsonProperty({ type: ExifDToes })
    _embedded?: ExifDToes  ;
    @JsonProperty({ type: ExifsLink })
    _links?: ExifsLink  ;
}



export class PageInformation {
    size: number = 0;
    totalElements: number = 0;
    totalPages = 0;
    number: number = 0;
    @JsonProperty({ type: ImageLinks, deserializer: ArrayImageLinksSerializerDeserializer, serializer: ArrayImageLinksSerializerDeserializer })
    links: ImageLinks[] = [];
}

export class PageLinks {
    first?: PageLink ;
    last?: PageLink ;
    next?: PageLink ;
    self?: PageLink ;
    prev?: PageLink ;
}

export class ComponentEvent {
    dataId?: string ;
    status?: string ;
    componentType?: string ;
    message?: string ;
    scannedFolder?: string[] ;
    componentName?: string ;
}


export class PageContent {
    @JsonProperty({ type: ExchangedImageDTO, deserializer: ArrayExchangedImageDTOLinksSerializerDeserializer, serializer: ArrayExchangedImageDTOLinksSerializerDeserializer })
    ExchangedImageDTOList: ExchangedImageDTO[] = [];
}


export class PageOfExchangedImageDTO {
    @JsonProperty({ type: PageInformation })
    page?: PageInformation ;
    @JsonProperty({ type: PageContent })
    _embedded?: PageContent ;
    @JsonProperty({ type: PageLinks })
    _links?: PageLinks ;
}

export function toExchangedImageDTO(json: string): ExchangedImageDTO[] {
    let imgDtos: ExchangedImageDTO[] = ObjectMapper.deserializeArray(ExchangedImageDTO, json);
    return imgDtos;
}

export function toMetadataDto(json: string): Metadata[] {
    let metadata: MetadataWrapper = ObjectMapper.deserialize(MetadataWrapper, json);
    return metadata._embedded != null ? metadata._embedded.strings : [];
}


export function toPageOfExchangedImageDTO(json: string): PageOfExchangedImageDTO {
    let imgDtos: PageOfExchangedImageDTO = ObjectMapper.deserialize(PageOfExchangedImageDTO, json);
    return imgDtos;
}

export function toSingleExchangedImageDTO(json: string): ExchangedImageDTO {
    let imgDto: ExchangedImageDTO = ObjectMapper.deserialize(ExchangedImageDTO, json);
    return imgDto;
}

export function toSingleMinMaxDto(json: string): MinMaxDatesDto {
    let minMaxDatesDto: MinMaxDatesDto = ObjectMapper.deserialize(MinMaxDatesDto, json);
    return minMaxDatesDto;
}


export function toExif(json: string): ExifOfImages {
    let exifs: ExifOfImages = ObjectMapper.deserialize(ExifOfImages, json);
    return exifs;
}

export function toJsonExchangedImageDTO(event: ExchangedImageDTO) {
    return ObjectMapper.serialize(event);
}

export function toJsonImageDTO(event?: ImageDto) {
    if ( event != null ) {
        return ObjectMapper.serialize(event);
    }
    return '';
}


export function toMap(json: string): Map<string, number> {
    const deser: MapDeserializer = new MapDeserializer();
    return deser.deserialize(json);
}

export function toArrayOfString(json: string): string[] {
    let retValue: String[] = ObjectMapper.deserializeArray(String, json);
    return retValue.map((s) => s.toString());
}

export function toArrayOfComponentEvent(json: string): ComponentEvent[] {
    let retValue: ComponentEvent[] = ObjectMapper.deserializeArray(ComponentEvent, json);
    return retValue;
}
export function toComponentEvent(json: string): ComponentEvent {
    let retValue: ComponentEvent = ObjectMapper.deserialize(ComponentEvent, json);
    return retValue;
}

