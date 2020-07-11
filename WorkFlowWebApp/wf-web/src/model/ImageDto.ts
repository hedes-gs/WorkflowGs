/* tslint:disable */
/* eslint-disable */
// Generated using typescript-generator version 2.23.603 on 2020-05-29 18:45:41.
import { ObjectMapper, JsonProperty, CacheKey, Deserializer, Serializer } from 'json-object-mapper'
import 'reflect-metadata';
import { array } from 'prop-types';
import MomentTimezone from 'moment-timezone';
import { Moment } from 'moment'

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

@CacheKey("ArrayImageDtoLinksSerializerDeserializer")
class ArrayImageDtoLinksSerializerDeserializer implements Deserializer, Serializer {

    deserialize = (json: string): ImageDto[] => {
        return ObjectMapper.deserializeArray(ImageDto, json);
    }
    serialize = (json: ImageDto[]): string => {
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
    @JsonProperty({ type: MomentType, deserializer: MomentSerializerDeserializer, serializer: MomentSerializerDeserializer })
    creationDate?: Moment | null = null;
    version: number = 0;
    imageId: string = '';
}

export class DefaultLink {
    self?: PageLink | null = null;
}

export class ExifsLink extends DefaultLink {
    _img?: PageLink | null = null;
    _prev?: PageLink | null = null;
    _next?: PageLink | null = null;
}

export class ImageLinks extends DefaultLink {
    _exif?: PageLink | null = null;
    _img?: PageLink | null = null;
    _upd?: PageLink | null = null;
}

export class ImageDto {

    @JsonProperty({ type: ImageKeyDto })
    data?: ImageKeyDto | null = null;
    imageName: string = '';
    creationDateAsString: string = '';
    importDate: Date | null = null;
    thumbnailWidth: number = 0;
    thumbnailHeight: number = 0;
    originalWidth: number = 0;
    originalHeight: number = 0;
    caption: string = '';
    aperture: string = '';
    speed: string = '';
    iso: string = '';
    keywords: string[] = [];
    albums: string[] = [];
    camera: string = '';
    lens: string = '';
    creationDate: Date | null = null;
    imageId: string = '';
    ratings: number = 0;
    @JsonProperty({ type: ImageLinks })
    _links?: ImageLinks | null = null;
    orientation: number = 0;
}


export class ExifDTO {
    @JsonProperty({ type: ImageKeyDto })
    imageOwner?: ImageKeyDto | null = null;
    path: number[] = [];
    displayableName: string = '';
    description: string = '';
    tagValue: number = 0;
    displayableValue: string = '';
    _links?: DefaultLink | null = null;
}

export class ExifDToes {
    @JsonProperty({ type: ExifDTO, deserializer: ArrayExifDTOSerializerDeserializer, serializer: ArrayExifDTOSerializerDeserializer })
    exifDToes: ExifDTO[] = [];
}

export class ExifOfImages {
    @JsonProperty({ type: ExifDToes })
    _embedded?: ExifDToes | null = null;
    @JsonProperty({ type: ExifsLink })
    _links?: ExifsLink | null = null;
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
    first: PageLink | null = null;
    last: PageLink | null = null;
    next: PageLink | null = null;
    self: PageLink | null = null;
    prev: PageLink | null = null;
}


export class PageContent {
    @JsonProperty({ type: ImageDto, deserializer: ArrayImageDtoLinksSerializerDeserializer, serializer: ArrayImageDtoLinksSerializerDeserializer })
    imageDtoes: ImageDto[] = [];
}


export class PageOfImageDto {
    @JsonProperty({ type: PageInformation })
    page?: PageInformation | null = null;
    @JsonProperty({ type: PageContent })
    _embedded?: PageContent | null = null;
    @JsonProperty({ type: PageLinks })
    _links: PageLinks | null = null;
}

export function toImageDto(json: string): ImageDto[] {
    let imgDtos: ImageDto[] = ObjectMapper.deserializeArray(ImageDto, json);
    return imgDtos;
}

export function toPageOfImageDto(json: string): PageOfImageDto {
    let imgDtos: PageOfImageDto = ObjectMapper.deserialize(PageOfImageDto, json);
    return imgDtos;
}

export function toSingleImageDto(json: string): ImageDto {
    let imgDto: ImageDto = ObjectMapper.deserialize(ImageDto, json);
    return imgDto;
}

export function toExif(json: string): ExifOfImages {
    let exifs: ExifOfImages = ObjectMapper.deserialize(ExifOfImages, json);
    return exifs;
}

export function toJsonImageDto(event: ImageDto) {
    return ObjectMapper.serialize(event);
}

export function toMap(json: string): Map<string, number> {
    const deser: MapDeserializer = new MapDeserializer();
    return deser.deserialize(json);
}

export function toArrayOfString(json: string): string[] {
    let retValue: String[] = ObjectMapper.deserializeArray(String, json);
    return retValue.map((s) => s.toString());
}
