import { ObjectMapper, JsonProperty, CacheKey, Deserializer, Serializer } from 'json-object-mapper'

export interface ImportEvent {
    dataId: string;
    keyWords: string[];
    scanners: string[];
    urlScanFolder: string;
    album: string;
    importDate: number;
    importName: string;
}

export function toJson(event: ImportEvent) {
    return ObjectMapper.serialize(event);
}
