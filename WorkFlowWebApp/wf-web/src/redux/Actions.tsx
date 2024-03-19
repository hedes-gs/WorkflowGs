import Actions from "./ActionsType";
import { ThunkAction, ThunkDispatch } from 'redux-thunk'
import ApplicationState from "./State";
import {
    ExchangedImageDTO, 
    toExchangedImageDTO, 
    toMetadataDto,
    ExifOfImages, 
    toExif, 
    toPageOfExchangedImageDTO,
    toSingleExchangedImageDTO, 
    PageOfExchangedImageDTO,
    ImageKeyDto,
    toMap, Metadata, toSingleMinMaxDto, MinMaxDatesDto
} from '../model/DataModel';
import ImagesServiceImpl, { ImagesService } from "../services/ImagesService";
import ExifImagesServiceImpl, { ExifImagesService } from "../services/ExifImagesService";
import LimitDatesServiceImpl, { LimitDatesService } from "../services/LimitDates";

import MomentTimezone from 'moment-timezone';
import RatingsServiceImpl, { RatingsService } from "../services/RatingsServices";
import KeywordsServiceImpl, { KeywordsService } from "../services/KeywordsServices";
import PersonsServiceImpl, { PersonsService } from "../services/PersonsServices";
import AlbumsServiceImpl, { AlbumsService } from "../services/AlbumsServices";
import WfEventsServicesImpl, { WfEventsServices } from '../services/EventsServices'
import { ImportEvent } from "../model/WfEvents";



const imagesService: ImagesService = new ImagesServiceImpl();
const ratingsService: RatingsService = new RatingsServiceImpl();
const exifImagesService: ExifImagesService = new ExifImagesServiceImpl();
const keywordService: KeywordsService = new KeywordsServiceImpl();
const albumService: AlbumsService = new AlbumsServiceImpl();
const personService: PersonsService = new PersonsServiceImpl();
const wfEventsServices: WfEventsServices = new WfEventsServicesImpl();
const limitDatesService: LimitDatesService = new LimitDatesServiceImpl();

export type ThunkResult<R> = ThunkAction<R, ApplicationState, undefined, ApplicationEvent>;
export type ApplicationThunkDispatch = ThunkDispatch<ApplicationState, undefined, ApplicationEvent>;
export const UnknownSelectedEventValue = "0";
export const PayloadIntervalDatesSelectedEvent = "1";
export const PayloadLoadedImagesEvent = '2'
export const ImageToDeleteEvent = '3'
export const LastImagesAreLoadedEvent = '7'
export const SaveImageEvent = '12'
export const AddKeywordEvent = '15'
export const SelectedImageEvent = '16';
export const DeleteKeywordEvent = '17'
export const LoadImagesOfMetadataEvent = '20';
export const LoadPagesOfImagesEvent = '21'
export const DownloadSelectedImageEvent = '22'
export const AddPersonEvent = '23'
export const DeletePersonEvent = '24'
export const LoadAllPersonsEvent = '25'
export const AllPersonsAreLoadedEvent = '26'
export const DisplayRealTimeImagesEvent = '27'
export const DownloadImageEvent = '28'
export const GetAllDatesOfImagesEvent = '29'
export const AddAlbumEvent = '30' ;
export const DeleteAlbumEvent = '31' ;
export const ImageToDeleteImmediatelyEvent = '32';
export const ImageToDownloadImmediatelyEvent = '32';



export interface GetAllDatesOfImagesEvent {
    payloadType: typeof GetAllDatesOfImagesEvent,
    type: Actions,
    payload: {
        url?: string,
        urlToGetfirstPage?: string,
        data?: MinMaxDatesDto[]
    }
}

export interface DownloadImageEvent {
    payloadType: typeof DownloadImageEvent,
    type: Actions,
    payload: {
        img: ExchangedImageDTO
    }
}

export interface DisplayRealTimeImagesEvent {
    payloadType: typeof DisplayRealTimeImagesEvent,
    type: Actions,
    payload: {
        importEvent: ImportEvent,
        isLoading: boolean
    }
}
export interface UnknownSelectedEvent {
    payloadType: typeof UnknownSelectedEventValue,
    type: Actions,
    payload: {}
}
export interface PayloadIntervalDatesSelectedEvent {
    payloadType: typeof PayloadIntervalDatesSelectedEvent,
    type: Actions,
    payload: {
        min: number,
        max: number,
        intervallType: string,
        titleOfImagesList: string
    }
}
export interface PayloadLoadedImagesEvent {
    payloadType: typeof PayloadLoadedImagesEvent,
    type: Actions,
    payload: {
        images: PageOfExchangedImageDTO,
        titleOfImagesList: string
    }
}

export interface ImageToDeleteEvent {
    payloadType: typeof ImageToDeleteEvent,
    type: Actions,
    payload: {
        img: ExchangedImageDTO
    }
}

export interface ImageToDeleteImmediatelyEvent {
    payloadType: typeof ImageToDeleteImmediatelyEvent,
    type: Actions,
    payload: {
        img: ExchangedImageDTO
    }
}

export interface ImageToDownloadImmediatelyEvent {
    payloadType: typeof ImageToDownloadImmediatelyEvent,
    type: Actions,
    payload: {
        img: ExchangedImageDTO
    }
}



export interface UndoImageToDeleteEvent {
    payloadType: "4",
    type: Actions,
    payload: {
        image: ImageKeyDto
    }
}

export interface SelectImageEvent {
    payloadType: "5",
    type: Actions,
    payload: {
        url: string,
        image: ImageKeyDto
    }
}

export interface ExifsAreLoadedEvent {
    payloadType: "6",
    type: Actions,
    payload: {
        exifs: ExifOfImages
    }
}

export interface LastImagesAreLoadedEvent {
    payloadType: typeof LastImagesAreLoadedEvent,
    type: Actions,
    payload: {
        pageNumber: number,
        titleOfImagesList: string
    }
}

export interface NextImageToLoadEvent {
    payloadType: "8",
    type: Actions,
    payload: {
        url: string
    }
}

export interface PrevImageToLoadEvent {
    payloadType: "9",
    type: Actions,
    payload: {
        url: string
    }
}

export interface ImagesLoadedEvent {
    payloadType: "10",
    type: Actions,
    payload: {
        img: ExchangedImageDTO
    }
}

export interface ExifsAreLoadingEvent {
    payloadType: "11",
    type: Actions,
    payload: {
        imageOwner: ExchangedImageDTO
    }
}

export interface SaveImageEvent {
    payloadType: typeof SaveImageEvent,
    type: Actions,
    payload: {
        url: string,
        img: ExchangedImageDTO
    }
}

export interface RatingsLoadedEvent {
    payloadType: "13",
    type: Actions,
    payload: {
        ratings: Map<string, number>;
    }
}
export interface RatingsLoadingEvent {
    payloadType: "14",
    type: Actions,
    payload: {

    }
}
export interface AddKeywordEvent {
    payloadType: typeof AddKeywordEvent,
    type: Actions,
    payload: {
        image: ExchangedImageDTO,
        keyword: string
    }
}

export interface AddPersonEvent {
    payloadType: typeof AddPersonEvent,
    type: Actions,
    payload: {
        image: ExchangedImageDTO,
        person: string
    }
}

export interface AddAlbumEvent {
    payloadType: typeof AddAlbumEvent,
    type: Actions,
    payload: {
        image: ExchangedImageDTO,
        album: string
    }
}

export interface DeletePersonEvent {
    payloadType: typeof DeletePersonEvent,
    type: Actions,
    payload: {
        image: ExchangedImageDTO,
        person: string
    }
}

export interface DeleteAlbumEvent {
    payloadType: typeof DeleteAlbumEvent,
    type: Actions,
    payload: {
        image: ExchangedImageDTO,
        album: string
    }
}

export interface SelectedImageEvent {
    payloadType: typeof SelectedImageEvent,
    type: Actions,
    payload: {
        img?: ExchangedImageDTO | null
    }
}

export interface DeleteKeywordEvent {
    payloadType: typeof DeleteKeywordEvent,
    type: Actions,
    payload: {
        image: ExchangedImageDTO,
        keyword: string
    }
}

export interface LoadAllKeywordsEvent {
    payloadType: "18",
    type: Actions,
    payload: {
    }
}

export interface AllPersonsAreLoadedEvent {
    payloadType: typeof AllPersonsAreLoadedEvent,
    type: Actions,
    payload: {
        persons: Metadata[]
    }
}


export interface LoadAllPersonsEvent {
    payloadType: typeof LoadAllPersonsEvent,
    type: Actions,
    payload: {
    }
}

export interface AllKeywordsAreLoadedEvent {
    payloadType: "19",
    type: Actions,
    payload: {
        keywords: Metadata[]
    }
}

export interface LoadImagesOfMetadataEvent {
    payloadType: typeof LoadImagesOfMetadataEvent,
    type: Actions,
    payload: {
        url: string,
        titleOfImagesList: string
    }
}

export interface LoadPagesOfImagesEvent {
    payloadType: typeof LoadPagesOfImagesEvent,
    type: Actions,
    payload: {
        url: string,
        titleOfImagesList: string
    }
}

export interface DownloadSelectedImageEvent {
    payloadType: typeof DownloadSelectedImageEvent,
    type: Actions,
    payload: {
        image: ExchangedImageDTO
    }
}

export type ApplicationEvent =
    ImagesLoadedEvent |
    LastImagesAreLoadedEvent |
    SelectImageEvent |
    ExifsAreLoadedEvent |
    PayloadLoadedImagesEvent |
    PayloadIntervalDatesSelectedEvent |
    ImageToDeleteEvent |
    UndoImageToDeleteEvent |
    NextImageToLoadEvent |
    PrevImageToLoadEvent |
    UnknownSelectedEvent |
    ExifsAreLoadingEvent |
    SaveImageEvent |
    RatingsLoadingEvent |
    RatingsLoadedEvent |
    SelectedImageEvent |
    AddKeywordEvent |
    LoadAllKeywordsEvent |
    LoadAllPersonsEvent |
    AllKeywordsAreLoadedEvent |
    AllPersonsAreLoadedEvent |
    LoadImagesOfMetadataEvent |
    LoadPagesOfImagesEvent |
    DeleteKeywordEvent |
    DownloadSelectedImageEvent |
    AddPersonEvent |
    DeletePersonEvent |
    AddAlbumEvent |
    DeleteAlbumEvent |
    DisplayRealTimeImagesEvent |
    DownloadImageEvent |
    GetAllDatesOfImagesEvent |
    ImageToDeleteImmediatelyEvent |
    ImageToDownloadImmediatelyEvent;

const DefaultUnknownSelectedEvent: UnknownSelectedEvent = {
    type: Actions.UNDEFINED,
    payloadType: UnknownSelectedEventValue,
    payload: {}
};

export const getAllDatesOfImages = (url?: string, urlToGetfirstPage?: string): ApplicationEvent => {
    return {
        payloadType: GetAllDatesOfImagesEvent,
        type: Actions.DATES_OF_IMAGES,
        payload: {
            url: url,
            urlToGetfirstPage: urlToGetfirstPage
        }
    }

}

export const loadRealTimeImages = (loadingState: boolean, importEvent: ImportEvent): ApplicationEvent => {
    return {
        payloadType: DisplayRealTimeImagesEvent,
        type: Actions.IMAGES_LOADING,
        payload: {
            isLoading: loadingState,
            importEvent: importEvent
        }
    }
}

export const loadImagesInterval = (min: number, max: number, intervallType: string, titleOfImagesList: string): ApplicationEvent => {
    return {
        payloadType: PayloadIntervalDatesSelectedEvent,
        type: Actions.START_STREAMING,
        payload: {
            min,
            max,
            intervallType,
            titleOfImagesList: titleOfImagesList
        }
    }
};


export const loadImages = (json: string, titleOfImagesList: string): ApplicationEvent => {
    return {
        payloadType: PayloadLoadedImagesEvent,
        type: Actions.IMAGES_ARE_LOADED,
        payload: {
            images: toPageOfExchangedImageDTO(json),
            titleOfImagesList: titleOfImagesList
        }
    }
};

export const loadImagesAsJsonD = (json: string[], titleOfImagesList: string): ApplicationEvent => {
    const images: ExchangedImageDTO[] =  json.map((j) =>toSingleExchangedImageDTO(j)) ;
    const page: PageOfExchangedImageDTO = {
        _embedded: {
            ExchangedImageDTOList: images
        },
        _links: {
        }
    }
    return {
        payloadType: PayloadLoadedImagesEvent,
        type: Actions.IMAGES_ARE_STREAMED,
        payload: {
            images: page,
            titleOfImagesList: titleOfImagesList
        }
    }
};

export const loadDatesOfImagesAsJsonD = (json: string): ApplicationEvent => {
    //const jsons = json.split(/[\r\n]+/);
    // const data: MinMaxDatesDto[] = jsons.filter(j => j.length > 0).map(j => toSingleMinMaxDto(JSON.parse(j)))
    // console.log(' loadDatesOfImagesAsJsonD  ' + json)
    const data: MinMaxDatesDto[] = [toSingleMinMaxDto(json)];
    return {
        payloadType: GetAllDatesOfImagesEvent,
        type: Actions.DATES_OF_IMAGES_ARE_STREAMED,
        payload: {
            data: data
        }
    }
};

export const deleteImage = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: ImageToDeleteEvent,
        type: Actions.DELETE_IMAGE,
        payload: {
            img: img
        }
    }
};

export const deleteImmediatelyImage = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: ImageToDeleteImmediatelyEvent,
        type: Actions.DELETE_IMAGE,
        payload: {
            img: img
        }
    }
};




export const selectImage = (img: ImageKeyDto, url: string): ApplicationEvent => {
    return {
        payloadType: "5",
        type: Actions.SELECT_IMAGE,
        payload: {
            url: url,
            image: img
        }
    }
};

export const loadExif = (json: string): ApplicationEvent => {
    return {
        payloadType: "6",
        type: Actions.EXIF_ARE_LOADED,
        payload: {
            exifs: toExif(json)
        }
    }
};

export const loadLastImages = (pageNumber: number, titleOfImagesList: string): ApplicationEvent => {
    return {
        payloadType: "7",
        type: Actions.START_STREAMING,
        payload: {
            pageNumber: pageNumber,
            titleOfImagesList: titleOfImagesList
        }
    }
};


export const loadImage = (json: string): ApplicationEvent => {
    return {
        payloadType: "10",
        type: Actions.IMAGE_IS_LOADED,
        payload: {
            img: toSingleExchangedImageDTO(json)
        }
    }
};

export const updateImage = (url: string, img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: SaveImageEvent,
        type: Actions.SAVE_IMAGE,
        payload: {
            url: url,
            img: img
        }
    }
};

export const loadedRatings = (json: string): ApplicationEvent => {
    return {
        payloadType: "13",
        type: Actions.RATINGS_ARE_LOADED,
        payload: {
            ratings: toMap(json)
        }
    }
};


export const loadingRatings = (): ApplicationEvent => {
    return {
        payloadType: "14",
        type: Actions.RATINGS_ARE_LOADING,
        payload: {

        }
    }
};

export const addKeywords = (img: ExchangedImageDTO, keyword: string): ApplicationEvent => {
    return {
        payloadType: AddKeywordEvent,
        type: Actions.ADDING_KEYWORDS,
        payload: {
            image: img,
            keyword: keyword
        }
    }
};

export const addAlbum = (img: ExchangedImageDTO, album: string): ApplicationEvent => {
    return {
        payloadType: AddAlbumEvent,
        type: Actions.ALBUMS,
        payload: {
            image: img,
            album: album
        }
    }
};

export const deleteAlbum = (img: ExchangedImageDTO, album: string): ApplicationEvent => {
    return {
        payloadType: DeleteAlbumEvent,
        type: Actions.ALBUMS,
        payload: {
            image: img,
            album: album
        }
    }
};



export const addPerson = (img: ExchangedImageDTO, person: string): ApplicationEvent => {
    return {
        payloadType: AddPersonEvent,
        type: Actions.ADDING_PERSONS,
        payload: {
            image: img,
            person: person
        }
    }
};


export const selectedImageIsLoading = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY,
        payload: {
            img: img
        }
    }
};

export const deselectImage = (): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.DESELECT_IMAGE_TO_DISPLAY,
        payload: {
            img: null
        }
    }
};


export const selectedImageIsLoaded = (json: string): ApplicationEvent => {
    const ExchangedImageDTO = toSingleExchangedImageDTO(json);
    return {
        payloadType: SelectedImageEvent,
        type: Actions.SELECTED_IMAGE_TO_DISPLAY_IS_LOADED,
        payload: {
            img: ExchangedImageDTO
        }
    }
};


export const nextImageToLoad = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY,
        payload: {
            img: img
        }
    }
};

export const prevImageToLoad = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY,
        payload: {
            img: img
        }
    }
};

export const deleteKeywords = (img: ExchangedImageDTO, keyword: string): ApplicationEvent => {
    return {
        payloadType: DeleteKeywordEvent,
        type: Actions.DELETE_KEYWORDS,
        payload: {
            image: img,
            keyword: keyword
        }
    }
};

export const deletePerson = (img: ExchangedImageDTO, person: string): ApplicationEvent => {
    return {
        payloadType: DeletePersonEvent,
        type: Actions.DELETE_KEYWORDS,
        payload: {
            image: img,
            person: person
        }
    }
};

export const loadAllKeywords = (): ApplicationEvent => {
    return {
        payloadType: "18",
        type: Actions.LOAD_ALL_KEYWORDS,
        payload: {
        }
    }
};

export const loadAllPersons = (): ApplicationEvent => {
    return {
        payloadType: LoadAllPersonsEvent,
        type: Actions.LOAD_ALL_PERSONS,
        payload: {
        }
    }
};


export const allKeywordsAreLoaded = (json: string): ApplicationEvent => {
    return {
        payloadType: "19",
        type: Actions.ALL_KEYWORDS_ARE_LOADED,
        payload: {
            keywords: toMetadataDto(json)
        }
    }
};

export const allPersonsAreLoaded = (json: string): ApplicationEvent => {
    return {
        payloadType: AllPersonsAreLoadedEvent,
        type: Actions.ALL_PERSONS_ARE_LOADED,
        payload: {
            persons: toMetadataDto(json)
        }
    }
};

export const loadImagesOfMetadata = (url: string, titleOfImagesList: string): ApplicationEvent => {
    return {
        payloadType: LoadImagesOfMetadataEvent,
        type: Actions.LOAD_IMAGES_OF_METADATA,
        payload: {
            url: url,
            titleOfImagesList: titleOfImagesList
        }
    }
};

export const loadPagesOfImages = (url: string, titleOfImagesList: string): ApplicationEvent => {
    return {
        payloadType: LoadPagesOfImagesEvent,
        type: Actions.START_STREAMING,
        payload: {
            url: url,
            titleOfImagesList: titleOfImagesList
        }
    }
};

export const downloadImage = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: DownloadImageEvent,
        type: Actions.DOWNLOAD_IMAGE,
        payload: {
            img: img,
        }
    }
};

export const downloadImmediatelyImage = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: DownloadImageEvent,
        type: Actions.DOWNLOAD_IMAGE,
        payload: {
            img: img,
        }
    }
};




export const downloadSelectedImage = (img: ExchangedImageDTO): ApplicationEvent => {
    return {
        payloadType: DownloadSelectedImageEvent,
        type: Actions.IMAGES_LOADING,
        payload: {
            image: img
        }
    }
};



export function dispatchLoadRealtimeImages(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DisplayRealTimeImagesEvent) {
            return wfEventsServices.startScan(x.payload.importEvent).then((e) => dispatch(x))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchLoadRatings(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == '14') {
            dispatch(x);
            return ratingsService.countAll()
                .then(json => dispatch(loadedRatings(json)));

        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchLastImages(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LastImagesAreLoadedEvent) {
            dispatch(x);
            imagesService.getLastImages(x.payload.pageNumber)
            .then((r) => r.getReader())
            .then((reader) => getReadableStreamForImages(reader,dispatch,'Date page'))
        };
        return Promise.resolve(DefaultUnknownSelectedEvent);
    }
}

export function dispatchCheckoutImage(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DownloadImageEvent) {
            dispatch(x);
            imagesService.checkout(x.payload.img)
                .then((r) => console.log('Checkot done!'))
        };
        return Promise.resolve(DefaultUnknownSelectedEvent);
    }
}

export function dispatchDeleteImage(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == ImageToDeleteEvent) {
            dispatch(x);
            imagesService.delete(x.payload.img)
                .then((json) => {
                    console.log('delete done!');
                    const ExchangedImageDTO = toSingleExchangedImageDTO(json);
                    const exifUrl = ExchangedImageDTO?._links?._exif?.href;
                    dispatch(selectedImageIsLoaded(json));
                    return exifImagesService.getExifDataOfImage(exifUrl);
                } )
                .then(json => dispatch(loadExif(json)));
                 
        };
        return Promise.resolve(DefaultUnknownSelectedEvent);
    }
}




export function dispatchGetAllDatesOfImages(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == GetAllDatesOfImagesEvent) {
            dispatch(x);
            limitDatesService.getDatesOfImagesWithURL(x.payload.url)
                .then((r) => r.getReader())
                .then((reader) => getReadableStreamForDatesOfImages(reader, dispatch))
                .then(() => {
                    if (x.payload.urlToGetfirstPage != null) {
                        dispatch(loadPagesOfImages(x.payload.urlToGetfirstPage, 'Date page'));
                        imagesService.getPageOfImages(x.payload.urlToGetfirstPage)
                            .then((r) => r.getReader())
                            .then((reader) => getReadableStreamForImages(reader,dispatch,'Date page'))
                    } else {
                        dispatch(loadLastImages(1, 'Page'));
                        imagesService.getLastImages(1)
                            .then((r) => r.getReader())
                            .then((reader) => getReadableStreamForImages(reader,dispatch,'Date page'))
                    }
                })
        };
        return Promise.resolve(DefaultUnknownSelectedEvent);
    }
}

function getReadableStreamForDatesOfImages(reader: ReadableStreamDefaultReader<any>, dispatch: ApplicationThunkDispatch): ReadableStream<any> | PromiseLike<ReadableStream<any>> {
    return new ReadableStream({
        start(controller) {
            pump();
            async function pump() {
                return reader.read().then((r: any) => {
                    if (r.done) {
                        controller.close();
                        return;
                    }
                    controller.enqueue(r.value);
                    dispatch(loadDatesOfImagesAsJsonD(r.value));
                    pump();
                });
            }
        }
    });
}

function getReadableStreamForImages(reader: ReadableStreamDefaultReader<any>, dispatch: ApplicationThunkDispatch, title: string): ReadableStream<any> | PromiseLike<ReadableStream<any>> {
    return new ReadableStream({
        
        start(controller) {
            const buffer : string[] = [];
            pump();
            async function pump() {
                return reader.read().then((r: any) => {
                    if (r.done) {
                        controller.close();
                        dispatch(loadImagesAsJsonD(buffer,title));
                        return;
                    }
                    controller.enqueue(r.value);
                    buffer.push(r.value) ;
                    if ( buffer.length > 50 ) {
                        dispatch(loadImagesAsJsonD(buffer,title));
                        buffer.length =0 ;
                    }
                    pump();
                });
            }
        }
    });
}


export function dispatchPhotoToDelete(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == ImageToDeleteEvent) {
            dispatch(x);
            return Promise.resolve(x);
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchPhotoToDeleteImmediately(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == ImageToDeleteImmediatelyEvent) {
            return imagesService.delete(x.payload.img)
                .then(json => {
                    const ExchangedImageDTO = toSingleExchangedImageDTO(json);
                    const exifUrl = ExchangedImageDTO?._links?._exif?.href;
                    dispatch(selectedImageIsLoaded(json))
                    return exifImagesService.getExifDataOfImage(exifUrl)
                })
                .then(json => dispatch(loadExif(json)));
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchDownloadImmediatelySelectedImageEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == ImageToDownloadImmediatelyEvent) {
            return imagesService.delete(x.payload.img)
                .then(json => {
                    const ExchangedImageDTO = toSingleExchangedImageDTO(json);
                    const exifUrl = ExchangedImageDTO?._links?._exif?.href;
                    dispatch(selectedImageIsLoaded(json))
                    return exifImagesService.getExifDataOfImage(exifUrl)
                })
                .then(json => dispatch(loadExif(json)));
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}


export function dispatchUndoPhotoToDelete(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == '4') {
            dispatch(x);
            return Promise.resolve(x);
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchImageToSelect(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == SelectedImageEvent) {
            const promiseOfGetImage = imagesService.getImage(x.payload.img?._links?.self?.href);
            const promiseOfGetExif = exifImagesService.getExifDataOfImage(x.payload.img?._links?._exif?.href);
            return Promise.all([promiseOfGetImage, promiseOfGetExif]).then((a: string[]) => {
                dispatch(selectedImageIsLoaded(a[0]));
                return dispatch(loadExif(a[1]));
            });
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}



export function dispatchPhotoToNext(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == SelectedImageEvent) {
            return imagesService.getNextImage(x.payload.img?._links?._next?.href)
                .then(json => {
                    const ExchangedImageDTO = toSingleExchangedImageDTO(json);
                    const exifUrl = ExchangedImageDTO?._links?._exif?.href;
                    dispatch(selectedImageIsLoaded(json))
                    return exifImagesService.getExifDataOfImage(exifUrl)
                })
                .then(json => dispatch(loadExif(json)));
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchPhotoToPrevious(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == SelectedImageEvent) {
            return imagesService.getPrevImage(x.payload.img?._links?._prev?.href)
                .then(json => {
                    const ExchangedImageDTO = toSingleExchangedImageDTO(json);
                    const exifUrl = ExchangedImageDTO?._links?._exif?.href;
                    dispatch(selectedImageIsLoaded(json))
                    return exifImagesService.getExifDataOfImage(exifUrl)
                })
                .then(json => dispatch(loadExif(json)));
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchSaveEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == SaveImageEvent) {
            dispatch(x);
            return imagesService.saveImage(x.payload.url, x.payload.img)
                .then(json => dispatch(selectedImageIsLoaded(json)));
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchAddKeywordEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == AddKeywordEvent) {
            dispatch(x);
            return keywordService.addKeyword(x.payload.keyword, x.payload.image)
                .then(json => dispatch(selectedImageIsLoaded(json)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchAddPersonEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == AddPersonEvent) {
            dispatch(x);
            return personService.addPerson(x.payload.person, x.payload.image)
                .then(json => dispatch(selectedImageIsLoaded(json)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchAddAlbumEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == AddAlbumEvent) {
            dispatch(x);
            return albumService.addAlbum (x.payload.album, x.payload.image)
                .then(json => dispatch(selectedImageIsLoaded(json)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchDeleteAlbumEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DeleteAlbumEvent) {
            dispatch(x);
            return albumService.deleteAlbum (x.payload.album, x.payload.image)
                .then(json => dispatch(selectedImageIsLoaded(json)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchDeleteKeywordEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DeleteKeywordEvent) {
            dispatch(x);
            return keywordService.deleteKeyword(x.payload.keyword, x.payload.image)
                .then(json => dispatch(selectedImageIsLoaded(json)));
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchDeletePersonEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DeletePersonEvent) {
            dispatch(x);
            return personService.deletePerson(x.payload.person, x.payload.image)
                .then(json => dispatch(selectedImageIsLoaded(json)));
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}


export function dispatchDeselectImageEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == SelectedImageEvent) {
            dispatch(x);
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchLoadAllKeywords(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == '18') {
            dispatch(x);
            return keywordService.getAll()
                .then(json => dispatch(allKeywordsAreLoaded(json)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}
export function dispatchLoadAllPersons(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LoadAllPersonsEvent) {
            dispatch(x);
            return personService.getAll()
                .then(json => dispatch(allPersonsAreLoaded(json)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}


export function dispatchLoadImagesOfMetadata(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    /*    return async (dispatch: ApplicationThunkDispatch, getState) => {
            if (x.payloadType == LoadImagesOfMetadataEvent) {
                dispatch(x);
                return imagesService.getPageOfImages(x.payload.url)
                    .then(json => dispatch(loadImages(json, x.payload.titleOfImagesList)))
            }
            return Promise.resolve(DefaultUnknownSelectedEvent);
        };
    */

    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LoadPagesOfImagesEvent) {
            dispatch(x);
            imagesService.getPageOfImages(x.payload.url)
                .then((r) => r.getReader())
                .then((reader) => new ReadableStream({
                    start(controller) {
                        pump();
                        async function pump() {
                            return reader.read().then((r: any) => {
                                // When no more data needs to be consumed, close the stream
                                if (r.done) {
                                    controller.close();
                                    return;
                                }
                                // Enqueue the next data chunk into our target stream
                                controller.enqueue(r.value);
                                dispatch(loadImagesAsJsonD(r.value, 'Test !! '))
                                pump();
                            });
                        }
                    }
                })
                )
        };
        return Promise.resolve(DefaultUnknownSelectedEvent);
    }

}



export function dispatchNextPage(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LoadPagesOfImagesEvent) {
            dispatch(x);
            imagesService.getPageOfImages(x.payload.url)
                .then((r) => r.getReader())
                .then((reader) => getReadableStreamForImages(reader,dispatch,'Date page'))
        };
        return Promise.resolve(DefaultUnknownSelectedEvent);
    }

}

export function dispatchPrevPage(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LoadPagesOfImagesEvent) {
            dispatch(x);
            imagesService.getPageOfImages(x.payload.url)
            .then((r) => r.getReader())
            .then((reader) => getReadableStreamForImages(reader,dispatch,'Date page'))
        };
        return Promise.resolve(DefaultUnknownSelectedEvent);
    }

}

export function dispatchDownloadSelectedImageEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DownloadSelectedImageEvent) {
            dispatch(x);
            return Promise.resolve(x);
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}



