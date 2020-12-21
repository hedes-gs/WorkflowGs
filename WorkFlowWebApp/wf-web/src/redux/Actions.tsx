import Actions from "./ActionsType";
import { ThunkAction, ThunkDispatch } from 'redux-thunk'
import ApplicationState from "./State";
import {
    ImageDto, toImageDto, toMetadataDto,
    ExifOfImages, toExif, toPageOfImageDto,
    toSingleImageDto, PageOfImageDto, ImageKeyDto,
    toMap, Metadata
} from '../model/ImageDto';
import ImagesServiceImpl, { ImagesService } from "../services/ImagesService";
import ExifImagesServiceImpl, { ExifImagesService } from "../services/ExifImagesService";

import MomentTimezone from 'moment-timezone';
import RatingsServiceImpl, { RatingsService } from "../services/RatingsServices";
import KeywordsServiceImpl, { KeywordsService } from "../services/KeywordsServices";
import PersonsServiceImpl, { PersonsService } from "../services/PersonsServices";
import WfEventsServicesImpl, { WfEventsServices } from '../services/EventsServices'
import { ImportEvent } from "../model/WfEvents";



const imagesService: ImagesService = new ImagesServiceImpl();
const ratingsService: RatingsService = new RatingsServiceImpl();
const exifImagesService: ExifImagesService = new ExifImagesServiceImpl();
const keywordService: KeywordsService = new KeywordsServiceImpl();
const personService: PersonsService = new PersonsServiceImpl();
const wfEventsServices: WfEventsServices = new WfEventsServicesImpl();

export type ThunkResult<R> = ThunkAction<R, ApplicationState, undefined, ApplicationEvent>;
export type ApplicationThunkDispatch = ThunkDispatch<ApplicationState, undefined, ApplicationEvent>;
export const UnknownSelectedEventValue = "0";
export const PayloadIntervalDatesSelectedEvent = "1";
export const PayloadLoadedImagesEvent = '2'
export const SaveImageEvent = '12'
export const AddKeywordEvent = '15'
export const DeleteKeywordEvent = '17'

export const SelectedImageEvent = '16';
export const LoadImagesOfMetadataEvent = '20';
export const LoadPagesOfImagesEvent = '21'
export const DownloadSelectedImageEvent = '22'
export const AddPersonEvent = '23'
export const DeletePersonEvent = '24'
export const LoadAllPersonsEvent = '25'
export const AllPersonsAreLoadedEvent = '26'
export const DisplayRealTimeImagesEvent = '27'



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
        images: PageOfImageDto,
        titleOfImagesList: string
    }
}

export interface AddImageToDeleteEvent {
    payloadType: "3",
    type: Actions,
    payload: {
        image: ImageKeyDto
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
    payloadType: "7",
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
        img: ImageDto
    }
}

export interface ExifsAreLoadingEvent {
    payloadType: "11",
    type: Actions,
    payload: {
        imageOwner: ImageDto
    }
}

export interface SaveImageEvent {
    payloadType: typeof SaveImageEvent,
    type: Actions,
    payload: {
        url: string,
        img: ImageDto
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
        image: ImageDto,
        keyword: string
    }
}

export interface AddPersonEvent {
    payloadType: typeof AddPersonEvent,
    type: Actions,
    payload: {
        image: ImageDto,
        person: string
    }
}

export interface DeletePersonEvent {
    payloadType: typeof DeletePersonEvent,
    type: Actions,
    payload: {
        image: ImageDto,
        person: string
    }
}

export interface SelectedImageEvent {
    payloadType: typeof SelectedImageEvent,
    type: Actions,
    payload: {
        url?: string | null,
        isLoading: boolean,
        exifUrl?: string | null,
        image?: ImageDto | null
    }
}

export interface DeleteKeywordEvent {
    payloadType: typeof DeleteKeywordEvent,
    type: Actions,
    payload: {
        image: ImageDto,
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
        image: ImageDto
    }
}







export type ApplicationEvent =
    ImagesLoadedEvent |
    LastImagesAreLoadedEvent |
    SelectImageEvent |
    ExifsAreLoadedEvent |
    PayloadLoadedImagesEvent |
    PayloadIntervalDatesSelectedEvent |
    AddImageToDeleteEvent |
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
    DisplayRealTimeImagesEvent;

const DefaultUnknownSelectedEvent: UnknownSelectedEvent = {
    type: Actions.UNDEFINED,
    payloadType: UnknownSelectedEventValue,
    payload: {}
};

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
        type: Actions.IMAGES_LOADING,
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
            images: toPageOfImageDto(json),
            titleOfImagesList: titleOfImagesList
        }
    }
};

export const deleteImage = (img: ImageKeyDto): ApplicationEvent => {
    return {
        payloadType: "3",
        type: Actions.DELETE_IMAGE,
        payload: {
            image: img
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
        type: Actions.LAST_IMAGES_LOADING,
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
            img: toSingleImageDto(json)
        }
    }
};

export const updateImage = (url: string, img: ImageDto): ApplicationEvent => {
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

export const addKeywords = (img: ImageDto, keyword: string): ApplicationEvent => {
    return {
        payloadType: AddKeywordEvent,
        type: Actions.ADDING_KEYWORDS,
        payload: {
            image: img,
            keyword: keyword
        }
    }
};

export const addPerson = (img: ImageDto, person: string): ApplicationEvent => {
    return {
        payloadType: AddPersonEvent,
        type: Actions.ADDING_PERSONS,
        payload: {
            image: img,
            person: person
        }
    }
};


export const selectedImageIsLoading = (url: string, exifUrl: string): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY,
        payload: {
            url: url,
            isLoading: true,
            exifUrl: exifUrl
        }
    }
};

export const deselectImage = (): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.DESELECT_IMAGE_TO_DISPLAY,
        payload: {
            url: null,
            isLoading: false,
            exifUrl: null
        }
    }
};


export const selectedImageIsLoaded = (json: string): ApplicationEvent => {
    const imageDto = toSingleImageDto(json);
    return {
        payloadType: SelectedImageEvent,
        type: Actions.SELECTED_IMAGE_TO_DISPLAY_IS_LOADED,
        payload: {
            isLoading: false,
            image: imageDto,
            url: imageDto._links != null && imageDto._links.self != null ? imageDto._links.self.href : null,
            exifUrl: imageDto._links != null && imageDto._links._exif != null ? imageDto._links._exif.href : null
        }
    }
};


export const nextImageToLoad = (url: string): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY,
        payload: {
            url: url,
            isLoading: true
        }
    }
};

export const prevImageToLoad = (url: string): ApplicationEvent => {
    return {
        payloadType: SelectedImageEvent,
        type: Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY,
        payload: {
            url: url,
            isLoading: true
        }
    }
};

export const deleteKeywords = (img: ImageDto, keyword: string): ApplicationEvent => {
    return {
        payloadType: DeleteKeywordEvent,
        type: Actions.DELETE_KEYWORDS,
        payload: {
            image: img,
            keyword: keyword
        }
    }
};

export const deletePerson = (img: ImageDto, person: string): ApplicationEvent => {
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
        type: Actions.IMAGES_LOADING,
        payload: {
            url: url,
            titleOfImagesList: titleOfImagesList
        }
    }
};

export const downloadSelectedImage = (img: ImageDto): ApplicationEvent => {
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
        if (x.payloadType == '7') {
            dispatch(x);
            return imagesService.getLastImages(x.payload.pageNumber)
                .then(json => dispatch(loadImages(json, x.payload.titleOfImagesList)));

        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchNewSelectedDateImagesInterval(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == PayloadIntervalDatesSelectedEvent) {
            dispatch(x);
            return imagesService.getImagesByDate(MomentTimezone(x.payload.min), MomentTimezone(x.payload.max), x.payload.intervallType)
                .then(json => dispatch(loadImages(json, x.payload.titleOfImagesList)));

        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchPhotoToDelete(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == '3') {
            dispatch(x);
            return Promise.resolve(x);
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
        if (x.payloadType == SelectedImageEvent && x.type == Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY && x.payload.url != null && x.payload.exifUrl != null) {
            const promiseOfGetImage = imagesService.getImage(x.payload.url);
            const promiseOfGetExif = exifImagesService.getExifDataOfImage(x.payload.exifUrl);
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
        if (x.payloadType == SelectedImageEvent && x.payload.url != null) {
            return imagesService.getNextImage(x.payload.url)
                .then(json => {
                    const imageDto = toSingleImageDto(json);
                    const exifUrl = imageDto._links != null && imageDto._links._exif != null ? imageDto._links._exif.href : '';
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
        if (x.payloadType == SelectedImageEvent && x.payload.url != null) {
            return imagesService.getPrevImage(x.payload.url)
                .then(json => dispatch(selectedImageIsLoaded(json)))
                .then(img => img.payloadType == SelectedImageEvent && img.payload.exifUrl != null ? exifImagesService.getExifDataOfImage(img.payload.exifUrl) : '')
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


export function dispatchDeleteKeywordEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DeleteKeywordEvent) {
            dispatch(x);
            if (x.payload.image._links != null && x.payload.image._links._img != null && x.payload.image._links._exif != null) {
                dispatch(selectedImageIsLoading(x.payload.image._links._img.href, x.payload.image._links._exif.href))
                return keywordService.deleteKeyword(x.payload.keyword, x.payload.image)
                    .then(json => dispatch(selectedImageIsLoaded(json)));
            }
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchDeletePersonEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == DeletePersonEvent) {
            dispatch(x);
            if (x.payload.image._links != null && x.payload.image._links._img != null && x.payload.image._links._exif != null) {
                dispatch(selectedImageIsLoading(x.payload.image._links._img.href, x.payload.image._links._exif.href))
                return personService.deletePerson(x.payload.person, x.payload.image)
                    .then(json => dispatch(selectedImageIsLoaded(json)));
            }
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}


export function dispatchDeselectImageEvent(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == '16') {
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
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LoadImagesOfMetadataEvent) {
            dispatch(x);
            return imagesService.getPageOfImages(x.payload.url)
                .then(json => dispatch(loadImages(json, x.payload.titleOfImagesList)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}



export function dispatchNextPage(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LoadPagesOfImagesEvent) {
            dispatch(x);
            return imagesService.getPageOfImages(x.payload.url)
                .then(json => dispatch(loadImages(json, x.payload.titleOfImagesList)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
}

export function dispatchPrevPage(x: ApplicationEvent): ThunkResult<Promise<ApplicationEvent>> {
    return async (dispatch: ApplicationThunkDispatch, getState) => {
        if (x.payloadType == LoadPagesOfImagesEvent) {
            dispatch(x);
            return imagesService.getPageOfImages(x.payload.url)
                .then(json => dispatch(loadImages(json, x.payload.titleOfImagesList)))
        }
        return Promise.resolve(DefaultUnknownSelectedEvent);
    };
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



