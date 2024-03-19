import {
    PayloadIntervalDatesSelectedEvent,
    PayloadLoadedImagesEvent,
    ApplicationEvent,
    DownloadSelectedImageEvent,
    LoadImagesOfMetadataEvent,
    LoadAllPersonsEvent,
    DisplayRealTimeImagesEvent,
    LastImagesAreLoadedEvent,
    LoadPagesOfImagesEvent,
    SelectedImageEvent,
    ImageToDeleteEvent,
    GetAllDatesOfImagesEvent,
    DeleteKeywordEvent,
    DeleteAlbumEvent,
    AddAlbumEvent,
    DeletePersonEvent,
    AddPersonEvent,
    AddKeywordEvent
} from './Actions'
import ApplicationSate from './State'

import Actions from "./ActionsType";
import { ExchangedImageDTO, ImageKeyDto, MinMaxDatesDto } from '../model/DataModel';

const initialState: ApplicationSate = {
    lastIntervallRequested: {
        min: 0,
        max: 0,
        intervallType: '',
        state: 'unset'
    },
    imagesLoaded: {
        state: 'unset',
        images: null,
        urlNext: '',
        urlPrev: '',
        pageNumber: 1,
        titleOfImagesList: ''
    },
    imagesToDelete: {},
    imagesToDownload: {},
    displayedExif: {},
    displayedRatings: {
        ratings: new Map()
    },
    imageIsSelectedToBeDisplayed: {
        isLoading: false
    },
    displayKeywords: {
        state: '',
        keywords: []
    },
    displayPersons: {
        state: '',
        persons: []
    },
    realTimeSelected: {
        isLoading: false
    },
    imagesAreStreamed: {
        state: 'unset',
        images: null,
        urlNext: '',
        urlPrev: '',
        pageNumber: 1,
        titleOfImagesList: ''
    },
    datesOfImages: {}
};

export function reducerDisplayedExif(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {
    switch (action.type) {
        case Actions.SELECTED_IMAGE_TO_DISPLAY_IS_LOADED: {
            const ExchangedImageDTO = action.payloadType == SelectedImageEvent ? action.payload.img : null;
            const currentExifs = state.displayedExif.exifs;
            const exifs = currentExifs != null &&
                ExchangedImageDTO != null &&
                currentExifs._embedded != null &&
                currentExifs._embedded.exifDTOList[0].imageOwner != null &&
                ExchangedImageDTO.image?.data != null &&
                currentExifs._embedded.exifDTOList[0].imageOwner.imageId == ExchangedImageDTO.image?.data.imageId ? currentExifs : null;
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    displayedExif: {
                        imageOwner: ExchangedImageDTO,
                        exifs: exifs
                    },

                },
            );
            return returnedTarget;

        }
        case Actions.EXIF_ARE_LOADED: {
            const exifs = action.payloadType == '6' ? action.payload.exifs : null;
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    displayedExif: {
                        imageOwner: state.displayedExif.imageOwner,
                        exifs: exifs
                    },

                },
            );
            return returnedTarget;
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}


export function reducerMetadata(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {
    switch (action.payloadType) {
        case PayloadLoadedImagesEvent: {
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    displayPersons: {
                        state: 'METADATA_UNSELECTED',
                        persons: []
                    }
                }
            );
            return returnedTarget;

        }
        case LoadAllPersonsEvent: {
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    displayPersons: {
                        state: 'METADATA_SELECTED',
                        persons: []
                    }

                }
            );
            return returnedTarget;

        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}


export function reducerImagesList(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {

    switch (action.payloadType) {
        case '1': {
            const min = action.payload.min;
            const max = action.payload.max;
            const intervallType = action.payload.intervallType;
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: false,
                        image: null
                    },
                    lastIntervallRequested: {
                        state: 'LOADING',
                        min: min,
                        max: max,
                        intervallType: intervallType
                    }
                },
            );
            return returnedTarget;
        }
        case DisplayRealTimeImagesEvent: {
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: false,
                        image: null
                    },
                    realTimeSelected: {
                        isLoading: true
                    }
                },
            );
            return returnedTarget;

        }
        case LoadImagesOfMetadataEvent: {
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: false,
                        image: null
                    },
                    imagesLoaded: {
                        state: 'LOADING',
                        images: null,
                        urlNext: '',
                        urlPrev: '',
                        pageNumber: 1
                    },

                },
            );
            return returnedTarget;
        }
        case '7': {
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: false,
                        image: null
                    },
                    imagesLoaded: {
                        state: 'LOADING',
                        images: null,
                        urlNext: '',
                        urlPrev: '',
                        pageNumber: action.payload.pageNumber
                    },

                },
            );
            return returnedTarget;
        }
        case PayloadIntervalDatesSelectedEvent: {
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imagesLoaded: {
                        state: 'LOADED',
                        images: null,
                        pageNumber: state.imagesAreStreamed.pageNumber,
                        urlNext: null,
                        urlPrev: null,
                        titleOfImagesList: null
                    },
                },
            );
            return returnedTarget;
        }
        case PayloadLoadedImagesEvent: {
            switch (action.type) {
                case Actions.IMAGES_ARE_STREAMED:
                case Actions.START_STREAMING: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imagesLoaded: {
                                state: 'LOADED',
                                images: null,
                                pageNumber: state.imagesAreStreamed.pageNumber,
                                urlNext: null,
                                urlPrev: null,
                                titleOfImagesList: null
                            },
                        },
                    );
                    return returnedTarget;

                }
                case Actions.IMAGES_ARE_LOADED: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imageIsSelectedToBeDisplayed: {
                                isLoading: false,
                                image: null
                            },
                            lastIntervallRequested: {
                                state: 'LOADED'
                            },
                            imagesLoaded: {
                                state: 'LOADED',
                                images: action.payload.images,
                                pageNumber: action.payload.images.page != null ? action.payload.images.page.number : 1,
                                urlNext: action.payload.images._links != null && action.payload.images._links.next != null ? action.payload.images._links.next.href : '',
                                urlPrev: action.payload.images._links != null && action.payload.images._links.prev != null ? action.payload.images._links.prev.href : '',
                                titleOfImagesList: action.payload.titleOfImagesList
                            },

                        },
                    );
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}

export function reducerInformImagesAreLoaded(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {

    switch (action.type) {
        case Actions.START_STREAMING: {
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imagesLoaded: {
                        state: null,
                        images: null
                    }
                });
            return returnedTarget;
        }
        case Actions.IMAGES_ARE_LOADED: {
            switch (action.payloadType) {
                case PayloadLoadedImagesEvent: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imagesLoaded: {
                                state: 'LOADED...',
                                images: action.payload.images,
                                titleOfImagesList: action.payload.titleOfImagesList
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}

export function reducerImagesAreStreamed(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {

    switch (action.type) {
        case Actions.START_STREAMING: {
            switch (action.payloadType) {
                case LastImagesAreLoadedEvent:
                case LoadPagesOfImagesEvent:
                case PayloadIntervalDatesSelectedEvent: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {

                            imagesAreStreamed: {
                                state: 'START_STREAMING',
                                images: null,
                                titleOfImagesList: action.payload.titleOfImagesList
                            }
                        });
                    return returnedTarget;
                }
            }
        }
        case Actions.IMAGES_ARE_STREAMED: {
            switch (action.payloadType) {
                case PayloadLoadedImagesEvent: {
                    var currentImages: ExchangedImageDTO[] | undefined = state.imagesAreStreamed.images?._embedded?.ExchangedImageDTOList;

                    if (action.payload.images._embedded != null) {
                        currentImages = currentImages != null ? currentImages?.concat(action.payload.images._embedded.ExchangedImageDTOList) :
                            action.payload.images._embedded.ExchangedImageDTOList;
                    }
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imagesAreStreamed: {
                                state: 'STREAMING',
                                images: {
                                    page: state.imagesAreStreamed.images?.page,
                                    _embedded: {
                                        ExchangedImageDTOList: currentImages
                                    },
                                    _links: state.imagesAreStreamed.images?._links
                                },
                                titleOfImagesList: action.payload.titleOfImagesList
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}




export function reducerImagesToDelete(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {

    switch (action.type) {
        case Actions.DELETE_IMAGE: {
            switch (action.payloadType) {
                case ImageToDeleteEvent: {
                    var imageToDelete: Set<ExchangedImageDTO> = state.imagesToDelete.images != null ? state.imagesToDelete.images : new Set();
                    imageToDelete.add(action.payload.img);
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imagesToDelete: {
                                images: imageToDelete
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}

export function reducerImagesToDownload(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {

    switch (action.payloadType) {
        case DownloadSelectedImageEvent: {
            var imageTodownload: Set<ExchangedImageDTO> = state.imagesToDownload.images != null ? state.imagesToDownload.images : new Set();
            imageTodownload.add(action.payload.image);
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imagesToDownload: {
                        images: imageTodownload
                    }
                });
            return returnedTarget;
        }
    }

    if (state != null) {
        return state;
    }

    return initialState;
}



export function reducerImageIsSelectedToBeDisplayed(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {
    switch (action.payloadType) {
        case '1':
        case '7':
        case PayloadLoadedImagesEvent:
        case LoadImagesOfMetadataEvent:
            {
                const returnedTarget = Object.assign(
                    {},
                    state,
                    {
                        imageIsSelectedToBeDisplayed: {
                            isLoading: false,
                            image: null
                        }
                    });
                return returnedTarget;
            }
        case DeleteKeywordEvent :{
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: true,
                        image: action.payload.image
                    }
                });
            return returnedTarget;
        }
        case DeleteAlbumEvent :{
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: true,
                        image: action.payload.image
                    }
                });
            return returnedTarget;
        }
        case DeletePersonEvent :{
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: true,
                        image: action.payload.image
                    }
                });
            return returnedTarget;
        }
        case AddKeywordEvent :{
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: true,
                        image: action.payload.image
                    }
                });
            return returnedTarget;
        }
        case AddAlbumEvent :{
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: true,
                        image: action.payload.image
                    }
                });
            return returnedTarget;
        }
        case AddPersonEvent :{
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    imageIsSelectedToBeDisplayed: {
                        isLoading: true,
                        image: action.payload.image
                    }
                });
            return returnedTarget;
        }
        case SelectedImageEvent: {
            switch (action.type) {
                case Actions.DESELECT_IMAGE_TO_DISPLAY: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imageIsSelectedToBeDisplayed: {
                                isLoading: false,
                                image: null
                            }
                        });
                    return returnedTarget;
                }
                case Actions.SELECTED_IMAGE_TO_DISPLAY_IS_LOADED: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imageIsSelectedToBeDisplayed: {
                                isLoading: false,
                                image: action.payload.img
                            }
                        });
                    return returnedTarget;
                }
                case Actions.LOADING_SELECTED_IMAGE_TO_DISPLAY: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            imageIsSelectedToBeDisplayed: {
                                isLoading: true,
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}


export function reducerDisplayRatings(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {
    switch (action.type) {
        case Actions.RATINGS_ARE_LOADED: {
            switch (action.payloadType) {
                case '13': {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            displayedRatings: {
                                ratings: action.payload.ratings
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}

export function reducerDisplayKeywords(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {
    switch (action.type) {
        case Actions.ALL_KEYWORDS_ARE_LOADED: {
            switch (action.payloadType) {
                case '19': {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            displayKeywords: {
                                keywords: action.payload.keywords
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }
    return initialState;
}


export function reducerDatesOfImages(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {

    switch (action.payloadType) {
        case GetAllDatesOfImagesEvent: {
            switch (action.type) {
                case Actions.DATES_OF_IMAGES: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            datesOfImages: {
                                datesOfImages: null
                            }
                        });
                    return returnedTarget;
                }
                case Actions.DATES_OF_IMAGES_ARE_STREAMED: {
                    var currentDates: MinMaxDatesDto[] | undefined = state.datesOfImages?.datesOfImages;
                    if (currentDates == null) {
                        currentDates = action.payload.data;
                    } else {
                        if (action.payload.data != null) {
                            currentDates = currentDates?.concat(action.payload.data);
                        }
                    }
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            datesOfImages: {
                                datesOfImages: currentDates
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }
    return initialState;
}



export function reducerDisplayPersons(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {
    switch (action.type) {
        case Actions.ALL_PERSONS_ARE_LOADED: {
            switch (action.payloadType) {
                case '26': {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
                            displayPersons: {
                                persons: action.payload.persons
                            }
                        });
                    return returnedTarget;
                }
            }
        }
    }
    if (state != null) {
        return state;
    }

    return initialState;
}


