import { PayloadLoadedImagesEvent, ApplicationEvent, DownloadSelectedImageEvent, LoadImagesOfMetadataEvent, LoadAllPersonsEvent } from './Actions'
import ApplicationSate from './State'

import Actions from "./ActionsType";
import { ImageDto, ImageKeyDto } from '../model/ImageDto';

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
    }
};

export function reducerDisplayedExif(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {
    switch (action.type) {
        case Actions.SELECTED_IMAGE_TO_DISPLAY_IS_LOADED: {
            const imageDto = action.payloadType == '16' ? action.payload.image : null;
            const currentExifs = state.displayedExif.exifs;
            const exifs = currentExifs != null &&
                imageDto != null &&
                currentExifs._embedded != null &&
                currentExifs._embedded.exifDToes[0].imageOwner != null &&
                imageDto.data != null &&
                currentExifs._embedded.exifDToes[0].imageOwner.imageId == imageDto.data.imageId ? currentExifs : null;
            const returnedTarget = Object.assign(
                {},
                state,
                {
                    displayedExif: {
                        imageOwner: imageDto,
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
        case LoadImagesOfMetadataEvent: {
            const returnedTarget = Object.assign(
                {},
                state,
                {
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
        case '2': {
            switch (action.type) {
                case Actions.IMAGES_ARE_LOADED: {
                    const returnedTarget = Object.assign(
                        {},
                        state,
                        {
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
        case Actions.IMAGES_ARE_LOADED: {
            switch (action.payloadType) {
                case '2': {
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

export function reducerImagesToDelete(state: ApplicationSate, action: ApplicationEvent): ApplicationSate {

    switch (action.type) {
        case Actions.DELETE_IMAGE: {
            switch (action.payloadType) {
                case '3': {
                    var imageToDelete: Set<ImageKeyDto> = state.imagesToDelete.images != null ? state.imagesToDelete.images : new Set();
                    imageToDelete.add(action.payload.image);
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
            var imageTodownload: Set<ImageDto> = state.imagesToDownload.images != null ? state.imagesToDownload.images : new Set();
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
        case '16': {
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
                                image: action.payload.image
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


