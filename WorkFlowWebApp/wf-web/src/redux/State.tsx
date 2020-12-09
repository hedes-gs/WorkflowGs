import { ImageDto, PageOfImageDto, ImageKeyDto, ExifOfImages, Metadata } from "../model/ImageDto";

export interface ApplicationState {
    lastIntervallRequested: {
        min: number,
        max: number,
        state: string,
        intervallType: string
    },
    imagesLoaded: {
        state: string,
        urlNext: string,
        urlPrev: string,
        pageNumber: number,
        titleOfImagesList: string,
        images: PageOfImageDto | null
    }
    imagesToDelete: {
        images?: Set<ImageKeyDto> | null;
    }
    imagesToDownload: {
        images?: Set<ImageDto> | null;
    }
    displayedExif: {
        imageOwner?: ImageDto | null,
        exifs?: ExifOfImages | null;
    }
    displayedRatings: {
        ratings: Map<string, number>
    }
    displayKeywords: {
        state: string;
        keywords: Metadata[]
    }
    displayPersons: {
        state: string;
        persons: Metadata[]
    }
    imageIsSelectedToBeDisplayed: {
        isLoading: boolean,
        image?: ImageDto | null
    }
};

export interface ClientApplicationState {
    reducerMetadata: ApplicationState,
    reducerImagesList: ApplicationState,
    reducerImagesToDelete: ApplicationState,
    reducerImagesToDownload: ApplicationState,
    reducerDisplayedExif: ApplicationState,
    reducerDisplayRatings: ApplicationState,
    reducerDisplayKeywords: ApplicationState,
    reducerDisplayPersons: ApplicationState,
    reducerImageIsSelectedToBeDisplayed: ApplicationState,
}

export default ApplicationState;
