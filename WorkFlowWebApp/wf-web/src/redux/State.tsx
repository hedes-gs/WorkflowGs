import { ExchangedImageDTO, PageOfExchangedImageDTO, MinMaxDatesDto, ExifOfImages, Metadata } from "../model/DataModel";

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
        images: PageOfExchangedImageDTO | null
    },
    imagesAreStreamed: {
        state: string,
        urlNext: string,
        urlPrev: string,
        pageNumber: number,
        titleOfImagesList: string,
        images: PageOfExchangedImageDTO | null
    },
    imagesToDelete: {
        images?: Set<ExchangedImageDTO> | null;
    }
    imagesToDownload: {
        images?: Set<ExchangedImageDTO> | null;
    }
    displayedExif: {
        imageOwner?: ExchangedImageDTO | null,
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
        image?: ExchangedImageDTO | null
    }
    realTimeSelected: {
        isLoading: boolean,
    }
    datesOfImages: {
        datesOfImages?: MinMaxDatesDto[]
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
    reducerImagesAreStreamed: ApplicationState,
    reducerDatesOfImages: ApplicationState,
}

export default ApplicationState;
