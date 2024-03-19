import { combineReducers } from 'redux'

import {
    reducerImagesList,
    reducerImagesToDelete,
    reducerImagesToDownload,
    reducerDisplayedExif,
    reducerDisplayRatings,
    reducerImageIsSelectedToBeDisplayed,
    reducerDisplayKeywords,
    reducerDisplayPersons,
    reducerMetadata,
    reducerImagesAreStreamed,
    reducerDatesOfImages
} from "./ReduxProcesses";

export default combineReducers({
    reducerImagesList,
    reducerImagesToDelete,
    reducerImagesToDownload,
    reducerDisplayedExif,
    reducerDisplayRatings,
    reducerImageIsSelectedToBeDisplayed,
    reducerDisplayKeywords,
    reducerDisplayPersons,
    reducerMetadata,
    reducerImagesAreStreamed,
    reducerDatesOfImages
});
