import { combineReducers } from 'redux'

import {
    reducerImagesList,
    reducerImagesToDelete,
    reducerImagesToDownload,
    reducerDisplayedExif,
    reducerDisplayRatings,
    reducerImageIsSelectedToBeDisplayed,
    reducerDisplayKeywords
} from "./LoadImagesProcess";

export default combineReducers({
    reducerImagesList,
    reducerImagesToDelete,
    reducerImagesToDownload,
    reducerDisplayedExif,
    reducerDisplayRatings,
    reducerImageIsSelectedToBeDisplayed,
    reducerDisplayKeywords
});
