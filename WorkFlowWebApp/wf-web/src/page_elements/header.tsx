import React from 'react';
import Button from '@mui/material/Button';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import TextField from '@mui/material/TextField';
import Input from '@mui/material/Input';


import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import MenuIcon from '@mui/icons-material/Menu';
import ImageSearchIcon from '@mui/icons-material/ImageSearch';
import SearchIcon from '@mui/icons-material/Search';
import SkipNextIcon from '@mui/icons-material/SkipNext';
import SkipPreviousIcon from '@mui/icons-material/SkipPrevious';
import DirectionsIcon from '@mui/icons-material/Directions';
import CloudDownloadIcon from '@mui/icons-material/CloudDownload';

import LimitDatesServiceImpl, { LimitDatesService } from '../services/LimitDates';
import { MinMaxDatesDto, ImageKeyDto, ExchangedImageDTO } from '../model/DataModel'
import { ParagraphTitle } from '../styles';
import MomentTimeZone, { Moment } from 'moment-timezone';
import { FloatProperty, DisplayInside, TableLayoutProperty } from 'csstype';
import { connect } from "react-redux";
import {
    loadImagesInterval,
    ApplicationThunkDispatch,
    dispatchCheckoutImage,
    dispatchDeleteImage,
    ApplicationEvent,
    dispatchLastImages,
    dispatchGetAllDatesOfImages,
    loadLastImages,
    loadPagesOfImages,
    dispatchNextPage,
    dispatchPrevPage,
    downloadImage,
    deleteImage,
    getAllDatesOfImages

} from '../redux/Actions';
import { ClientApplicationState } from '../redux/State';
import DeleteForeverIcon from '@mui/icons-material/DeleteForever';
import { Badge } from '@mui/material';
import RightPanel from './RightPanel';
import ComponentStatus from './ComponentStatus';
import RatingsList from '../components/RatingsList'
import withStyles from '@mui/styles/withStyles';
import FavoriteIcon from '@mui/icons-material/Favorite';
import Paper from '@mui/material/Paper';
import Divider from '@mui/material/Divider';

export interface HeaderProps {
    urlNext?: string;
    urlPrevious?: string;
    nbOfPhotos?: number;
    photosToDelete?: Set<ExchangedImageDTO> | null;
    photosToDownload?: Set<ExchangedImageDTO> | null;
    pageNumber?: number ;
    totalStreamedImages?: number ;
    min?: Moment;
    max?: Moment;
    intervallType?: string;
    loadLastImages?(pageNumber: number, title: string): ApplicationEvent;
    loadPagesOfImages?(url: string, title: string): ApplicationEvent;
    loadDatesOfImages?(): ApplicationEvent;
    downloadImage?(img: ExchangedImageDTO): ApplicationEvent;
    deleteImage?(img: ExchangedImageDTO): ApplicationEvent;
    thunkActionToLoadAllImages?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToGetDatesOfImages?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToCheckoutImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToDeleteImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    loadImagesInterval?(min: number, max: number, intervallType: string): ApplicationEvent;
    thunkAction?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToLoadNextPage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToLoadPrevPage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;


}

interface HeaderState {
    pageNumber: number
    headerIsUpdating: boolean
    leftPanelIsOpened: boolean;
    streamedMinDate?: Moment ;
    streamedMaxDate?: Moment ;
    minMaxDates?: MinMaxDatesDto;
    totalStreamedImages?: number;
    totalImages: number;
    firstSelectDate: Moment;
    lastSelectedDate: Moment;
};

export const styles = {
    '@global': {
        '.MuiInputBase-input': {
            fontSize: '10px'
        },
        '.MuiInputLabel-root': {
            fontSize: '14px'
        },
        '.MuiBadge-badge': {
            fontSize: '10px'
        },
        '.MuiInputBase-root': {
            alignItems: 'unset'
        },
        '.MuiIconButton-root': {
            padding: '12px'
        }
    }
}


export class Header extends React.Component<HeaderProps, HeaderState> {

    protected limitDatesService: LimitDatesService;
    protected pageNumber: number;

    constructor(props: HeaderProps) {
        super(props);
        this.state = {
            pageNumber: 1,
            headerIsUpdating: false,
            leftPanelIsOpened: false, totalImages: 0, firstSelectDate: MomentTimeZone(),
            lastSelectedDate: MomentTimeZone()
        }
        this.limitDatesService = new LimitDatesServiceImpl();
        this.handleSecundDateChange = this.handleSecundDateChange.bind(this);
        this.handleFirstDateChange = this.handleFirstDateChange.bind(this);
        this.handleUpdate = this.handleUpdate.bind(this);
        this.handlePageNumberUpdate = this.handlePageNumberUpdate.bind(this);
        this.handleRefresh = this.handleRefresh.bind(this);
        this.handleNextPage = this.handleNextPage.bind(this);
        this.handlePreviousPage = this.handlePreviousPage.bind(this);
        this.handleClickCheckout = this.handleClickCheckout.bind(this);
        this.handleClickDelete = this.handleClickDelete.bind(this);
        this.pageNumber = 1;
    }
    static getDerivedStateFromProps(props: HeaderProps, state: HeaderState): HeaderState {
        if (props != null) {
            if (state.headerIsUpdating) {

                return {
                    pageNumber: state.pageNumber,
                    headerIsUpdating: false,
                    leftPanelIsOpened: state.leftPanelIsOpened,
                    minMaxDates: state.minMaxDates,
                    streamedMinDate: props.min,
                    streamedMaxDate: props.max,
                    totalImages: state.totalImages,
                    totalStreamedImages : props.totalStreamedImages,
                    firstSelectDate: state.firstSelectDate,
                    lastSelectedDate: state.lastSelectedDate,
                }
            } else {
                return {
                    pageNumber: props.pageNumber != null ? props.pageNumber : state.pageNumber,
                    headerIsUpdating: false,
                    leftPanelIsOpened: state.leftPanelIsOpened,
                    minMaxDates: state.minMaxDates,
                    streamedMinDate: props.min,
                    streamedMaxDate: props.max,
                    totalImages: state.totalImages,
                    totalStreamedImages : props.totalStreamedImages,
                    firstSelectDate: state.firstSelectDate,
                    lastSelectedDate: state.lastSelectedDate,
                }
            }
        } else {
            return state;
        }
    }


    handleNextPage() {
        if (this.props.loadPagesOfImages != null && this.props.thunkActionToLoadNextPage != null && this.props.urlNext != null) {
            this.props.thunkActionToLoadNextPage(this.props.loadPagesOfImages(this.props.urlNext, 'Page ' + (this.state.pageNumber + 1) + ' de vos photos '))
        }
    }

    handlePreviousPage() {
        if (this.props.loadPagesOfImages != null && this.props.thunkActionToLoadNextPage != null && this.props.urlPrevious != null) {
            this.props.thunkActionToLoadNextPage(this.props.loadPagesOfImages(this.props.urlPrevious, 'Page ' + (this.state.pageNumber - 1) + ' de vos photos '))
        }
    }

    handleUpdate() {
        if (this.props.loadImagesInterval != null && this.props.thunkAction != null) {
            this.props.thunkAction(this.props.loadImagesInterval(this.state.firstSelectDate.valueOf(), this.state.lastSelectedDate.valueOf(), 'minute'));
            this.setState({
                pageNumber: this.state.pageNumber,
                headerIsUpdating: false,
                leftPanelIsOpened: this.state.leftPanelIsOpened,
                minMaxDates: this.state.minMaxDates,
                totalImages: this.state.totalImages,
                firstSelectDate: this.state.firstSelectDate,
                lastSelectedDate: this.state.lastSelectedDate,
            });
        }
    }

    handleFirstDateChange(date: any) {
        const moment: Moment = (date != null ? date : MomentTimeZone());
        this.setState({
            pageNumber: this.state.pageNumber,
            headerIsUpdating: true,
            leftPanelIsOpened: this.state.leftPanelIsOpened,
            minMaxDates: this.state.minMaxDates,
            totalImages: this.state.totalImages,
            firstSelectDate: moment,
            lastSelectedDate: this.state.lastSelectedDate,
        });

    }

    handleSecundDateChange(date: any) {
        const moment: Moment = (date != null ? date : MomentTimeZone());
        this.setState({
            pageNumber: this.state.pageNumber,
            headerIsUpdating: true,
            leftPanelIsOpened: this.state.leftPanelIsOpened,
            minMaxDates: this.state.minMaxDates,
            totalImages: this.state.totalImages,
            firstSelectDate: this.state.firstSelectDate,
            lastSelectedDate: moment
        });

    }

    handlePageNumberUpdate(pageNumber: string) {
        if (pageNumber.length > 0) {
            this.setState({
                pageNumber: Number(pageNumber),
                headerIsUpdating: true,
                leftPanelIsOpened: this.state.leftPanelIsOpened,
                minMaxDates: this.state.minMaxDates,
                totalImages: this.state.totalImages,
                firstSelectDate: this.state.firstSelectDate,
                lastSelectedDate: this.state.lastSelectedDate
            });
        }
    }

    handleRefresh() {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(this.state.pageNumber, 'Page ' + this.state.pageNumber + ' de vos photos '))
        }
    }

    handleNextPageNumberUpdate(pageNumber: number) {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(pageNumber, 'Page ' + this.state.pageNumber + ' de vos photos '))
        }
    }

    handlePreviousPageNumberUpdate(pageNumber: number) {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(pageNumber, 'Page ' + this.state.pageNumber + ' de vos photos '))
        }
    }


    handleClickCheckout() {
        if (this.props.thunkActionToCheckoutImage != null) {
            this.props.photosToDownload?.forEach(
                (img) => {
                    if (this.props.thunkActionToCheckoutImage != null && this.props.downloadImage != null) {
                        this.props.thunkActionToCheckoutImage(
                            this.props.downloadImage(img))
                    }
                });
        }
    }

    handleClickDelete() {
        if (this.props.thunkActionToDeleteImage != null) {
            this.props.photosToDelete?.forEach(
                (img) => {
                    if (this.props.thunkActionToDeleteImage != null && this.props.deleteImage != null) {
                        this.props.thunkActionToDeleteImage(
                            this.props.deleteImage(img))
                    }
                });
        }
    }


    openLeftPanel() {
    }
    componentDidMount() {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(1, 'Dernières images'))
        }
        if (this.props.thunkActionToGetDatesOfImages != null && this.props.loadDatesOfImages != null) {
            this.props.thunkActionToGetDatesOfImages(this.props.loadDatesOfImages());
        }
        this.limitDatesService.getLimits((lim?: MinMaxDatesDto) => {
            this.setState({
                headerIsUpdating: false,
                leftPanelIsOpened: this.state.leftPanelIsOpened,
                minMaxDates: lim,
                totalImages: lim == null ? 0 : lim.countNumber,
                firstSelectDate: MomentTimeZone(),
                lastSelectedDate: MomentTimeZone(),
            });
        })
    }

    render() {
        const nbOfImagesToDelete = this.props.photosToDelete != null ? this.props.photosToDelete.size : 0;
        const nbOfImagesToDownload = this.props.photosToDownload != null ? this.props.photosToDownload.size : 0;
        const currentMinMax = this.state.minMaxDates;
        const streamedMinDate = this.state.streamedMinDate ;
        const streamedMaxDate = this.state.streamedMaxDate ;
        const totalImages = this.state.totalStreamedImages != null ? this.state.totalStreamedImages : this.state.totalImages;
        const pageNumber = this.state.pageNumber;
        const right: FloatProperty = "right";
        const left: FloatProperty = "left";

        const tableFixed: TableLayoutProperty = "fixed";
        const dispalyInside: DisplayInside = "table";
        const selectedDateFirst = this.state.headerIsUpdating ? this.state.firstSelectDate : (this.props.min != null  ? this.props.min : MomentTimeZone());
        const selectedDateLast = this.state.headerIsUpdating ? this.state.lastSelectedDate : (this.props.max != null  ? this.props.max : MomentTimeZone());

        const divStyle = {
            float: right,
            border: 'solid 1px rgba(255, 255, 255, 0.23)',
            borderRadius: '7px',
            width: '-webkit-fill-available',
        };

        const searchElementStyle = {
            display: 'flex',
            width: '-webkit-fill-available',
            backgroundColor: '#424242',
            marginRight: '1em'
        };

        const tableStyle = {
            display: 'table',
            minWidth: 'fit-content'
        }

        const table2Style = {
            display: 'table',
            border: 'solid 1px rgba(255, 255, 255, 0.23)',
            borderRadius: '7px',
            margin: '5px',

        }


        return (
            <React.Fragment>
                <AppBar position="static">
                    <Toolbar>
                        <IconButton edge="start" color="inherit" aria-label="menu" size="large">
                            <MenuIcon onClick={this.openLeftPanel} />
                        </IconButton>
                        <Typography variant="h6" >
                            Photos Workflow&nbsp;
			            </Typography>
                        <div style={tableStyle} >
                            <div style={{ display: 'table-row' }} >
                                <div style={{ float: 'left', margin: '5px' }} >
                                    <ParagraphTitle text={'Date premiere image'} size='10px' />
                                    <ParagraphTitle text={( streamedMinDate?.locale('fr').format("ddd DD MMMM YYYY HH:mm:ss") ?? (currentMinMax?.minDate?.locale('fr').format("ddd DD MMMM YYYY HH:mm:ss") ?? 'unset'))} />
                                </div>
                                <div style={{ float: 'left', margin: '5px' }} >
                                    <ParagraphTitle text={'Date derniere image '} size='10px' />
                                    <ParagraphTitle text={( streamedMaxDate?.locale('fr').format("ddd DD MMMM YYYY HH:mm:ss") ?? ( currentMinMax?.maxDate?.locale('fr').format("ddd DD MMMM  YYYY HH:mm:ss") ?? 'unset'))} />
                                </div>
                                <div style={{ float: 'left', margin: '5px' }} >
                                    <ParagraphTitle text={"Nombre total d'images"} size='10px' />
                                    <ParagraphTitle text={'' + totalImages} />
                                </div>
                            </div>
                            <div style={{ margin: '5px', display: 'table-row' }}>
                                <RatingsList />
                            </div>

                        </div>
                        <div style={table2Style} >
                            <ComponentStatus />
                            <Badge badgeContent={nbOfImagesToDownload} color="primary" style={{ display: 'table-cell', margin: '5px', padding: '5px' }} >
                                <Button onClick={this.handleClickCheckout} >

                                    <CloudDownloadIcon />
                                </Button>
                            </Badge>
                            <Badge badgeContent={nbOfImagesToDelete} color="primary" style={{ display: 'table-cell', margin: '5px', padding: '5px' }} >
                                <Button  onClick={this.handleClickDelete}  >
                                    <DeleteForeverIcon />
                                </Button>
                            </Badge>
                            <Badge badgeContent={nbOfImagesToDelete} color="primary" style={{ display: 'table-cell', margin: '5px', padding: '5px' }} >
                                <Button  >
                                    <FavoriteIcon color="secondary" />
                                </Button>
                            </Badge>
                        </div>
                        <form style={divStyle}>

                            <div style={{ backgroundColor: '#424242', display: 'flex' }}>
                                <div style={searchElementStyle} >
                                    <IconButton color="inherit" size="large">
                                        <SkipPreviousIcon onClick={this.handlePreviousPage} />
                                    </IconButton>
                                    <TextField style={{ width: '60%' }} label="Page" type="number" value={pageNumber} onChange={(e) => { this.handlePageNumberUpdate(e.target.value); }} />
                                    <IconButton color="inherit" size="large">
                                        <ImageSearchIcon onClick={this.handleRefresh} />
                                    </IconButton>
                                    <IconButton color="inherit" size="large">
                                        <SkipNextIcon onClick={this.handleNextPage} />
                                    </IconButton>
                                </div>
                                <div style={searchElementStyle} >
                                    <TextField
                                        placeholder="Mots-clé" style={{ width: '80%' }}
                                    />
                                    <IconButton type="submit" aria-label="search" size="large">
                                        <SearchIcon />
                                    </IconButton>
                                </div>
                            </div>
                        </form>


                    </Toolbar>
                </AppBar>
            </React.Fragment >
        );

    }
}

const mapStateToProps = (state: ClientApplicationState, ownProps: HeaderProps): HeaderProps => {

    const nbOfPhotosToDelete = state.reducerImagesToDelete.imagesToDelete.images != null ? state.reducerImagesToDelete.imagesToDelete.images.size : 0
    const nbOfPhotosToDownload = state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images.size : 0


    switch (state.reducerImagesAreStreamed.imagesAreStreamed.state) {
        case 'START_STREAMING': {
            return {
                pageNumber: state.reducerImagesList.imagesLoaded.pageNumber,
                nbOfPhotos: nbOfPhotosToDelete + nbOfPhotosToDownload,
                photosToDelete: state.reducerImagesToDelete.imagesToDelete.images != null ? state.reducerImagesToDelete.imagesToDelete.images : null,
                photosToDownload: state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images : null,
            };
        }
        case 'STREAMING': {
            const urlNext = state.reducerImagesAreStreamed.imagesAreStreamed.images?._embedded?.ExchangedImageDTOList[0]?._links?.next?.href;
            const urlPrevious = state.reducerImagesAreStreamed.imagesAreStreamed.images?._embedded?.ExchangedImageDTOList[0]?._links?.prev?.href;
            const pageNumber = state.reducerImagesAreStreamed.imagesAreStreamed.images?._embedded?.ExchangedImageDTOList[0]?.currentPage;
            const minDate = state.reducerImagesAreStreamed.imagesAreStreamed.images?._embedded?.ExchangedImageDTOList[0]?.minDate ;
            const maxDate = state.reducerImagesAreStreamed.imagesAreStreamed.images?._embedded?.ExchangedImageDTOList[0]?.maxDate ;
            const totalStreamedImages = state.reducerImagesAreStreamed.imagesAreStreamed.images?._embedded?.ExchangedImageDTOList[0]?.totalNbOfElements ;
            return {
                pageNumber: pageNumber,
                urlNext: urlNext,
                urlPrevious: urlPrevious,
                min: minDate,
                max: maxDate,
                totalStreamedImages: totalStreamedImages,
                nbOfPhotos: nbOfPhotosToDelete + nbOfPhotosToDownload,
                photosToDelete: state.reducerImagesToDelete.imagesToDelete.images != null ? state.reducerImagesToDelete.imagesToDelete.images : null,
                photosToDownload: state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images : null,
            };
        }
    }

    switch (state.reducerImagesList.lastIntervallRequested.state) {
        case 'LOADING': {
            return {
                intervallType: state.reducerImagesList.lastIntervallRequested.intervallType,
                nbOfPhotos: nbOfPhotosToDelete + nbOfPhotosToDownload,
                photosToDelete: state.reducerImagesToDelete.imagesToDelete.images != null ? state.reducerImagesToDelete.imagesToDelete.images : null,
                photosToDownload: state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images : null,
            };
        }
    }

    switch (state.reducerImagesList.imagesLoaded.state) {

        case 'LOADING': {
            return {
                pageNumber: state.reducerImagesList.imagesLoaded.pageNumber,
                nbOfPhotos: nbOfPhotosToDelete + nbOfPhotosToDownload,
                photosToDownload: state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images : null,
            };
        }
        case 'LOADED': {
            return {
                pageNumber: state.reducerImagesList.imagesLoaded.pageNumber,
                urlNext: state.reducerImagesList.imagesLoaded.urlNext,
                urlPrevious: state.reducerImagesList.imagesLoaded.urlPrev,
                nbOfPhotos: nbOfPhotosToDelete + nbOfPhotosToDownload,
                photosToDelete: state.reducerImagesToDelete.imagesToDelete.images != null ? state.reducerImagesToDelete.imagesToDelete.images : null,
                photosToDownload: state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images : null,
            };
        }

    }
    return {
        ...ownProps,
        nbOfPhotos: nbOfPhotosToDelete + nbOfPhotosToDownload,
        photosToDelete: state.reducerImagesToDelete.imagesToDelete.images != null ? state.reducerImagesToDelete.imagesToDelete.images : null,
        photosToDownload: state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images : null,
    };
};


const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        loadImagesInterval: loadImagesInterval,
        loadLastImages: loadLastImages,
        loadPagesOfImages: loadPagesOfImages,
        downloadImage: downloadImage,
        deleteImage: deleteImage,
        loadDatesOfImages: getAllDatesOfImages,
        thunkActionToLoadAllImages: (x: ApplicationEvent) => {
            const r = dispatchLastImages(x);
            return dispatch(r);
        },
        thunkActionToGetDatesOfImages: (x: ApplicationEvent) => {
            const r = dispatchGetAllDatesOfImages(x);
            return dispatch(r);
        },
        thunkActionToCheckoutImage: (x: ApplicationEvent) => {
            const r = dispatchCheckoutImage(x);
            return dispatch(r);
        },
        thunkActionToDeleteImage: (x: ApplicationEvent) => {
            const r = dispatchDeleteImage(x);
            return dispatch(r);
        },
        thunkActionToLoadNextPage: (x: ApplicationEvent) => {
            const r = dispatchNextPage(x);
            return dispatch(r);
        },
        thunkActionToLoadPrevPage: (x: ApplicationEvent) => {
            const r = dispatchPrevPage(x);
            return dispatch(r);
        }

    }
};


export default connect(mapStateToProps, mapDispatchToProps, null, { forwardRef: true })(withStyles(styles)(Header));


