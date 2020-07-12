import React from 'react';
import Button from '@material-ui/core/Button';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import TextField from '@material-ui/core/TextField';
import Input from '@material-ui/core/Input';


import Typography from '@material-ui/core/Typography';
import IconButton from '@material-ui/core/IconButton';
import MenuIcon from '@material-ui/icons/Menu';
import ImageSearchIcon from '@material-ui/icons/ImageSearch';
import SearchIcon from '@material-ui/icons/Search';
import SkipNextIcon from '@material-ui/icons/SkipNext';
import SkipPreviousIcon from '@material-ui/icons/SkipPrevious';
import DirectionsIcon from '@material-ui/icons/Directions';
import CloudDownloadIcon from '@material-ui/icons/CloudDownload';

import LimitDatesServiceImpl, { LimitDatesService } from '../services/LimitDates';
import { MinMaxDatesDto } from '../model/MinMaxDatesDto'
import LeftPanel, { LeftPanelClass } from './leftpanel'
import { ParagraphTitle } from '../styles';
import { KeyboardDateTimePicker } from "@material-ui/pickers";
import MomentTimeZone, { Moment } from 'moment-timezone';
import { FloatProperty, DisplayInside, TableLayoutProperty } from 'csstype';
import { connect } from "react-redux";
import {
    loadImagesInterval,
    ApplicationThunkDispatch,
    dispatchNewSelectedDateImagesInterval,
    ApplicationEvent,
    dispatchLastImages,
    loadLastImages,
    loadPagesOfImages,
    dispatchNextPage,
    dispatchPrevPage

} from '../redux/Actions';
import { ClientApplicationState } from '../redux/State';
import { MaterialUiPickersDate } from '@material-ui/pickers/typings/date';
import { ImageKeyDto, ImageDto } from '../model/ImageDto';
import DeleteForeverIcon from '@material-ui/icons/DeleteForever';
import { Badge } from '@material-ui/core';
import RightPanel from './RightPanel';
import ComponentStatus from './ComponentStatus';
import RatingsList from '../components/RatingsList'
import { withStyles } from '@material-ui/core/styles';
import FavoriteIcon from '@material-ui/icons/Favorite';
import Paper from '@material-ui/core/Paper';
import Divider from '@material-ui/core/Divider';

export interface HeaderProps {
    urlNext?: string;
    urlPrevious?: string;
    nbOfPhotos?: number;
    photosToDelete?: Set<ImageKeyDto> | null;
    photosToDownload?: Set<ImageDto> | null;
    pageNumber?: number
    min?: number;
    max?: number;
    intervallType?: string;
    loadLastImages?(pageNumber: number, title: string): ApplicationEvent;
    loadPagesOfImages?(url: string, title: string): ApplicationEvent;
    thunkActionToLoadAllImages?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    loadImagesInterval?(min: number, max: number, intervallType: string): ApplicationEvent;
    thunkAction?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToLoadNextPage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToLoadPrevPage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;

}

interface HeaderState {
    pageNumber: number
    headerIsUpdating: boolean
    leftPanelIsOpened: boolean;
    minMaxDates?: MinMaxDatesDto | null;
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
        }
    }
}


export class Header extends React.Component<HeaderProps, HeaderState> {

    protected limitDatesService: LimitDatesService;
    protected refToLeftPanel: React.RefObject<LeftPanelClass>;

    constructor(props: HeaderProps) {
        super(props);
        this.openLeftPanel = this.openLeftPanel.bind(this);
        this.state = {
            pageNumber: 1,
            headerIsUpdating: false,
            leftPanelIsOpened: false, totalImages: 0, firstSelectDate: MomentTimeZone(),
            lastSelectedDate: MomentTimeZone()
        }
        this.limitDatesService = new LimitDatesServiceImpl();
        this.refToLeftPanel = React.createRef();
        this.handleSecundDateChange = this.handleSecundDateChange.bind(this);
        this.handleFirstDateChange = this.handleFirstDateChange.bind(this);
        this.handleUpdate = this.handleUpdate.bind(this);
        this.handlePageNumberUpdate = this.handlePageNumberUpdate.bind(this);
        this.handleRefresh = this.handleRefresh.bind(this);
        this.handleNextPage = this.handleNextPage.bind(this);
        this.handlePreviousPage = this.handlePreviousPage.bind(this);

    }

    handleNextPage() {
        if (this.props.loadPagesOfImages != null && this.props.thunkActionToLoadNextPage != null && this.props.urlNext != null) {
            this.props.thunkActionToLoadNextPage(this.props.loadPagesOfImages(this.props.urlNext,'Page '+(this.state.pageNumber+1)+' de vos photos '))
        }
    }

    handlePreviousPage() {
        if (this.props.loadPagesOfImages != null && this.props.thunkActionToLoadNextPage != null && this.props.urlPrevious != null) {
            this.props.thunkActionToLoadNextPage(this.props.loadPagesOfImages(this.props.urlPrevious,'Page '+(this.state.pageNumber-1)+' de vos photos '))
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

    handleFirstDateChange(date: MaterialUiPickersDate) {
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

    handleSecundDateChange(date: MaterialUiPickersDate) {
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

        this.setState({
            pageNumber: Number(pageNumber),
            headerIsUpdating: this.state.headerIsUpdating,
            leftPanelIsOpened: this.state.leftPanelIsOpened,
            minMaxDates: this.state.minMaxDates,
            totalImages: this.state.totalImages,
            firstSelectDate: this.state.firstSelectDate,
            lastSelectedDate: this.state.lastSelectedDate
        });
    }

    handleRefresh() {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(this.state.pageNumber,'Page '+this.state.pageNumber+' de vos photos ' ))
        }
    }

    handleNextPageNumberUpdate(pageNumber: number) {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(pageNumber,'Page '+this.state.pageNumber+' de vos photos '))
        }
    }

    handlePreviousPageNumberUpdate(pageNumber: number) {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(pageNumber,'Page '+this.state.pageNumber+' de vos photos '))
        }
    }




    openLeftPanel() {
        if (this.refToLeftPanel.current != null) {
            this.refToLeftPanel.current.handleOnOpen();
        }
    }
    componentDidMount() {
        if (this.props.loadLastImages != null && this.props.thunkActionToLoadAllImages != null) {
            this.props.thunkActionToLoadAllImages(this.props.loadLastImages(1,'Dernières images'))
        }
        this.limitDatesService.getLimits((lim?: MinMaxDatesDto) => {
            this.setState({
                headerIsUpdating: false,
                leftPanelIsOpened: this.state.leftPanelIsOpened,
                minMaxDates: lim,
                totalImages: lim == null ? 0 : lim.nbOfImages,
                firstSelectDate: MomentTimeZone(),
                lastSelectedDate: MomentTimeZone(),
            });
        })
    }

    render() {
        const nbOfImagesToDelete = this.props.photosToDelete != null ? this.props.photosToDelete.size : 0;
        const nbOfImagesToDownload = this.props.photosToDownload != null ? this.props.photosToDownload.size : 0;
        const currentMinMax = this.state.minMaxDates;
        const totalImages = this.state.totalImages;
        const pageNumber = this.state.pageNumber;
        const right: FloatProperty = "right";
        const tableFixed: TableLayoutProperty = "fixed";
        const dispalyInside: DisplayInside = "table";
        const selectedDateFirst = this.state.headerIsUpdating ? this.state.firstSelectDate : (this.props.min != 0 ? MomentTimeZone(this.props.min) : MomentTimeZone());
        const selectedDateLast = this.state.headerIsUpdating ? this.state.lastSelectedDate : (this.props.max != 0 ? MomentTimeZone(this.props.max) : MomentTimeZone());

        const divStyle = {
            float: right,
            border: 'solid 1px rgba(255, 255, 255, 0.23)',
            borderRadius: '7px',
            width: '-webkit-fill-available',
            display: 'table',
            tableLayout: tableFixed
        };

        const rightStyle = {
            float: right,
            marginTop: '15px',
            marginBottomw: '15px',
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
                        <IconButton edge="start" color="inherit" aria-label="menu">
                            <MenuIcon onClick={this.openLeftPanel} />
                        </IconButton>
                        <Typography variant="h6" >
                            Photos Workflow&nbsp;
			            </Typography>
                        <div style={tableStyle} >
                            <div style={{ display: 'table-row' }} >
                                <div style={{ float: 'left', margin: '5px' }} >
                                    <ParagraphTitle text={'Date premiere image'} size='10px' />
                                    <ParagraphTitle text={(currentMinMax != null ? currentMinMax.minDate.locale('fr').format("ddd DD MMMM YYYY HH:mm:ss") : 'unset')} />
                                </div>
                                <div style={{ float: 'left', margin: '5px' }} >
                                    <ParagraphTitle text={'Date derniere image '} size='10px' />
                                    <ParagraphTitle text={(currentMinMax != null ? currentMinMax.maxDate.locale('fr').format("ddd DD MMMM  YYYY HH:mm:ss") : 'unset')} />
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
                                <Button  >
                                    <CloudDownloadIcon />
                                </Button>
                            </Badge>
                            <Badge badgeContent={nbOfImagesToDelete} color="primary" style={{ display: 'table-cell', margin: '5px', padding: '5px' }} >
                                <Button  >
                                    <DeleteForeverIcon />
                                </Button>
                            </Badge>
                            <Badge badgeContent={nbOfImagesToDelete} color="primary" style={{ display: 'table-cell', margin: '5px', padding: '5px' }} >
                                <Button  >
                                    <FavoriteIcon color="secondary" />
                                </Button>
                            </Badge>
                        </div>
                        <div style={divStyle}>
                            <div style={{ margin: '7px', backgroundColor: '#424242', display: 'table-cell', width: '-webkit-fill-available' }}>
                                <div style={{ float: 'right' }} >
                                    <IconButton edge="start" color="inherit" style={{ float: 'right' }} >
                                        <ImageSearchIcon onClick={this.handleUpdate} />
                                    </IconButton>
                                    <KeyboardDateTimePicker
                                        ampm={false}
                                        style={rightStyle}
                                        label="Date courante finale"
                                        value={selectedDateLast}
                                        onChange={this.handleSecundDateChange}
                                    />
                                    <KeyboardDateTimePicker
                                        ampm={false}
                                        style={rightStyle}
                                        label="Date courante initiale"
                                        value={selectedDateFirst}
                                        onChange={this.handleFirstDateChange}
                                    />
                                </div>
                                <div style={rightStyle} >
                                    <IconButton edge="start" color="inherit" style={{ float: 'right' }} >
                                        <SkipNextIcon onClick={this.handleNextPage} />
                                    </IconButton>
                                    <IconButton edge="start" color="inherit" style={{ float: 'right' }} >
                                        <ImageSearchIcon onClick={this.handleRefresh} />
                                    </IconButton>
                                    <IconButton edge="start" color="inherit" style={{ float: 'right' }} >
                                        <SkipPreviousIcon onClick={this.handlePreviousPage} />
                                    </IconButton>
                                    <TextField label="Page courante" value={pageNumber} onChange={(e) => { this.handlePageNumberUpdate(e.target.value); }} />
                                </div>
                            </div>
                            <div style={{
                                margin: '7px', backgroundColor: '#424242', display: 'table-cell', width: '-webkit-fill-available', verticalAlign: 'middle', paddingLeft: '20px'

                            }}>
                                <TextField
                                    style={{
                                        width: '-webkit-fill-available'
                                    }}

                                    placeholder="Mots-clé"
                                />
                                <IconButton type="submit" aria-label="search" style={{
                                    position: 'absolute',
                                    right: '20px',
                                    top: '25px'
                                }}>
                                    <SearchIcon />
                                </IconButton>
                            </div>
                        </div>
                    </Toolbar>
                </AppBar>
                <LeftPanel ref={this.refToLeftPanel} />
            </React.Fragment >
        );

    }
}

const mapStateToProps = (state: ClientApplicationState, ownProps: HeaderProps): HeaderProps => {

    const nbOfPhotosToDelete = state.reducerImagesToDelete.imagesToDelete.images != null ? state.reducerImagesToDelete.imagesToDelete.images.size : 0
    const nbOfPhotosToDownload = state.reducerImagesToDownload.imagesToDownload.images != null ? state.reducerImagesToDownload.imagesToDownload.images.size : 0

    switch (state.reducerImagesList.lastIntervallRequested.state) {
        case 'LOADING': {
            return {
                min: state.reducerImagesList.lastIntervallRequested.min,
                max: state.reducerImagesList.lastIntervallRequested.max,
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
        thunkActionToLoadAllImages: (x: ApplicationEvent) => {
            const r = dispatchLastImages(x);
            return dispatch(r);
        },
        thunkAction: (x: ApplicationEvent) => {
            const r = dispatchNewSelectedDateImagesInterval(x);
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


