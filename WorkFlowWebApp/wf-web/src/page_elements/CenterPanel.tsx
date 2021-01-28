import 'moment/locale/fr'
import Grid, { GridProps } from '@material-ui/core/Grid';

import { connect } from "react-redux";
import React from 'react';
import { ClientApplicationState } from '../redux/State';
import { ImageDto, ImageLinks, PageOfImageDto, ImageKeyDto } from '../model/ImageDto';
import { GridList, GridListTile, GridListTileBar, IconButton } from '@material-ui/core';
import InfoIcon from '@material-ui/icons/Info';
import TrashIcon from '@material-ui/icons/Delete';
import CloudDownloadIcon from '@material-ui/icons/CloudDownload';
import { withStyles } from '@material-ui/core/styles';
import {
    ApplicationThunkDispatch,
    dispatchDownloadSelectedImageEvent,
    ApplicationEvent,
    dispatchPhotoToDelete,
    deleteImage,
    selectedImageIsLoading,
    dispatchImageToSelect,
    downloadSelectedImage
} from '../redux/Actions';
import CircularProgress from '@material-ui/core/CircularProgress';
import RightPanel from './RightPanel';
import ListSubheader from '@material-ui/core/ListSubheader';
import RealtimeImportImages from './RealTimeImportImages';

let idGlobal: number = 0;

export interface CenterPanelProps {
    thunkActionForDeleteImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForSelectImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDownloadImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    deleteImage?(img: ImageDto): ApplicationEvent;
    selectImage?(img: ImageDto | undefined): ApplicationEvent;
    downloadImage?(img: ImageDto): ApplicationEvent;
    titleOfImagesList?: string;
    imgs?: PageOfImageDto | null
    id: number,
    min?: number,
    max?: number,
    status?: string,
    displaySelectedImage: boolean,
    selectedImageIsPresent?: boolean
    displayImportImages?: boolean
}
export interface CenterPanelState {
    min: number,
    max: number,
    width: number,
    height: number
};


export const styles = {
    '@global': {
        '.MuiGridListTile-root': {
            backgroundColor: '#424242'
        },
        '.MuiGridListTile-imgFullWidth': {
            top: '50%',
            position: 'relative',
            transform: 'translateY(-50%)',
            width: 'unset',
        },
        '.MuiGridListTile-tile': {
            backgroundColor: 'rgba(255, 255, 255, 0.08)'
        },
        '.MuiGridListTileBar-titleWrap': {
            flexGrow: 1,
            marginLeft: 8,
            marginRight: 8,
            overflow: 'hidden',
            width: '66%'
        },
        '*::-webkit-scrollbar-thumb': {
            outline: '1px solid slategrey',
            backgroundColor: 'rgba(81, 81, 81, .6)'
        },
        '*::-webkit-scrollbar': {
            width: '0.8em'
        }
    }
};

class CenterPanel extends React.Component<CenterPanelProps, CenterPanelState> {

    private nbOfColumnsInFullSize: number;
    private nbOfColumnsWhenImageIsSelected: number;
    protected refToLeftColum: React.RefObject<any>;

    constructor(props: CenterPanelProps) {
        super(props);
        this.handleClickInfo = this.handleClickInfo.bind(this);
        this.handleClickDelete = this.handleClickDelete.bind(this);
        this.handleClickDownload = this.handleClickDownload.bind(this);
        this.updateWindowDimensions = this.updateWindowDimensions.bind(this);
        this.nbOfColumnsInFullSize = 5;
        this.nbOfColumnsWhenImageIsSelected = 2;
        this.refToLeftColum = React.createRef()
    }

    handleClickInfo(img: ImageDto) {
        if (this.props.thunkActionForSelectImage != null && this.props.selectImage != null) {
            this.props.thunkActionForSelectImage(this.props.selectImage(img));
        }
    }
    handleClickDelete(img: ImageDto) {
        if (this.props.thunkActionForDeleteImage != null && this.props.deleteImage != null) {
            this.props.thunkActionForDeleteImage(this.props.deleteImage(img));
        }
    }

    handleClickDownload(img?: ImageDto | null) {
        if (img != null && img.data != null && this.props.thunkActionForDownloadImage != null && this.props.downloadImage != null) {
            this.props.thunkActionForDownloadImage(this.props.downloadImage(img));
        }

    }

    updateWindowDimensions() {
        if (this.state != null) {
            this.setState({
                min: this.state.min,
                max: this.state.max,
                width: window.innerWidth,
                height: window.innerHeight
            });
        }
    }

    componentDidMount() {
        this.updateWindowDimensions();
        window.addEventListener('resize', this.updateWindowDimensions);
    }

    componentWillUnmount() {
        this.updateWindowDimensions();
        window.removeEventListener('resize', this.updateWindowDimensions);
    }


    getWidth(img: ImageDto, cellHeight: number): number {
        if (img.orientation == 8) {
            return img.thumbnailHeight * (cellHeight / img.thumbnailWidth);
        }
        return img.thumbnailWidth * (cellHeight / img.thumbnailHeight);
    }

    getHeight(img: ImageDto, cellHeight: number): number {
        return cellHeight;
    }


    getImgRef(imgLinks?: ImageLinks | null): string {
        return imgLinks != null ? (imgLinks._img != null ? imgLinks._img.href : '') : '';
    }


    render() {
        if (this.props.displayImportImages) {
            return (<RealtimeImportImages />)
        } else
            if (this.props.imgs != null && this.props.imgs._embedded != null) {
                const titleOfImagesList = this.props.titleOfImagesList;
                const imageDtoes = this.props.imgs._embedded;
                const iconStyle = {
                    transform: "scale(0.5)"
                };
                var imageContent;
                imageContent = <div style={{ display: 'flex', justifyContent: 'center', backgroundColor: '#424242' }}><RightPanel /></div>;

                if (this.props.displaySelectedImage) {
                    return (
                        <React.Fragment>
                            <Grid container spacing={0} style={{ backgroundColor: 'rgb(66, 66, 66)' }}>
                                <Grid item ref={this.refToLeftColum}>
                                    <GridList cellHeight={200} cols={1} style={{ backgroundColor: 'rgb(66, 66, 66)' }}>
                                        <GridListTile key="Subheader" style={{ height: 'auto' }}>
                                            <ListSubheader component="div">{titleOfImagesList}</ListSubheader>
                                        </GridListTile>
                                        <GridListTile key="Subheader" style={{ height: 'auto' }}>
                                            <Grid container spacing={0} style={{ backgroundColor: 'rgb(66, 66, 66)' }}>
                                                <Grid item style={{ width: '100%' }} >
                                                    <GridList component="div" cellHeight={200} style={{ width: '100%', backgroundColor: 'rgb(66, 66, 66)', display: 'block', height: '90vh' }} cols={2}>
                                                        {imageDtoes.imageDtoList.filter(img => img.data != null && img.data.version == 1).map((img) => (
                                                            <GridListTile style={{ float: 'left', width: '50%' }} component="div">
                                                                <img src={this.getImgRef(img._links)} width={this.getWidth(img, 200)} height={this.getHeight(img, 200)} />
                                                                <GridListTileBar
                                                                    classes={{
                                                                        titleWrap: 'Mon-MuiGridListTileBar-titleWrap',
                                                                    }}
                                                                    title={
                                                                        <div style={{ display: 'table' }}>
                                                                            <div>{img.creationDateAsString}</div>
                                                                            <div>{img.imageId}</div>
                                                                        </div>
                                                                    }
                                                                    actionIcon={
                                                                        <div style={{ float: 'right', width: '100%' }}>
                                                                            <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} onClick={(e) => this.handleClickInfo(img)}>
                                                                                <InfoIcon style={iconStyle} />
                                                                            </IconButton>
                                                                            <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} onClick={(e) => this.handleClickDelete(img)} >
                                                                                <TrashIcon style={iconStyle} />
                                                                            </IconButton>
                                                                            <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} onClick={(e) => this.handleClickDownload(img)}>
                                                                                <CloudDownloadIcon style={iconStyle} />
                                                                            </IconButton>
                                                                        </div>
                                                                    }
                                                                />
                                                            </GridListTile>
                                                        ))
                                                        }
                                                    </GridList>
                                                </Grid>
                                            </Grid>
                                        </GridListTile>


                                    </GridList>
                                </Grid>
                                <Grid item>
                                    {imageContent}
                                </Grid>
                            </Grid>
                        </React.Fragment >);
                } else {
                    return (
                        <GridList cellHeight={200} cols={this.nbOfColumnsInFullSize} style={{ backgroundColor: '#000000' }}>
                            {imageDtoes.imageDtoList.filter(img => img.data != null && img.data.version == 1).map((img) => (
                                <GridListTile cols={1} >
                                    <img src={this.getImgRef(img._links)} width={this.getWidth(img, 200)} height={this.getHeight(img, 200)} />
                                    <GridListTileBar
                                        classes={{
                                            titleWrap: 'Mon-MuiGridListTileBar-titleWrap', // class name, e.g. `classes-nesting-root-x`
                                        }}
                                        title={
                                            <div style={{ display: 'table' }}>
                                                <div>{img.creationDateAsString}</div>
                                                <div>{img.imageId}</div>
                                            </div>
                                        }
                                        actionIcon={
                                            <div style={{ float: 'right', width: '100%' }}>
                                                <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} onClick={(e) => this.handleClickInfo(img)}>
                                                    <InfoIcon style={iconStyle} />
                                                </IconButton>
                                                <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} onClick={(e) => this.handleClickDelete(img)} >
                                                    <TrashIcon style={iconStyle} />
                                                </IconButton>
                                                <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} onClick={(e) => this.handleClickDownload(img)} >
                                                    <CloudDownloadIcon style={iconStyle} />
                                                </IconButton>
                                            </div>
                                        }
                                    />
                                </GridListTile>
                            ))
                            }
                        </GridList>
                    );
                }
            } else {
                // to put : <Backdrop />
                return (
                    <div style={{ backgroundColor: '#000000', height: '100vh' }}>
                        <CircularProgress color="secondary" style={{ backgroundColor: '#000000', marginTop: '50vh' }} />
                        <div style={{ color: '#ff0000' }}>
                            Images en cours de chargement...
                    </div>
                    </div>);
            }
    }
}

const mapStateToProps = (state: ClientApplicationState): CenterPanelProps => {
    if (state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        var currentPage: PageOfImageDto | null = null;

        if (state.reducerImagesList.imagesLoaded.images != null) {
            currentPage = state.reducerImagesList.imagesLoaded.images;
        } else if (state.reducerImagesAreStreamed.imagesAreStreamed.images != null) {
            currentPage = state.reducerImagesAreStreamed.imagesAreStreamed.images;
        }

        return {
            id: idGlobal,
            displaySelectedImage: true,
            imgs: currentPage,
            selectedImageIsPresent: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null,
            titleOfImagesList: state.reducerImagesList.imagesLoaded.titleOfImagesList,
        };
    }

    switch (state.reducerImagesAreStreamed.imagesAreStreamed.state) {
        case 'START_STREAMING': {
            return {
                id: idGlobal++,
                status: state.reducerImagesAreStreamed.imagesAreStreamed.state,
                min: state.reducerImagesAreStreamed.lastIntervallRequested.min,
                max: state.reducerImagesAreStreamed.lastIntervallRequested.min,
                imgs: null,
                titleOfImagesList: state.reducerImagesAreStreamed.imagesAreStreamed.titleOfImagesList,
                displaySelectedImage: false
            };
        }
        case 'STREAMING': {
            return {
                id: idGlobal++,
                status: state.reducerImagesAreStreamed.imagesAreStreamed.state,
                min: state.reducerImagesAreStreamed.lastIntervallRequested.min,
                max: state.reducerImagesAreStreamed.lastIntervallRequested.min,
                imgs: state.reducerImagesAreStreamed.imagesAreStreamed.images,
                titleOfImagesList: state.reducerImagesAreStreamed.imagesAreStreamed.titleOfImagesList,
                displaySelectedImage: false
            };
        }
    }




    return {
        id: idGlobal++,
        status: 'Not known',
        min: 0,
        max: 0,
        displaySelectedImage: false
    };
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        deleteImage: deleteImage,
        selectImage: selectedImageIsLoading,
        downloadImage: downloadSelectedImage,
        thunkActionForDeleteImage: (x: ApplicationEvent) => {
            const r = dispatchPhotoToDelete(x);
            return dispatch(r);
        },
        thunkActionForSelectImage: (x: ApplicationEvent) => {
            const r = dispatchImageToSelect(x);
            return dispatch(r);
        },
        thunkActionForDownloadImage: (x: ApplicationEvent) => {
            const r = dispatchDownloadSelectedImageEvent(x);
            return dispatch(r);
        },
    }
};

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(CenterPanel));
