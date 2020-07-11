import 'moment/locale/fr'

import { connect } from "react-redux";
import React from 'react';
import { ClientApplicationState } from '../redux/State';
import { ImageDto, ImageLinks, PageOfImageDto, ImageKeyDto } from '../model/ImageDto';
import { GridList, GridListTile, GridListTileBar, IconButton } from '@material-ui/core';
import InfoIcon from '@material-ui/icons/Info';
import TrashIcon from '@material-ui/icons/Delete';
import CloudDownloadIcon from '@material-ui/icons/CloudDownload';
import { withStyles } from '@material-ui/core/styles';
import { ApplicationThunkDispatch, ApplicationEvent, dispatchPhotoToDelete, deleteImage, selectedImageIsLoading, dispatchImageToSelect } from '../redux/Actions';
import CircularProgress from '@material-ui/core/CircularProgress';
import RightPanel from './RightPanel';


let idGlobal: number = 0;

export interface CenterPanelProps {
    thunkActionForDeleteImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForSelectImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    deleteImage?(img: ImageKeyDto): ApplicationEvent;
    selectImage?(url: string, exifUrl: string): ApplicationEvent;
    imgs?: PageOfImageDto | null
    id: number,
    min?: number,
    max?: number,
    status?: string,
    displaySelectedImage: boolean,
    selectedImageIsPresent?: boolean
}
export interface CenterPanelState {
    min: number,
    max: number
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
        }
    }
};

class CenterPanel extends React.Component<CenterPanelProps, CenterPanelState> {

    constructor(props: CenterPanelProps) {
        super(props);
        this.handleClickInfo = this.handleClickInfo.bind(this);
        this.handleClickDelete = this.handleClickDelete.bind(this);
    }

    handleClickInfo(img: ImageDto) {
        if (img.data != null && img._links != null && img._links.self != null && img._links._exif != null && img.data != null && this.props.thunkActionForSelectImage != null && this.props.selectImage != null) {
            this.props.thunkActionForSelectImage(this.props.selectImage(img._links.self.href, img._links._exif.href));
        }
    }
    handleClickDelete(img: ImageDto) {
        if (img.data != null && this.props.thunkActionForDeleteImage != null && this.props.deleteImage != null) {
            this.props.thunkActionForDeleteImage(this.props.deleteImage(img.data));
        }
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
        if (this.props.imgs != null && this.props.imgs._embedded != null) {

            const imageDtoes = this.props.imgs._embedded;
            const iconStyle = {
                transform: "scale(0.5)"
            };
            var imageContent;
            imageContent = <div style={{ display: 'flex', justifyContent: 'center', backgroundColor: '#424242' }}><RightPanel /></div>;

            if (this.props.displaySelectedImage) {
                return (
                    <React.Fragment>
                        {imageContent}

                        <GridList cellHeight={200} cols={7} style={{ backgroundColor: '#000000', flexWrap: 'nowrap', transform: 'translateZ(0)' }}>
                            {imageDtoes.imageDtoes.filter(img => img.data != null && img.data.version == 1).map((img) => (
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
                                                <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} >
                                                    <CloudDownloadIcon style={iconStyle} />
                                                </IconButton>
                                            </div>
                                        }
                                    />
                                </GridListTile>
                            ))
                            }
                        </GridList>
                    </React.Fragment>);
            } else {
                return (
                    <GridList cellHeight={300} cols={5} style={{ backgroundColor: '#000000' }}>
                        {imageDtoes.imageDtoes.filter(img => img.data != null && img.data.version == 1).map((img) => (
                            <GridListTile cols={1} >
                                <img src={this.getImgRef(img._links)} width={this.getWidth(img, 300)} height={this.getHeight(img, 300)} />
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
                                            <IconButton style={{ float: 'left', margin: '0px', padding: '0px' }} >
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
    if (state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading || state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        return {
            id: idGlobal,
            displaySelectedImage: true,
            imgs: state.reducerImagesList.imagesLoaded.images,
            selectedImageIsPresent: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null
        };
    }
    switch (state.reducerImagesList.imagesLoaded.state) {
        case 'LOADING': {
            return {
                id: idGlobal++,
                status: state.reducerImagesList.imagesLoaded.state,
                min: 0,
                max: 0,
                displaySelectedImage: false
            };
        }
        case 'LOADED': {
            return {
                id: idGlobal,
                status: state.reducerImagesList.imagesLoaded.state,
                min: state.reducerImagesList.lastIntervallRequested.min,
                max: state.reducerImagesList.lastIntervallRequested.min,
                imgs: state.reducerImagesList.imagesLoaded.images,
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
        thunkActionForDeleteImage: (x: ApplicationEvent) => {
            const r = dispatchPhotoToDelete(x);
            return dispatch(r);
        },
        thunkActionForSelectImage: (x: ApplicationEvent) => {
            const r = dispatchImageToSelect(x);
            return dispatch(r);
        }
    }
};

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(CenterPanel));
