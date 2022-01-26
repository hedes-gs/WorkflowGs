import 'moment/locale/fr'
import Grid, { GridProps } from '@mui/material/Grid';
import { withStyles } from '@mui/styles';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import { connect } from "react-redux";
import React from 'react';
import { ClientApplicationState } from '../redux/State';
import { ExchangedImageDTO, ImageLinks, PageOfExchangedImageDTO, ImageKeyDto, ImageDto } from '../model/DataModel';
import { ImageList, ImageListItem, ImageListItemBar, IconButton } from '@mui/material';
import InfoIcon from '@mui/icons-material/Info';
import TrashIcon from '@mui/icons-material/Delete';
import CloudDownloadIcon from '@mui/icons-material/CloudDownload';
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
import CircularProgress from '@mui/material/CircularProgress';
import RightPanel from './RightPanel';
import ListSubheader from '@mui/material/ListSubheader';
import RealtimeImportImages from './RealTimeImportImages';

let idGlobal: number = 0;

export interface CenterPanelProps {
    thunkActionForDeleteImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForSelectImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDownloadImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    deleteImage?(img?: ExchangedImageDTO): ApplicationEvent;
    selectImage?(img?: ExchangedImageDTO | undefined): ApplicationEvent;
    downloadImage?(img?: ExchangedImageDTO): ApplicationEvent;
    titleOfImagesList?: string;
    imgs?: PageOfExchangedImageDTO | null
    id?: number,
    min?: number,
    max?: number,
    status?: string,
    displaySelectedImage?: boolean,
    selectedImageIsPresent?: boolean,
    displayImportImages?: boolean,
    startStreaming?: boolean
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
        '.MuiChip-root': {
            borderRadius: '8px',
            fontSize: '12px'
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
        this.nbOfColumnsInFullSize = 10;
        this.nbOfColumnsWhenImageIsSelected = 2;
        this.refToLeftColum = React.createRef()
    }

    handleClickInfo(img?: ExchangedImageDTO) {
        if (this.props.thunkActionForSelectImage != null && this.props.selectImage != null) {
            this.props.thunkActionForSelectImage(this.props.selectImage(img));
        }
    }
    handleClickDelete(img?: ExchangedImageDTO) {
        if (this.props.thunkActionForDeleteImage != null && this.props.deleteImage != null) {
            this.props.thunkActionForDeleteImage(this.props.deleteImage(img));
        }
    }

    handleClickDownload(img?: ExchangedImageDTO) {
        if (this.props.thunkActionForDownloadImage != null && this.props.downloadImage != null) {
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

    shouldComponentUpdate(nextProps: CenterPanelProps, nextState: CenterPanelState): boolean {
        return nextProps.displayImportImages ||
            (nextProps.startStreaming != this.props.startStreaming) ||
            (nextProps.displaySelectedImage != this.props.displaySelectedImage) ||
            (nextProps.imgs != null && nextProps.imgs._embedded != null && (this.props.imgs != null && this.props.imgs._embedded != null && this.props.imgs._embedded.ExchangedImageDTOList.length != nextProps.imgs._embedded.ExchangedImageDTOList.length))
    }


    getWidth(cellHeight: number, img?: ExchangedImageDTO): number {
        let image = img?.image;
        let retValue = 0;
        if (image) {
            if (image?.orientation == 8) {
                retValue = image?.thumbnailHeight * (cellHeight / image?.thumbnailWidth);
            } else {
                retValue = image?.thumbnailWidth * (cellHeight / image?.thumbnailHeight);
            }
        }
        return retValue;
    }

    getHeight(cellHeight: number, img: ExchangedImageDTO): number {
        return cellHeight;
    }


    getImgRef(imgLinks?: ImageLinks): string {
        return imgLinks != null ? (imgLinks._img != null ? imgLinks._img.href : '') : '';
    }

    getLowImgRef(imgLinks?: ImageLinks): string {
        return imgLinks != null ? (imgLinks._lowRes != null ? imgLinks._lowRes.href : '') : '';
    }


    render() {
        const titleOfImagesList = this.props.titleOfImagesList;
        const ExchangedImageDTOes = this.props.imgs?._embedded;
        const exchangedImageDTOList = ExchangedImageDTOes?.ExchangedImageDTOList ?? [];

        const iconStyle = {
            transform: "scale(0.5)"
        };

        const theme = createTheme({

            components: {
                // Name of the component
                MuiSvgIcon: {
                    styleOverrides: {
                        root: {
                            color: 'white'
                        },
                    }
                },
                MuiChip:{
                    styleOverrides: {
                        label: {
                            fontSize: '0.6em'
                        },
                        root:{
                            borderRadius :'5px'
                        }
                    }
                },
                MuiFormControl:{
                    styleOverrides: {
                        root:{
                            width: '95%'
                        }
                    }
                },
                MuiAutocomplete:{
                    styleOverrides:{
                        inputRoot:{
                            margin: '6px'
                        },
                        noOptions: {
                            fontSize: '0.6em'
                        },
                        option: {
                            fontSize: '0.6em'
                        },
                    }
                },
                MuiInput:{
                    styleOverrides:{
                        root:{
                            before: {
                                borderBottom: '1px solid rgba(0, 0, 0, 0.12)'
                            },
                            margin: '1px',
                            borderBottom: '1px solid rgba(0, 0, 0, 0.12)',
                            hover :{
                                borderBottom: '1px solid rgba(0, 0, 0, 0.12)'
                            }   
                        },
                    }
                },
                MuiButton: {
                    styleOverrides: {
                        root: {
                            fontSize: '0.2em'
                        },
                    }
                },
                MuiImageListItem: {
                    styleOverrides: {

                        // Name of the slot
                        standard: {
                            backgroundColor: 'grey'
                        },
                        img: {
                            // Some CSS
                            width: 'initial',
                            height: 'initial',
                            objectFit: 'initial',
                            WebkitFlexGrow: 'initial',

                        },
                    },
                },
            },
        });
        if (this.props.displayImportImages) {
            return (<RealtimeImportImages />)
        }
        else if (this.props.startStreaming != null && this.props.startStreaming) {
            return (
                <div style={{ backgroundColor: '#000000', height: '80vh' }}>
                    <CircularProgress color="secondary" style={{ backgroundColor: '#000000', marginTop: '50vh' }} />
                    <div style={{ color: '#ff0000' }}>
                        Images en cours de chargement...
                    </div>
                </div>);

        } else if (this.props.displaySelectedImage) {
            var imageContent;
            imageContent = <div style={{ backgroundColor: '#424242' }}><RightPanel /></div>;
            return (
                <ThemeProvider theme={theme}>
                    <div>
                        <Grid container spacing={0} style={{ backgroundColor: 'rgb(66, 66, 66)' }} >
                            <Grid item ref={this.refToLeftColum} >
                                <div style={{ display: 'block', height: '80vh', flexWrap: 'wrap', justifyContent: 'space-around', overflowX: 'hidden', overflowY: 'scroll' }}>
                                    <ImageList style={{ backgroundColor: '#000000' }} cols={2} >
                                        {exchangedImageDTOList.map((img) => (
                                            <ImageListItem  >
                                                <img src={this.getLowImgRef(img?._links)} onClick={(e) => this.handleClickInfo(img)}  style={{ margin: 'auto' }} />
                                                <ImageListItemBar
                                                    position="below"
                                                    sx={{
                                                        background:
                                                            'linear-gradient(to bottom, rgba(0,0,0,0.7) 0%, ' +
                                                            'rgba(0,0,0,0.3) 70%, rgba(0,0,0,0) 100%)',
                                                    }}
                                                    title={
                                                        <div style={{ fontSize: '0.5em', display: 'table', color: 'white' }}>
                                                            <div>{img?.image?.creationDateAsString}</div>
                                                            <div>{img?.image?.imageId}</div>
                                                        </div>
                                                    }
                                                    actionIcon={
                                                        <div style={{ float: 'right', width: '100%' }}>
                                                            <IconButton
                                                                style={{ float: 'left', margin: '0px', padding: '0px' }}
                                                                onClick={(e) => this.handleClickInfo(img)}
                                                                size="large">
                                                                <InfoIcon style={iconStyle} />
                                                            </IconButton>
                                                            <IconButton
                                                                style={{ float: 'left', margin: '0px', padding: '0px' }}
                                                                onClick={(e) => this.handleClickDelete(img)}
                                                                size="large">
                                                                <TrashIcon style={iconStyle} />
                                                            </IconButton>
                                                            <IconButton
                                                                style={{ float: 'left', margin: '0px', padding: '0px' }}
                                                                onClick={(e) => this.handleClickDownload(img)}
                                                                size="large">
                                                                <CloudDownloadIcon style={iconStyle} />
                                                            </IconButton>
                                                        </div>
                                                    }
                                                />
                                            </ImageListItem>
                                        ))
                                        }

                                    </ImageList>
                                </div>
                            </Grid>
                            <Grid item style={{margin: 'auto'}} >
                                <Grid
                                    container
                                    direction="row"
                                    justifyContent="center"
                                    alignItems="center"
                                    style={{margin: 'auto'}}>
                                    <Grid item>
                                        {imageContent}
                                    </Grid>
                                </Grid>
                            </Grid>
                        </Grid>
                    </div>
                </ThemeProvider>
            );
        }
        else {

            return (
                <ThemeProvider theme={theme}>
                    <div>
                        <div style={{ display: 'block', height: '80vh', flexWrap: 'wrap', justifyContent: 'space-around', overflowX: 'hidden', overflowY: 'scroll' }}>
                            <ImageList style={{ backgroundColor: '#000000' }} cols={5} >
                                {exchangedImageDTOList.map((img) => (
                                    <ImageListItem
                                    >
                                        <img src={this.getLowImgRef(img?._links)} onClick={(e) => this.handleClickInfo(img)} style={{ margin: 'auto' }}
                                        />
                                        <ImageListItemBar
                                            position="below"
                                            sx={{
                                                background:
                                                    'linear-gradient(to bottom, rgba(0,0,0,0.7) 0%, ' +
                                                    'rgba(0,0,0,0.3) 70%, rgba(0,0,0,0) 100%)',
                                            }}
                                            title={
                                                <div style={{ fontSize: '0.5em', display: 'table', color: 'white' }}>
                                                    <div>{img?.image?.creationDateAsString}</div>
                                                    <div>{img?.image?.imageId}</div>
                                                </div>
                                            }
                                            actionIcon={
                                                <div style={{ float: 'right', width: '100%' }}>
                                                    <IconButton
                                                        style={{ float: 'left', margin: '0px', padding: '0px' }}
                                                        onClick={(e) => this.handleClickInfo(img)}
                                                        size="large">
                                                        <InfoIcon style={iconStyle} />
                                                    </IconButton>
                                                    <IconButton
                                                        style={{ float: 'left', margin: '0px', padding: '0px' }}
                                                        onClick={(e) => this.handleClickDelete(img)}
                                                        size="large">
                                                        <TrashIcon style={iconStyle} />
                                                    </IconButton>
                                                    <IconButton
                                                        style={{ float: 'left', margin: '0px', padding: '0px' }}
                                                        onClick={(e) => this.handleClickDownload(img)}
                                                        size="large">
                                                        <CloudDownloadIcon style={iconStyle} />
                                                    </IconButton>
                                                </div>
                                            }
                                        />
                                    </ImageListItem>
                                ))
                                }
                            </ImageList>

                        </div>

                    </div>
                </ThemeProvider>
            );
        }
    }
}

const mapStateToProps = (state: ClientApplicationState): CenterPanelProps => {
    if (state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        var currentPage: PageOfExchangedImageDTO | null = null;

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
            startStreaming: false
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
                displaySelectedImage: false,
                startStreaming: true
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
                displaySelectedImage: false,
                startStreaming: false
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

export default connect(mapStateToProps, mapDispatchToProps)(CenterPanel);