import 'moment/locale/fr'

import { connect } from "react-redux";
import React from 'react';
import { ClientApplicationState } from '../redux/State';
import { ImageDto, ImageKeyDto, ExifOfImages, ExifDTO, ExifDToes, ExifsLink } from '../model/ImageDto';
import { IconButton } from '@material-ui/core';
import TrashIcon from '@material-ui/icons/Delete';
import CloudDownloadIcon from '@material-ui/icons/CloudDownload';
import CloseIcon from '@material-ui/icons/Close';
import { withStyles } from '@material-ui/core/styles';
import {
    ApplicationThunkDispatch,
    ApplicationEvent,
    dispatchPhotoToDelete,
    deleteImage,
    selectImage,
    nextImageToLoad,
    prevImageToLoad,
    dispatchPhotoToPrevious,
    dispatchPhotoToNext,
    dispatchSaveEvent,
    dispatchDeselectImageEvent,
    deselectImage,
    downloadSelectedImage,
    dispatchDownloadSelectedImageEvent,
    updateImage
} from '../redux/Actions';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import SkipNextIcon from '@material-ui/icons/SkipNext';
import SkipPreviousIcon from '@material-ui/icons/SkipPrevious';
import Rating from '@material-ui/lab/Rating';

import Grid from '@material-ui/core/Grid';
import { CSSProperties } from '@material-ui/styles';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import Box from '@material-ui/core/Box';

import ChipInput from 'material-ui-chip-input'
import ReactAutosuggestRemote from '../components/KeyWordsPersonsAutoSuggest'
import CircularProgress from '@material-ui/core/CircularProgress';


export interface RightPanelProps {
    isLoading?: boolean,
    isOpenedStateId?: number | 0,
    image?: ImageDto | null,
    exifOfImages?: ExifOfImages | null,
    thunkActionForDeleteImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDeselectImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    deselectImage?(): ApplicationEvent,
    deleteImage?(img: ImageKeyDto): ApplicationEvent,
    nextImageToLoad?(url: String): ApplicationEvent,
    prevImageToLoad?(url: String): ApplicationEvent,
    updateImage?(url: String, img: ImageDto): ApplicationEvent,
    downloadImage?(img: ImageDto): ApplicationEvent,
    thunkActionForNextImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForPreviousImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForSaveImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDownloadImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>
}

export interface RightPanelState {
    image?: ImageDto | null,
    exifOfImages?: ExifOfImages | null,
    isOpenedStateId: number,
    width: number,
    height: number,
    tabValue?: number,
    isLoading?: boolean
};
var globalId: number = 0;

const StyledTableRow = withStyles((theme) => ({
    root: {
        '&:nth-of-type(odd)': {
            backgroundColor: theme.palette.action.hover,
        },
    },
}))(TableRow);

export const styles = {
    '@global': {
        '.MuiTable-root': {
            fontSize: '10px'
        },
        '.MuiDrawer-root': {
            backgroundColor: 'rgba(66, 66, 66, 0.80)'
        },
        '.MuiDrawer-paper': {
            backgroundColor: 'inherit'
        },
        '.MuiIconButton-root': {
            padding: '12px'
        },
        '.MuiTableCell-sizeSmall': {
            fontSize: '10px'

        },
        '.MuiTableRow-head': {
            backgroundColor: '#999999'
        },
        '*::-webkit-scrollbar': {
            width: '1.2em'
        },
        '*::-webkit-scrollbar-track': {
            '-webkit-box-shadow': 'inset 0 0 6px rgba(0,0,0,0.00)'
        },
        '*::-webkit-scrollbar-thumb': {
            backgroundColor: 'rgba(0,0,0,.2)',
            outline: '1px solid slategrey'
        }
        ,
        '.Local-MuiToolbar-gutters': {
            padding: '0px'
        }
    }
};


class RightPanel extends React.Component<RightPanelProps, RightPanelState> {

    constructor(props: RightPanelProps) {
        super(props);
        this.state = { isOpenedStateId: 0, width: 0, height: 0 };
        this.handleClickDelete = this.handleClickDelete.bind(this);
        this.handleClickPrevious = this.handleClickPrevious.bind(this);
        this.handleClickNext = this.handleClickNext.bind(this);
        this.upHandler = this.upHandler.bind(this);
        this.setRatingValue = this.setRatingValue.bind(this);
        this.updateWindowDimensions = this.updateWindowDimensions.bind(this);
        this.closeImage = this.closeImage.bind(this);
        this.handleClickDownload = this.handleClickDownload.bind(this);
    }


    static getDerivedStateFromProps(props: RightPanelProps, state: RightPanelState): RightPanelState {
        if (props != null && props.isOpenedStateId != null && props.isOpenedStateId != state.isOpenedStateId) {
            const isOpenedStateId = props.isOpenedStateId != null ? props.isOpenedStateId : 0;
            return {
                isLoading: props.isLoading,
                isOpenedStateId: isOpenedStateId,
                image: props.image != null ? props.image : state.image,
                exifOfImages: props.exifOfImages != null ? props.exifOfImages : state.exifOfImages,
                width: state.width,
                height: state.height
            }
        } else {
            return state;
        }
    }



    handleClickPrevious(exifs?: ExifsLink | null) {
        if (exifs != null) {
            if (exifs._prev != null && this.props.thunkActionForNextImage != null && this.props.nextImageToLoad != null) {
                this.props.thunkActionForNextImage(this.props.nextImageToLoad(exifs._prev.href));
                this.setState({
                    isLoading: true,
                    image: this.state.image,
                    exifOfImages: this.state.exifOfImages,
                    isOpenedStateId: this.state.isOpenedStateId,
                    width: this.state.width,
                    height: this.state.height
                });
            }
        }
    }

    handleClickNext(exifs?: ExifsLink | null) {
        if (exifs != null) {
            if (exifs._next != null && this.props.thunkActionForNextImage != null && this.props.nextImageToLoad != null) {
                this.props.thunkActionForNextImage(this.props.nextImageToLoad(exifs._next.href));
                this.setState({
                    isLoading: true,
                    image: this.state.image,
                    exifOfImages: this.state.exifOfImages,
                    isOpenedStateId: this.state.isOpenedStateId,
                    width: this.state.width,
                    height: this.state.height
                });
            }
        }
    }

    handleClickDelete(img?: ImageDto | null) {
        if (img != null && img.data != null && this.props.thunkActionForDeleteImage != null && this.props.deleteImage != null) {
            this.props.thunkActionForDeleteImage(this.props.deleteImage(img.data));
        }

    }

    handleClickDownload(img?: ImageDto | null) {
        if (img != null && img.data != null && this.props.thunkActionForDownloadImage != null && this.props.downloadImage != null) {
            this.props.thunkActionForDownloadImage(this.props.downloadImage(img));
        }

    }

    closeImage() {
        if (this.props.thunkActionForDeselectImage != null && this.props.deselectImage) {
            this.props.thunkActionForDeselectImage(this.props.deselectImage());
        }
    }


    getImgRef(exifsLink?: ExifsLink | null): string {
        return (exifsLink != null && exifsLink._img != null)
            ? exifsLink._img.href : '';
    }

    decimalToHexString(number: number): string {
        if (number < 0) {
            number = 0xFFFFFFFF + number + 1;
        }

        return number.toString(16).toUpperCase();
    }

    upHandler(key: KeyboardEvent) {
        const exifsLink = this.state.exifOfImages != null ? this.state.exifOfImages._links : null;
        const img = this.state.image;
        switch (key.keyCode) {
            case 39: {
                this.handleClickNext(exifsLink);
                break;
            }
            case 37: {
                this.handleClickPrevious(exifsLink);
                break;
            }
            case 32: {
                this.handleClickDownload(img);
                break;
            }
            case 46: {
                this.handleClickDelete(img);
                break;
            }

        }

    }

    setRatingValue(ratingValue: number | null, img?: ImageDto | null) {
        if (img != null && ratingValue != null) {
            img.ratings = ratingValue;
            if (this.props.thunkActionForSaveImage != null && this.props.updateImage != null && img._links != null && img._links._upd != null) {
                this.props.thunkActionForSaveImage(this.props.updateImage(img._links._upd.href, img));
            }
        }
    }




    componentDidMount() {
        this.updateWindowDimensions();
        window.addEventListener('resize', this.updateWindowDimensions);
        window.addEventListener('keyup', this.upHandler);
    }

    componentWillUnmount() {
        window.removeEventListener('resize', this.updateWindowDimensions);
        window.removeEventListener('keyup', this.upHandler);
    }

    updateWindowDimensions() {
        if (this.state != null) {
            this.setState({
                isOpenedStateId: this.state.isOpenedStateId,
                image: this.state.image,
                exifOfImages: this.state.exifOfImages,
                width: window.innerWidth,
                height: window.innerHeight
            });
        }
    }

    getImgHeight(img?: ImageDto | null): number {
        if (img != null) {
            if (img.orientation == 8) {
                return img.thumbnailWidth;
            }
            return img.thumbnailHeight;
        }
        return 0
    }

    getImgWidth(img?: ImageDto | null): number {
        if (img != null) {
            if (img.orientation == 8) {
                return img.thumbnailHeight;
            }
            return img.thumbnailWidth;
        }
        return 0
    }

    getNewDimension(viewportWidth: number, viewportHeight: number, img?: ImageDto | null) {
        if (img != null) {
            const imgHeight = this.getImgHeight(img);
            const imgWidth = this.getImgWidth(img);

            const ratioH = viewportHeight / imgHeight / 1.1;
            const ratioW = imgWidth > viewportWidth / 2.25 ? viewportWidth / 2.25 / imgWidth : viewportWidth / imgWidth;
            const ratio = Math.min(ratioH, ratioW);
            return {
                newHeight: ratio < 1 ? imgHeight * ratio : imgHeight,
                newWidth: ratio < 1 ? imgWidth * ratio : imgWidth
            }
        }
        return {
            newHeight: 0,
            newWidth: 0
        }

    }

    getExifsToRender(exifsLink?: ExifsLink | null, exifDToes?: ExifDToes | null, img?: ImageDto | null) {

        const tableStyle = {
            fontSize: '12px',
            maxHeight: '550px',
            marginTop: '15px'
        }
        const aroundKeywords: CSSProperties = {
            padding: '8px',
            margin: '5px',
            border: 'solid 1px rgba(255,255,255,0.5)',
            borderRadius: '7px',
            position: 'relative'
        }

        const aroundPersons: CSSProperties = {
            padding: '8px',
            marginTop: '15px',
            marginLeft: '5px',
            marginRight: '5px',
            border: 'solid 1px rgba(255,255,255,0.5)',
            borderRadius: '7px',
            position: 'relative'
        }

        const overLayStyleLeft: CSSProperties = {
            backgroundColor: 'rgba(0,0,0,0.2)',
            color: '#ffffff',
            position: 'absolute',
            top: '10px',
            left: '10px',
            width: '300px',
            height: '150px',
            fontSize: '10px',
            right: '20px',
            display: 'table',
            padding: '8px',
            margin: '5px',
            border: 'solid 1px rgba(0,0,0,0.3)',
            borderRadius: '7px'
        }
        const overLayStyleRight: CSSProperties = {
            backgroundColor: 'rgba(0,0,0,0.0)',
            float: 'right',
            position: 'absolute',
            top: '10px',
            right: '0px',
            fontSize: '10px',
            display: 'table',
            padding: '8px',
            margin: '5px',
            border: 'solid 1px rgba(0,0,0,0.3)',
            borderRadius: '7px'
        }
        const overLayCircularProgress: CSSProperties = {
            position: 'absolute',
            right: '0px',
            top: '60px'
        }



        var moment = require('moment-timezone');
        const newDimension = this.getNewDimension(this.state.width, this.state.height, img);
        const imgWidth = newDimension.newWidth;
        const imgHeihgt = newDimension.newHeight;
        const ratingValue = img != null ? img.ratings : 0
        const orienation = img != null ? img.orientation : 0;
        const exifTableStyle: CSSProperties = {
            width: orienation != 8 ? imgWidth / 1.5 : imgWidth
        }
        const exifDToesToDisplay = exifDToes != null ? exifDToes : new ExifDToes();
        return (
            <div >
                <Grid container direction="row" >
                    <Grid item >
                        <div style={{ margin: '5px' }}>
                            <div style={{ position: 'relative' }}>
                                <img src={this.getImgRef(exifsLink)} width={imgWidth} height={imgHeihgt} style={{ marginTop: '11px' }} />
                                <div style={overLayStyleLeft}>
                                    <div style={{ float: 'left' }}> image id </div> <div>{img != null ? img.imageId : ' img not found'}</div>
                                    <div style={{ float: 'left' }}> version </div> <div>{img != null && img.data != null ? img.data.version : ''}</div>
                                    <div style={{ float: 'left' }}> date </div> <div>{img != null && img.data != null ? moment.tz(img.data.creationDate, 'Europe/Paris').format('YYYY-MM-DD HH:mm:ss +01') : ''}</div>
                                    <div style={{ float: 'left' }}> fichier </div> <div>{img != null && img.imageName != null ? img.imageName : ''}</div>
                                    <div style={{ float: 'left' }}> Dimensions </div> <div>{img != null ? (img.originalWidth + 'x' + img.originalHeight) : ''}</div>
                                    <div style={{ float: 'left' }}> Vit./Ouverture </div> <div>{img != null ? (img.speed + ' - F' + img.aperture) : ''}</div>
                                    <div style={{ float: 'left' }}> Iso</div> <div>{img != null ? img.iso : ''}</div>
                                    <div style={{ float: 'left' }}> Albums</div> <div>{img != null ? img.albums : ''}</div>
                                    <div style={{ float: 'left' }}> APN</div> <div>{img != null ? img.camera : ''}</div>
                                    <div style={{ float: 'left' }}> Objecttif</div> <div>{img != null ? img.lens : ''}</div>
                                </div>
                                <div style={overLayStyleRight}>
                                    <Rating
                                        name="simple-controlled"
                                        value={ratingValue}
                                        onChange={(event, newValue) => {
                                            this.setRatingValue(newValue, img);
                                        }}
                                    />
                                </div>
                                <div style={overLayCircularProgress}>
                                    {
                                        this.renderCircularProgressIfNeeded()
                                    }
                                </div>
                            </div>
                        </div>
                    </Grid>

                    <Grid item style={exifTableStyle} >
                        <Toolbar classes={{ gutters: 'Local-MuiToolbar-gutters' }}>
                            <Grid
                                container
                                style={{ backgroundColor: '#555555', margin: '5px' }}

                            >
                                <Grid item xs zeroMinWidth>
                                    <IconButton onClick={(e) => this.closeImage()}>
                                        <CloseIcon />
                                    </IconButton>
                                </Grid>
                                <Grid item xs zeroMinWidth>
                                    <IconButton onClick={(e) => this.handleClickPrevious(exifsLink)} >
                                        <SkipPreviousIcon />
                                    </IconButton>
                                </Grid>
                                <Grid item xs zeroMinWidth>
                                    <IconButton onClick={(e) => this.handleClickDelete(img)}>
                                        <TrashIcon />
                                    </IconButton>
                                </Grid>
                                <Grid item xs zeroMinWidth>
                                    <IconButton >
                                        <CloudDownloadIcon onClick={(e) => this.handleClickDownload(img)}></CloudDownloadIcon>
                                    </IconButton>
                                </Grid>
                                <Grid item xs zeroMinWidth>
                                    <IconButton onClick={(e) => this.handleClickNext(exifsLink)}>
                                        <SkipNextIcon />
                                    </IconButton>
                                </Grid>
                            </Grid>
                        </Toolbar>
                        <div style={aroundKeywords}>
                            <div style={{
                                position: 'absolute',
                                top: '-0.8em',
                                backgroundColor: 'rgba(81, 81, 81, 1)',
                                color: 'rgba(255, 255, 255, 0.3)',
                                paddingRight: '15px',
                                paddingLeft: '15px',
                                marginTop: '0.4em',
                                paddingBottom: '0.2em',
                                borderRadius: '5px',
                                fontSize: '0.8em'
                            }}>Mots-clés</div>
                            <ReactAutosuggestRemote />
                        </div>
                        <div style={aroundPersons}>
                            <div style={{
                                position: 'absolute',
                                top: '-0.8em',
                                backgroundColor: 'rgba(81, 81, 81, 1)',
                                color: 'rgba(255, 255, 255, 0.3)',
                                paddingRight: '15px',
                                paddingLeft: '15px',
                                marginTop: '0.2em',
                                paddingBottom: '0.2em',
                                borderRadius: '5px',
                                fontSize: '0.8em'
                            }}>Personnes identifiées</div>
                            <ReactAutosuggestRemote />
                        </div>

                        <div style={aroundPersons}>
                            <div style={{
                                position: 'absolute',
                                top: '-0.8em',
                                backgroundColor: 'rgba(81, 81, 81, 1)',
                                color: 'rgba(255, 255, 255, 0.3)',
                                paddingRight: '15px',
                                paddingLeft: '15px',
                                marginTop: '0.2em',
                                paddingBottom: '0.2em',
                                borderRadius: '5px',
                                fontSize: '0.8em'
                            }}>Paramètres EXIFs</div>
                            <TableContainer style={tableStyle} component={Paper}>
                                <Table stickyHeader size="small" >
                                    <TableHead>
                                        <TableRow>
                                            <TableCell align="right">Exif tag</TableCell>
                                            <TableCell align="right">Nom</TableCell>
                                            <TableCell align="right">Valeur</TableCell>
                                        </TableRow>
                                    </TableHead>
                                    <TableBody>
                                        {exifDToesToDisplay.exifDToes.map((row: ExifDTO) => (
                                            <StyledTableRow key={row.tagValue}>
                                                <TableCell component="th" scope="row">
                                                    {'0x' + this.decimalToHexString(row.tagValue)}
                                                </TableCell>
                                                <TableCell align="right">{row.displayableName}</TableCell>
                                                <TableCell align="right">{row.displayableValue}, path : {row.path}</TableCell>
                                            </StyledTableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </div>
                    </Grid>
                </Grid>
            </div >);
    }

    renderCircularProgressIfNeeded() {
        if (this.state.isLoading != null && this.state.isLoading) {
            return (<div>
                <CircularProgress color="secondary" />
                <div style={{ color: '#ff0000' }}>
                    Image en cours de chargement...
                        </div>
            </div>)
        } else {
            return (<React.Fragment></React.Fragment>)
        }
    }

    render() {

        return (
            <React.Fragment>

                {
                    this.getExifsToRender(
                        this.state.exifOfImages != null ? this.state.exifOfImages._links : null,
                        this.state.exifOfImages != null ? this.state.exifOfImages._embedded : null,
                        this.state.image)
                }
            </React.Fragment>
        );
    }

}

const mapStateToProps = (state: ClientApplicationState, previousState: RightPanelProps): RightPanelProps => {

    if (state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading) {
        return {
            isOpenedStateId: ++globalId,
            isLoading: true
        };
    }

    if (state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {

        if (state.reducerDisplayedExif.displayedExif.exifs != null) {
            return {
                isOpenedStateId: ++globalId,
                image: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image,
                exifOfImages: state.reducerDisplayedExif.displayedExif.exifs,
                isLoading: false
            };
        } else {
            return {
                isOpenedStateId: globalId,
                image: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image,
                isLoading: true,
                exifOfImages: null
            }
        }
    } else if (state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading) {
        return {
            isOpenedStateId: globalId,
            image: null,
            exifOfImages: null

        }
    }
    return previousState;
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        deleteImage: deleteImage,
        selectImage: selectImage,
        nextImageToLoad: nextImageToLoad,
        prevImageToLoad: prevImageToLoad,
        updateImage: updateImage,
        deselectImage: deselectImage,
        downloadImage: downloadSelectedImage,
        thunkActionForDeselectImage: (x: ApplicationEvent) => {
            const r = dispatchDeselectImageEvent(x);
            return dispatch(r);
        },

        thunkActionForDeleteImage: (x: ApplicationEvent) => {
            const r = dispatchPhotoToDelete(x);
            return dispatch(r);
        },
        thunkActionForNextImage: (x: ApplicationEvent) => {
            const r = dispatchPhotoToNext(x);
            return dispatch(r);
        },
        thunkActionForPreviousImage: (x: ApplicationEvent) => {
            const r = dispatchPhotoToPrevious(x);
            return dispatch(r);
        },
        thunkActionForSaveImage: (x: ApplicationEvent) => {
            const r = dispatchSaveEvent(x);
            return dispatch(r);
        },
        thunkActionForDownloadImage: (x: ApplicationEvent) => {
            const r = dispatchDownloadSelectedImageEvent(x);
            return dispatch(r);
        },


    }
};

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(RightPanel));
