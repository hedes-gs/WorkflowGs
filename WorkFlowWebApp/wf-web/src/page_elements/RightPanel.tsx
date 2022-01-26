import 'moment/locale/fr'

import { connect } from "react-redux";
import React from 'react';
import { ClientApplicationState } from '../redux/State';
import { ExchangedImageDTO, ImageKeyDto, ExifOfImages, ExifDTO, ExifDToes, ExifsLink } from '../model/DataModel';
import { IconButton } from '@mui/material';
import TrashIcon from '@mui/icons-material/Delete';
import CloudDownloadIcon from '@mui/icons-material/CloudDownload';
import CloseIcon from '@mui/icons-material/Close';
import Button from '@mui/material/Button';
import PropTypes from 'prop-types';

import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemAvatar from '@mui/material/ListItemAvatar';
import ListItemText from '@mui/material/ListItemText';
import DialogTitle from '@mui/material/DialogTitle';
import Dialog from '@mui/material/Dialog';
import withStyles from '@mui/styles/withStyles';
import {
    ApplicationThunkDispatch,
    ApplicationEvent,
    dispatchPhotoToDelete,
    deleteImmediatelyImage,
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
    dispatchCheckoutImage,
    updateImage,
    dispatchPhotoToDeleteImmediately,
    downloadImmediatelyImage
} from '../redux/Actions';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import SkipNextIcon from '@mui/icons-material/SkipNext';
import SkipPreviousIcon from '@mui/icons-material/SkipPrevious';
import Rating from '@mui/material/Rating';

import Grid from '@mui/material/Grid';
import { CSSProperties } from '@mui/styles';
import { PersonsElement, KeywordsElement, AlbumsElement } from '../components/KeyWordsPersonsAutoSuggest'
import CircularProgress from '@mui/material/CircularProgress';


export interface RightPanelProps {
    isLoading?: boolean,
    isOpenedStateId?: number | 0,
    image?: ExchangedImageDTO | null,
    exifOfImages?: ExifOfImages | null,
    thunkActionForDoImmediaterlyDeleteImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDeselectImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    deselectImage?(): ApplicationEvent,
    deleteImmediatelyImage?(img?: ExchangedImageDTO): ApplicationEvent,
    nextImageToLoad?(img?: ExchangedImageDTO): ApplicationEvent,
    prevImageToLoad?(img?: ExchangedImageDTO): ApplicationEvent,
    updateImage?(url: String, img?: ExchangedImageDTO): ApplicationEvent,
    downloadImmediatelyImage?(img?: ExchangedImageDTO): ApplicationEvent,
    thunkActionForNextImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForPreviousImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForSaveImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDownloadImageImmediately?: (x: ApplicationEvent) => Promise<ApplicationEvent>
}

export interface RightPanelState {
    image?: ExchangedImageDTO,
    exifOfImages?: ExifOfImages,
    isOpenedStateId: number,
    width: number,
    height: number,
    tabValue?: number,
    isLoading?: boolean,
    exifDialogIsOpen?: boolean
    albumbDialogIsOpen?: boolean
    peopleDialogIsOpen?: boolean
    tagsDialogIsOpen?: boolean

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
        '.MuiRating-root': {
            fontSize: '0.8rem'
        },
        '.MuiSvgIcon-root': {
            fontSize: '0.8rem'
        },
        '.MuiToolbar-regular': {
            minHeight: 'unset'
        },
        '.MuiTableRow-head': {
            backgroundColor: '#999999'
        },
        '.WAMuiChipInput-inputRoot-13': {
            flex: 'none'
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

SimpleExifsDialog.propTypes = {
    onClose: PropTypes.func.isRequired,
    open: PropTypes.bool.isRequired,
    exifDToesToDisplay: ExifDToes
}
SimpleAlbumDialog.propTypes = {
    onClose: PropTypes.func.isRequired,
    open: PropTypes.bool.isRequired,

}
SimpleTagDialog.propTypes = {
    onClose: PropTypes.func.isRequired,
    open: PropTypes.bool.isRequired,

}
SimplePeopleDialog.propTypes = {
    onClose: PropTypes.func.isRequired,
    open: PropTypes.bool.isRequired,

}


function decimalToHexString(number: number): string {
    if (number < 0) {
        number = 0xFFFFFFFF + number + 1;
    }

    return number.toString(16).toUpperCase();
}

function SimpleAlbumDialog(props: any): any {

    const { onClose,  open } = props;

    const handleClose = () => {
        onClose();
    };
    return (
        <Dialog onClose={handleClose} open={open}>
            <DialogTitle>Albums</DialogTitle>
            <AlbumsElement />
        </Dialog>
    );
}

function SimpleTagDialog(props: any): any {

    const { onClose,  open } = props;

    const handleClose = () => {
        onClose();
    };
    return (
        <Dialog onClose={handleClose} open={open}>
            <DialogTitle>Mots clés</DialogTitle>
            <KeywordsElement />
        </Dialog>
    );
}

function SimplePeopleDialog(props: any): any {

    const { onClose,  open } = props;

    const handleClose = () => {
        onClose();
    };
    return (
        <Dialog onClose={handleClose} open={open}>
            <DialogTitle>Personnes identifiées</DialogTitle>
            <PersonsElement/>
        </Dialog>
    );
}






function SimpleExifsDialog(props: any): any {

    const tableExifStyle = {
        fontSize: '12px',
        maxHeight: '40.4vh',
        marginTop: '15px',

    }
    const aroundPersons: CSSProperties = {
        marginTop: '15px',
        position: 'relative',
        width: '25%'
    }
    const titleProperties: CSSProperties = {
        position: 'absolute',
        top: '-0.8em',
        backgroundColor: 'rgba(81, 81, 81, 1)',
        color: 'rgba(255, 255, 255, 0.6)',
        marginTop: '0.2em',
        paddingBottom: '0.2em',
        borderRadius: '5px',
        fontSize: '0.5em',
        width: '95%'
    }

    const { onClose, selectedValue, open } = props;

    const handleClose = () => {
        onClose();
    };
    return (
        <Dialog onClose={handleClose} open={open}>
            <DialogTitle>Paramètres EXIFs</DialogTitle>
            <TableContainer style={tableExifStyle} component={Paper}>
                <Table stickyHeader size="small" >
                    <TableHead>
                        <TableRow>
                            <TableCell align="left">Exif tag</TableCell>
                            <TableCell align="left">Nom</TableCell>
                            <TableCell align="left">Valeur</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {props.exifDToesToDisplay.exifDTOList.map((row: ExifDTO) => (
                            <StyledTableRow key={row.tagValue}>
                                <TableCell component="th" scope="row">
                                    {'0x' + decimalToHexString(row.tagValue)}
                                </TableCell>
                                <TableCell align="left">{row.displayableName}</TableCell>
                                <TableCell align="left">{row.displayableValue}, path : {row.path}</TableCell>
                            </StyledTableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
        </Dialog>
    );
}


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
        this.handleClickExifsOpen = this.handleClickExifsOpen.bind(this);
        this.handleClickExifsClose = this.handleClickExifsClose.bind(this);
         this.handleClickPeopleClose = this.handleClickPeopleClose.bind(this);
        this.handleClickTagsClose = this.handleClickTagsClose.bind(this);
        this.handleClickAlbumClose = this.handleClickAlbumClose.bind(this);
        this.handleClickTagsOpen = this.handleClickTagsOpen.bind(this);
        this.handleClickAlbumOpen = this.handleClickAlbumOpen.bind(this);
        this.handleClickPeopleOpen  = this.handleClickPeopleOpen.bind(this);
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



    handleClickPrevious(img?: ExchangedImageDTO | null) {
        if (this.props.prevImageToLoad != null && this.props.thunkActionForPreviousImage != null && img != null) {
            this.props.thunkActionForPreviousImage(this.props.prevImageToLoad(img));
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


    handleClickNext(img?: ExchangedImageDTO | null) {
        if (this.props.thunkActionForNextImage != null && this.props.nextImageToLoad != null && img != null) {
            this.props.thunkActionForNextImage(this.props.nextImageToLoad(img));
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

    handleClickDelete(img?: ExchangedImageDTO) {
        if (img != null && this.props.thunkActionForDoImmediaterlyDeleteImage != null && this.props.deleteImmediatelyImage != null) {
            this.props.thunkActionForDoImmediaterlyDeleteImage(this.props.deleteImmediatelyImage(img));
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

    handleClickExifsOpen() {
        this.setState({
            exifDialogIsOpen: true
        });
    }

    handleClickExifsClose() {
        this.setState({
            exifDialogIsOpen: false
        });
    }

    handleClickAlbumClose() {
        this.setState({
            albumbDialogIsOpen: false
        });
    }
    handleClickAlbumOpen() {
        this.setState({
            albumbDialogIsOpen: true
        });
    }

    handleClickTagsClose() {
        this.setState({
            tagsDialogIsOpen: false
        });
    }
    handleClickTagsOpen() {
        this.setState({
            tagsDialogIsOpen: true
        });
    }
    handleClickPeopleClose() {
        this.setState({
            peopleDialogIsOpen: false
        });
    }
    handleClickPeopleOpen() {
        this.setState({
            peopleDialogIsOpen: true
        });
    }




    handleClickDownload(img?: ExchangedImageDTO) {
        if (this.props.thunkActionForDownloadImageImmediately != null && this.props.downloadImmediatelyImage != null) {
            this.props.thunkActionForDownloadImageImmediately(this.props.downloadImmediatelyImage(img));
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



    upHandler(key: KeyboardEvent) {
        const img = this.state.image;
        switch (key.key) {
            case "Delete": {
                this.handleClickDelete(img);
                break;
            }
            case "ArrowRight": {
                this.handleClickNext(img);
                break;
            }
            case "ArrowLeft": {
                this.handleClickPrevious(img);
                break;
            }
            case " ":
            case "Space": {
                this.handleClickDownload(img);
                break;
            }
        }

    }

    setRatingValue(ratingValue: number | null, img?: ExchangedImageDTO | null) {
        if (img != null && img.image != null && ratingValue != null) {
            img.image.ratings = ratingValue;
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

    getImgHeight(img?: ExchangedImageDTO): number {
        if (img != null && img?.image != null) {
            if (img?.image?.orientation == 8) {
                return img?.image?.thumbnailWidth;
            }
            return img?.image?.thumbnailHeight;
        }
        return 0
    }

    getImgWidth(img?: ExchangedImageDTO): number {
        if (img != null && img?.image != null) {
            if (img?.image?.orientation == 8) {
                return img?.image?.thumbnailHeight;
            }
            return img?.image?.thumbnailWidth;
        }
        return 0
    }

    getNewDimension(viewportWidth: number, viewportHeight: number, img?: ExchangedImageDTO | null) {
        if (img != null) {
            const imgHeight = this.getImgHeight(img);
            const imgWidth = this.getImgWidth(img);

            const maxAllowedHeight = viewportHeight * 0.70;


            const ratioH = maxAllowedHeight / imgHeight;
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



    getExifsToRender(exifsLink?: ExifsLink, exifDToes?: ExifDToes, img?: ExchangedImageDTO) {

        const tableStyle = {
            fontSize: '12px',
            maxHeight: '53.4vh',
            marginTop: '15px'
        }
        const tableExifStyle = {
            fontSize: '12px',
            maxHeight: '40.4vh',
            marginTop: '15px',

        }
        const aroundKeywords: CSSProperties = {
            marginTop: '15px',
            position: 'relative',
            width: '25%'
        }

        const aroundPersons: CSSProperties = {
            marginTop: '15px',
            position: 'relative',
            width: '25%'
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

        const overLayStyleBottomLeft: CSSProperties = {
            backgroundColor: 'rgba(0,0,0,0.3)',
            color: '#ffffff',
            position: 'absolute',
            bottom: '10px',
            display: 'table',
            border: 'solid 1px rgba(0,0,0,0.3)',
            borderRadius: '7px',
            fontSize: '0.8rem',
            marginLeft: '5px',
        }

        const overLayStyleBottomRight: CSSProperties = {
            backgroundColor: 'rgba(0,0,0,0.3)',
            float: 'right',
            position: 'absolute',
            bottom: '10px',
            right: '0px',
            fontSize: '0.8rem',
            display: 'table',
            padding: '10px',
            marginRight: '5px',
            border: 'solid 1px rgba(0,0,0,0.3)',
            borderRadius: '7px'
        }
        const overLayCircularProgress: CSSProperties = {
            position: 'absolute',
            right: '0px',
            top: '60px'
        }

        const titleProperties: CSSProperties = {
            position: 'absolute',
            backgroundColor: 'rgba(81, 81, 81, 1)',
            color: 'rgba(255, 255, 255, 0.6)',
            marginTop: '0.2em',
            paddingBottom: '0.2em',
            borderRadius: '5px',
            fontSize: '0.5em',
            width: '95%'
        }

        var moment = require('moment-timezone');
        const newDimension = this.getNewDimension(this.state.width, this.state.height, img);
        const imgWidth = newDimension.newWidth;
        const imgHeihgt = newDimension.newHeight;
        const ratingValue = img != null ? img?.image?.ratings : 0
        const orienation = img != null ? img?.image?.orientation : 0;
        const exifTableStyle: CSSProperties = {
            width: orienation != 8 ? imgWidth / 1.5 : imgWidth
        }
        const exifDToesToDisplay = exifDToes != null ? exifDToes : new ExifDToes();

        return (
            <div >
                <Grid container direction="column" style={{ height: '80vh' }} >
                    <Grid item >
                        <Grid container spacing={1}>
                            <Grid item style={aroundKeywords} >
                                <Button variant="contained" style={{
                                    width: '100%',
                                    marginBottom: '5px'
                                }} onClick={this.handleClickTagsOpen} >Tags...</Button>
                            </Grid>
                            <Grid item style={aroundPersons}>
                                <Button variant="contained" style={{
                                    width: '100%',
                                    marginBottom: '5px'
                                }} onClick={this.handleClickPeopleOpen} >Personnes...</Button>
                            </Grid>
                            <Grid item style={aroundPersons}>
                                <Button variant="contained" style={{
                                    width: '100%',
                                    marginBottom: '5px'
                                }} onClick={this.handleClickAlbumOpen} >Albums...</Button>
                            </Grid>
                            <Grid item style={aroundPersons}>
                                <Button variant="contained" style={{
                                    width: '100%',
                                    marginBottom: '5px'
                                }} onClick={this.handleClickExifsOpen} >Exifs...</Button>
                            </Grid>
                        </Grid>
                    </Grid>
                    <Grid item >
                        <div style={{ marginLeft: '5px' }}>
                            <div style={{ position: 'relative' }}>
                                <img src={this.getImgRef(exifsLink)} width={imgWidth} height={imgHeihgt} />
                                <div style={overLayStyleLeft}>
                                    <div style={{ float: 'left' }}> image id </div> <div>{img != null ? img?.image?.imageId : ' img not found'}</div>
                                    <div style={{ float: 'left' }}> version </div> <div>{img != null && img.image != null && img.image.data != null ? img?.image?.data.version : ''}</div>
                                    <div style={{ float: 'left' }}> date </div> <div>{img != null && img.image != null && img.image.data != null ? moment.tz(img.image.data.creationDate, 'Europe/Paris').format('YYYY-MM-DD HH:mm:ss +01') : ''}</div>
                                    <div style={{ float: 'left' }}> fichier </div> <div>{img != null && img.image != null && img.image.data != null ? img.image.imageName : ''}</div>
                                    <div style={{ float: 'left' }}> Dimensions </div> <div>{img != null && img.image != null ? (img.image.originalWidth + 'x' + img.image.originalHeight) : ''}</div>
                                    <div style={{ float: 'left' }}> Vit./Ouverture </div> <div>{img != null && img.image != null ? (img.image.speed + ' - F' + img.image.aperture) : ''}</div>
                                    <div style={{ float: 'left' }}> Iso</div> <div>{img != null && img.image != null ? img.image.iso : ''}</div>
                                    <div style={{ float: 'left' }}> Albums</div> <div>{img != null && img.image != null ? img.image.albums : ''}</div>
                                    <div style={{ float: 'left' }}> APN</div> <div>{img != null && img.image != null ? img.image.camera : ''}</div>
                                    <div style={{ float: 'left' }}> Objecttif</div> <div>{img != null && img.image != null ? img.image.lens : ''}</div>
                                </div>

                                <div style={overLayStyleBottomLeft}>
                                    <IconButton
                                        style={{ fontSize: '0.8rem' }}
                                        onClick={(e) => this.handleClickPrevious(this.state.image)}
                                        size="large">
                                        <SkipPreviousIcon />
                                    </IconButton>
                                    <IconButton style={{ fontSize: '0.8rem' }} size="large">
                                        <CloudDownloadIcon onClick={(e) => this.handleClickDownload(img)}></CloudDownloadIcon>
                                    </IconButton >
                                    <IconButton
                                        style={{ fontSize: '0.8rem' }}
                                        onClick={(e) => this.closeImage()}
                                        size="large">
                                        <CloseIcon />
                                    </IconButton>
                                    <IconButton
                                        style={{ fontSize: '0.8rem' }}
                                        onClick={(e) => this.handleClickDelete(this.state.image)}
                                        size="large">
                                        <TrashIcon />
                                    </IconButton>
                                    <IconButton
                                        style={{ fontSize: '0.8rem' }}
                                        onClick={(e) => this.handleClickNext(this.state.image)}
                                        size="large">
                                        <SkipNextIcon />
                                    </IconButton>
                                </div>
                                <div style={overLayStyleBottomRight}>
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
                </Grid>
                <div>
                    <SimpleExifsDialog
                        open={this.state.exifDialogIsOpen ?? false}
                        onClose={this.handleClickExifsClose}
                        exifDToesToDisplay={exifDToesToDisplay}
                    />
                    <SimpleAlbumDialog
                        open={this.state.albumbDialogIsOpen ?? false}
                        onClose={this.handleClickAlbumClose}
                    />
                    <SimpleTagDialog
                        open={this.state.tagsDialogIsOpen ?? false}
                        onClose={this.handleClickTagsClose}
                    />
                    <SimplePeopleDialog
                        open={this.state.peopleDialogIsOpen ?? false}
                        onClose={this.handleClickPeopleClose}
                    />

                </div>
            </div >
        );
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
                        this.state.exifOfImages?._links,
                        this.state.exifOfImages?._embedded,
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
        deleteImmediatelyImage: deleteImmediatelyImage,
        selectImage: selectImage,
        nextImageToLoad: nextImageToLoad,
        prevImageToLoad: prevImageToLoad,
        updateImage: updateImage,
        deselectImage: deselectImage,
        downloadImmediatelyImage: downloadImmediatelyImage,
        thunkActionForDeselectImage: (x: ApplicationEvent) => {
            const r = dispatchDeselectImageEvent(x);
            return dispatch(r);
        },
        thunkActionForDoImmediaterlyDeleteImage: (x: ApplicationEvent) => {
            const r = dispatchPhotoToDeleteImmediately(x);
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
        thunkActionForDownloadImageImmediately: (x: ApplicationEvent) => {
            const r = dispatchCheckoutImage(x);
            return dispatch(r);
        },


    }
};

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(RightPanel));
