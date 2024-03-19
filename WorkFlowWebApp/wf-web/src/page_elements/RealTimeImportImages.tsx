import React from 'react';
import { Client, Message, IMessage } from '@stomp/stompjs';
import { ClientApplicationState } from '../redux/State';
import { ExchangedImageDTO, ImageLinks, PageOfExchangedImageDTO, ImageKeyDto, toSingleExchangedImageDTO } from '../model/DataModel';
import { ImageList, ImageListItem, ImageListItemBar, IconButton } from '@mui/material';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import { connect } from "react-redux";
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import { CSSProperties } from '@mui/styles';
import withStyles from '@mui/styles/withStyles';
import Checkbox, { CheckboxProps } from '@mui/material/Checkbox';
import WfEventsServicesImpl, { WfEventsServices } from '../services/EventsServices'
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
import { Badge } from '@mui/material';
import InfoIcon from '@mui/icons-material/Info';
import TrashIcon from '@mui/icons-material/Delete';
import CloudDownloadIcon from '@mui/icons-material/CloudDownload';

export interface RealtimeImportImagesProps {
    thunkActionForDeleteImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForSelectImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDownloadImage?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    deleteImage?(img?: ImageKeyDto | null): ApplicationEvent;
    selectImage?(url?: string, exifUrl?: string): ApplicationEvent;
    downloadImage?(img?: ExchangedImageDTO): ApplicationEvent;

}

export interface RealtimeImportImagesState {
    imgs: ExchangedImageDTO[]
};
const StyledTableRow = withStyles((theme) => ({
    root: {
        '&:nth-of-type(odd)': {
            backgroundColor: theme.palette.action.hover,
            fontSize: "0.5rem",
            padding: "unset"
        },
        '&:nth-of-type(even)': {
            fontSize: "0.5rem",
            padding: "unset"
        }
    },
}))(TableRow);


const StyledTableCell = withStyles((theme) => ({
    root: {
        fontSize: "0.5rem",
        padding: "unset"
    }
}))(TableCell);

const StyledTableBody = withStyles((theme) => ({
    root: {
        padding: "unset"
    }
}))(TableBody);


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

class RealtimeImportImages extends React.Component<RealtimeImportImagesProps, RealtimeImportImagesState> {


    private nbOfColumnsInFullSize: number;
    private socket: WebSocket;

    constructor(props: RealtimeImportImagesProps) {
        super(props);

        var SortedMap = require("collections/sorted-map");
        this.state = {
            imgs: new Array(0)
        };
        const handler = this.handler.bind(this);
        const handlerSocketEvent = this.handlerSocketEvent.bind(this);

        this.socket = new WebSocket("ws://192.168.1.128/ws/fullyImagesProcessed");
        this.socket.onmessage = (event => {
            handlerSocketEvent(event);
        });
        this.socket.onopen = (event => console.log('... event ' + event))


        const client = new Client({
            brokerURL: "ws://192.168.1.128/app/websocket",
            connectHeaders: {
            },
            debug: function (str: string) {
                console.log(str);
            },
            reconnectDelay: 5000,
            heartbeatIncoming: 4000,
            heartbeatOutgoing: 4000
        });

        client.onConnect = function (frame) {
            console.log('Stomp is connected in realtime...');
            var subscription = client.subscribe("/topic/realtimeImportImages", handler);
        };

        client.onStompError = function (frame) {
            // Will be invoked in case of error encountered at Broker
            // Bad login/passcode typically will cause an error
            // Complaint brokers will set `message` header with a brief message. Body may contain details.
            // Compliant brokers will terminate the connection after any error
            console.log('Broker reported error: ' + frame.headers['message']);
            console.log('Additional details: ' + frame.body);
        };
        // client.activate();
        this.nbOfColumnsInFullSize = 5;
    }
    handleClickInfo(img?: ExchangedImageDTO) {
        if ( this.props.selectImage != null && this.props.thunkActionForSelectImage !=null) {
           this.props.thunkActionForSelectImage(this.props.selectImage(img?._links?.self?.href, img?._links?._exif?.href));
        }
    }
    handleClickDelete(img?: ExchangedImageDTO) {
        if (this.props.thunkActionForDeleteImage != null && this.props.deleteImage != null) {
            this.props?.thunkActionForDeleteImage(this.props?.deleteImage(img?.image?.data));
        }
    }

    handleClickDownload(img?: ExchangedImageDTO) {
        if (this.props.thunkActionForDownloadImage != null && this.props.downloadImage != null) {
            this.props.thunkActionForDownloadImage(this.props.downloadImage(img));
        }

    }

    handler(msg: IMessage) {
        const imgs = this.state.imgs;
        const receivedImage = toSingleExchangedImageDTO(JSON.parse(msg.body));
        if (imgs.length > 25) {
            imgs.shift()
        }
        imgs.push(receivedImage);
        this.setState(
            {
                imgs: imgs,
            }
        )
    };

    handlerSocketEvent(msg: MessageEvent) {
        const imgs = this.state.imgs;
        const receivedImage = toSingleExchangedImageDTO(JSON.parse(msg.data));
        if (imgs.length > 25) {
            imgs.shift()
        }
        imgs.push(receivedImage);
        this.setState(
            {
                imgs: imgs,
            }
        )
    };


    getWidth(cellHeight: number, img?: ExchangedImageDTO): number {
        let image = img?.image; 
        let retValue = 0 ;
        if ( image ) {
        if (image?.orientation == 8) {
            retValue = image?.thumbnailHeight * (cellHeight / image?.thumbnailWidth);
        }else{
            retValue =  image?.thumbnailWidth * (cellHeight / image?.thumbnailHeight);
        }
    }
        return retValue;
    }

    getHeight(cellHeight: number, img: ExchangedImageDTO ): number {
        return cellHeight;
    }


    getImgRef(imgLinks?: ImageLinks | null): string {
        return imgLinks != null ? (imgLinks._img != null ? imgLinks._img.href : '') : '';
    }

    render() {
        if (this.state.imgs != null) {
            const ExchangedImageDTOes = this.state.imgs;
            const iconStyle = {
                transform: "scale(0.5)"
            };
            var imageContent;


            return (
                <ImageList style={{ backgroundColor: '#000000' }}>
                    {ExchangedImageDTOes.filter(img => img != null && img.image != null &&  img.image?.data?.version == 1).map((img) => (
                        <ImageListItem cols={1} >
                            <img src={this.getImgRef(img?._links)} width={this.getWidth(200, img)} height={this.getHeight(200, img)} />
                            <ImageListItemBar
                                classes={{
                                    titleWrap: 'Mon-MuiGridListTileBar-titleWrap', // class name, e.g. `classes-nesting-root-x`
                                }}
                                title={
                                    <div style={{ display: 'table' }}>
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
            );
        }
    }
}

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

const mapStateToProps = (state: ClientApplicationState): RealtimeImportImagesProps => {
    return {
    };
}

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(RealtimeImportImages));


