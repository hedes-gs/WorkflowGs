import React from 'react';
import { Client, Message, IMessage } from '@stomp/stompjs';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import Paper, { PaperProps } from '@material-ui/core/Paper';
import CloudUploadIcon from '@material-ui/icons/CloudUpload';
import TextField from '@material-ui/core/TextField';
import Grid from '@material-ui/core/Grid';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { CSSProperties } from '@material-ui/styles';
import { withStyles } from '@material-ui/core/styles';
import Checkbox, { CheckboxProps } from '@material-ui/core/Checkbox';
import WfEventsServicesImpl, { WfEventsServices } from '../services/EventsServices'
import { ImportEvent } from '../model/WfEvents'
import { toComponentEvent, ComponentEvent } from '../model/ImageDto'
import { Badge } from '@material-ui/core';


export interface ComponentStatusProps {
}

export interface ComponentStatusState {
    dialogIsOpened: boolean;
    message: Map<string, ComponentEvent>;
};
const StyledTableRow = withStyles((theme) => ({
    root: {
        '&:nth-of-type(odd)': {
            backgroundColor: theme.palette.action.hover,
            fontSize: "0.5rem",
            padding : "unset"
        },
        '&:nth-of-type(even)': {
            fontSize: "0.5rem",
            padding : "unset"
        }
    },
}))(TableRow);


const StyledTableCell = withStyles((theme) => ({
    root: {
            fontSize: "0.5rem",
            padding : "unset"
        }
}))(TableCell);

const StyledTableBody = withStyles((theme) => ({
    root: {
        padding : "unset"
    }
}))(TableBody);




export default class ComponentStatus extends React.Component<ComponentStatusProps, ComponentStatusState> {

    
    private importName: string;
    private keywords: string;
    private album: string;
    private scanFolder: string;
    private scans: Set<string>
    private importEvents: Map<String, ImportEvent>;
    private wfEventsServices: WfEventsServices = new WfEventsServicesImpl();

    constructor(props: ComponentStatusProps) {
        super(props);
        var SortedMap = require("collections/sorted-map");
        this.state = {
            message: new SortedMap(),
            dialogIsOpened: false };
        this.importName = '';
        this.album = '';
        this.keywords = '';
        this.scanFolder = '';
        this.importEvents = new Map();
        const client = new Client({
            brokerURL: "ws://192.168.1.128:8080/app/websocket",
            connectHeaders: {
            },
            debug: function (str: string) {
                console.log(str);
            },
            reconnectDelay: 5000,
            heartbeatIncoming: 4000,
            heartbeatOutgoing: 4000
        });
        const handler = this.handler.bind(this);
        client.onConnect = function (frame) {
            console.log('Stomp is connected');
            var subscription = client.subscribe("/topic/componentStatus", handler);
        };

        client.onStompError = function (frame) {
            // Will be invoked in case of error encountered at Broker
            // Bad login/passcode typically will cause an error
            // Complaint brokers will set `message` header with a brief message. Body may contain details.
            // Compliant brokers will terminate the connection after any error
            console.log('Broker reported error: ' + frame.headers['message']);
            console.log('Additional details: ' + frame.body);
        };
        this.scans = new Set();
        client.activate();
        this.handleClickOpen = this.handleClickOpen.bind(this);
        this.handleClose = this.handleClose.bind(this);
        this.handleAddScanToStart = this.handleAddScanToStart.bind(this);
        this.handleSetKeyWords = this.handleSetKeyWords.bind(this);
        this.handleSetImportName = this.handleSetImportName.bind(this);
        this.handleSetAlbum = this.handleSetAlbum.bind(this);
        this.handleSartScan = this.handleSartScan.bind(this);
    }

    handler(msg: IMessage) {
        const message = this.state.message;
        const componentEvent = toComponentEvent(JSON.parse(msg.body));
        if (componentEvent.componentName != null) {
            message.set(componentEvent.componentName, componentEvent);
            this.setState(
                {
                    message: message,
                    dialogIsOpened: this.state.dialogIsOpened
                }
            )
        }
    };

    handleClickOpen() {
        this.setState(
            {
                message: this.state.message,
                dialogIsOpened: true
            }
        )
    };

    handleClose() {
        this.setState(
            {
                message: this.state.message,
                dialogIsOpened: false
            }
        )
    };

    handleAddScanToStart(key: string, folder: string) {
        const event: ImportEvent = {
            dataId: '',
            keyWords: this.keywords.split(','),
            scanners: [key],
            scanFolder: folder,
            album: this.album,
            importDate: 0,
            importName: this.importName
        }
        this.importEvents.set(key, event);
    }

    handleSetImportName(importName: string) {
        this.importName = importName;
    }
    handleSetKeyWords(keywords: string) {
        this.keywords = keywords;
    }
    handleSetAlbum(album: string) {
        this.album = album;
    }

    handleSartScan() {
        Array.from(this.importEvents.values()).forEach((v: ImportEvent) => { this.wfEventsServices.startScan(v); });

    }

    render() {
        const tableStyle = {
        }
        const overLayStyle: CSSProperties = {
            backgroundColor: 'rgba(0,0,0,0.2)',
            float: 'right',
            position: 'absolute',
            top: '65px',
            left: '10px',
            width: '300px',
            height: '150px',
            fontSize: '10px',
            display: 'table'
        }
        const open = this.state.dialogIsOpened;
        const message = this.state.message;
        return (
            <div>
                <Badge color="primary" style={{ display: 'table-cell', margin: '5px', padding: '5px' }} >
                <Button onClick={this.handleClickOpen}>
                    <CloudUploadIcon />
                </Button>
                </Badge>
                <Dialog
                    open={open}
                    onClose={this.handleClose}
                >

                    <DialogTitle >{"Lancer un import de photos"}</DialogTitle>
                    <DialogContent>
                        <div style={{ display: 'table' }}>
                            <div>
                                <TextField
                                    defaultValue="<import name>"
                                    label="Intitulé de l'import"
                                    onChange={(e) => this.handleSetImportName(e.target.value)}
                                />
                                <TextField
                                    defaultValue="<key word>"
                                    label="Mots clés"
                                    onChange={(e) => this.handleSetKeyWords(e.target.value)}
                                />
                                <TextField
                                    defaultValue="Album"
                                    label="Album"
                                    onChange={(e) => this.handleSetAlbum(e.target.value)}
                                />
                            </div>
                        </div>
                        <Grid container spacing={3} direction="column">

                            <Grid item zeroMinWidth >
                                <TableContainer style={tableStyle} component={Paper}>
                                    <Table stickyHeader size="small" >
                                        <TableHead>
                                            <StyledTableRow>
                                                <StyledTableCell align="right">Scanner</StyledTableCell>
                                                <StyledTableCell align="right">Démarrer</StyledTableCell>
                                            </StyledTableRow>
                                        </TableHead>
                                        <StyledTableBody>
                                            {Array.from(message.entries()).map(([key, value]) => {
                                                return (
                                                    <StyledTableRow key={key}>
                                                        <StyledTableCell component="th" scope="row">
                                                            {key}
                                                        </StyledTableCell>
                                                        <StyledTableCell align="right">
                                                            <Table>
                                                                <StyledTableBody>
                                                                    {value.scannedFolder != null &&
                                                                        value.scannedFolder.map((folder) => {
                                                                            return (

                                                                                <TableRow key={key + '-' + folder}>
                                                                                    <StyledTableCell component="th" scope="row">
                                                                                        {folder}
                                                                                    </StyledTableCell>

                                                                                    <StyledTableCell align="right">
                                                                                        <Checkbox onClick={(e) => this.handleAddScanToStart(key, folder)} />
                                                                                    </StyledTableCell>
                                                                                </TableRow>

                                                                            )
                                                                        })
                                                                    }
                                                                </StyledTableBody>
                                                            </Table>
                                                        </StyledTableCell>
                                                    </StyledTableRow>
                                                )
                                            }
                                            )
                                            }
                                        </StyledTableBody>
                                    </Table>
                                </TableContainer>
                            </Grid>
                        </Grid>

                    </DialogContent>

                    <DialogActions>
                        <Button onClick={this.handleSartScan}>
                            Demarrer
                        </Button>
                        <Button onClick={this.handleClose} color="primary" autoFocus>
                            Annuler
                        </Button>
                    </DialogActions>

                </Dialog>
            </div>)
    }
}

