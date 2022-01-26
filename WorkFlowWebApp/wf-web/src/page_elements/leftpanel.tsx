import 'moment/locale/fr'

import { connect } from "react-redux";

import { loadImagesInterval, ApplicationThunkDispatch,  ApplicationEvent, dispatchLastImages, loadLastImages } from '../redux/Actions';
import React from 'react';
import TreeView from '@mui/lab/TreeView';

import Drawer from '@mui/material/SwipeableDrawer';
import LimitDatesServiceImpl, { LimitDatesService } from '../services/LimitDates';
import { MinMaxDatesDto } from '../model/DataModel'
import TreeLimitDates from '../components/TreeLimitDates'
import { KeywordsComponent, PersonsComponent } from '../components/KeywordsComponent'

import { ParagraphTitle } from '../styles';
import { DisplayInside } from 'csstype';
import { ClientApplicationState } from '../redux/State';
import { CSSProperties } from '@mui/styles';
import MomentTimeZone, { Moment } from 'moment-timezone';


interface LeftPanelProps {
    isOpenedStateId?: number | 0;
    drawerIsOpen?: boolean | null;
    onClose?(): void | null;
    loadImagesInterval?(intervallType: string, title: string, min?: number, max?: number): ApplicationEvent;
    thunkAction?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
}
interface LeftPanelState {
    isOpenedStateId?: number,
    drawerIsOpen?: boolean | null;
    minMaxDates?: MinMaxDatesDto | null;
};

var globalId: number = 0;

export class LeftPanelClass extends React.Component<LeftPanelProps, LeftPanelState> {

    protected limitDatesService: LimitDatesService;
    protected refToTreeLimitDatesElement: React.RefObject<TreeLimitDates>;

    constructor(props: LeftPanelProps) {
        super(props);
        this.state = { drawerIsOpen: false, minMaxDates: null, isOpenedStateId: 0 };
        this.handleOnClose = this.handleOnClose.bind(this);
        this.handleOnOpen = this.handleOnOpen.bind(this);
        this.limitDatesService = new LimitDatesServiceImpl();
        this.refToTreeLimitDatesElement = React.createRef();
        this.handleClickOnFirstDateButton = this.handleClickOnFirstDateButton.bind(this);
        this.handleIntervalSelected = this.handleIntervalSelected.bind(this);
    }

    handleIntervalSelected(intervallType: string, min?: number, max?: number) {
        if (this.props.loadImagesInterval != null && this.props.thunkAction != null) {
            this.setState({
                drawerIsOpen: false,
                minMaxDates: this.state.minMaxDates
            });
            this.props.thunkAction(this.props.loadImagesInterval(
                intervallType,
                min != null ? ' Photos du ' + MomentTimeZone(min).format('ddd DD MMMM YYYY, HH:mm:ss') : '',
                min,
                max))
        }
    }

    openDrawer() {
    }

    handleOnOpen() {
        this.limitDatesService.getLimits((lim?: MinMaxDatesDto) => {
            this.setState({
                drawerIsOpen: true,
                minMaxDates: lim
            });
        })
    }

    handleOnClose() {
        this.setState({
            drawerIsOpen: false,
            minMaxDates: this.state.minMaxDates
        });
    }


    handleClickOnFirstDateButton() {
        if (this.refToTreeLimitDatesElement.current != null) {
            this.refToTreeLimitDatesElement.current.reload();
        }
    }

    static getDerivedStateFromProps(props: LeftPanelProps, state: LeftPanelState): LeftPanelState {
        if (props.isOpenedStateId != state.isOpenedStateId) {
            return {
                isOpenedStateId: props.isOpenedStateId,
                drawerIsOpen: props.drawerIsOpen,
                minMaxDates: state.minMaxDates
            }
        } else {
            return state;
        }
    }


    render() {
        const drawerIsopened = this.state.drawerIsOpen != null ? this.state.drawerIsOpen : false;
        const currentMinMax = this.state.minMaxDates;
        const dispalyInside: DisplayInside = "table";

        const tableStyle = {
            display: dispalyInside,
            width: '100%',
        }
        if (currentMinMax != null) {
            const aroundKeywords: CSSProperties = {
                padding: '8px',
                marginTop: '15px',
                marginLeft: '5px',
                marginRight: '5px',
                border: 'solid 1px rgba(255,255,255,0.5)',
                borderRadius: '7px',
                position: 'relative'
            }
            const min = currentMinMax.minDate;
            const max = currentMinMax.maxDate;
            return (
                <Drawer
                    anchor='left'
                    open={drawerIsopened}
                    onClose={this.handleOnClose}
                    onOpen={this.handleOnOpen}
                >
                    <div style={tableStyle} >
                        <ParagraphTitle text={'Recherche de photos... '} size='16px' />
                        <hr />
                    </div>
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
                        }}>Par date.. </div>
                        <ParagraphTitle text={'Annee ' + (min != null ? min.format('yyyy') : null)} size='14px' />
                        <TreeView>
                            <TreeLimitDates
                                parentNodeType="init"
                                min={min?.valueOf() ?? 0}
                                max={max?.valueOf() ?? 0}
                                ref={this.refToTreeLimitDatesElement}
                                handleIntervalSelected={this.handleIntervalSelected} />
                        </TreeView>
                        <ParagraphTitle text={'Annee ' + (max != null ? max.format('yyyy') : null)} size='14px' />
                    </div>
                    <KeywordsComponent title="Mots-clé" />
                    <PersonsComponent title="Personnes" />
                </Drawer>
            );
        } else {
            return (<div></div>);
        }

    }
}

const mapStateToProps = (state: ClientApplicationState, ownProps: LeftPanelProps): LeftPanelProps => {

    if (state.reducerImagesList.imagesLoaded.state == 'LOADED') {
        return {
            drawerIsOpen: false
        };
    }
    return {
        drawerIsOpen: false
    };
};

const initialProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        loadImagesInterval: loadImagesInterval,
        isOpened: false,
    }
};

export default connect(mapStateToProps, initialProps, null, { forwardRef: true })(LeftPanelClass);


























