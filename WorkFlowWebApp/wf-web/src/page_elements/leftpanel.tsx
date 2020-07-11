import 'moment/locale/fr'

import { connect } from "react-redux";

import { loadImagesInterval, ApplicationThunkDispatch, dispatchNewSelectedDateImagesInterval, ApplicationEvent, dispatchLastImages, loadLastImages } from '../redux/Actions';
import React from 'react';
import TreeView from '@material-ui/lab/TreeView';

import Drawer from '@material-ui/core/SwipeableDrawer';
import LimitDatesServiceImpl, { LimitDatesService } from '../services/LimitDates';
import { MinMaxDatesDto } from '../model/MinMaxDatesDto'
import TreeLimitDates from '../components/TreeLimitDates'
import KeywordsComponent from '../components/KeywordsComponent'

import { ParagraphTitle } from '../styles';
import { DisplayInside } from 'csstype';
import { ClientApplicationState } from '../redux/State';
import { Divider } from '@material-ui/core';


interface LeftPanelProps {
    isOpenedStateId?: number | 0;
    drawerIsOpen?: boolean | null;
    onClose?(): void | null;
    loadImagesInterval?(min: number, max: number, intervallType: string): ApplicationEvent;
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

    handleIntervalSelected(min: number, max: number, intervallType: string) {
        if (this.props.loadImagesInterval != null && this.props.thunkAction != null) {
            this.props.thunkAction(this.props.loadImagesInterval(min, max, intervallType))
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
                    <ParagraphTitle text={'Annee ' + (min != null ? min.format('yyyy') : null)} size='14px' />
                    <TreeView>
                        <TreeLimitDates
                            parentNodeType="year"
                            min={min.valueOf()}
                            max={max.valueOf()}
                            ref={this.refToTreeLimitDatesElement}
                            handleIntervalSelected={this.handleIntervalSelected} />
                    </TreeView>
                    <ParagraphTitle text={'Annee ' + (max != null ? max.format('yyyy') : null)} size='14px' />
                    <Divider />
                    <KeywordsComponent />
                </Drawer>
            );
        } else {
            return (<div></div>);
        }

    }
}

const mapStateToProps = (state: ClientApplicationState, ownProps: LeftPanelProps): LeftPanelProps => {
    switch (state.reducerImagesList.imagesLoaded.state) {
        case 'LOADED': {
            return {
                isOpenedStateId: globalId++,
                drawerIsOpen: false
            };
        }
    }
    return ownProps;
};

const initialProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        loadImagesInterval: loadImagesInterval,
        isOpened: false,
        thunkAction: (x: ApplicationEvent) => {
            const r = dispatchNewSelectedDateImagesInterval(x);
            return dispatch(r);
        }

    }
};

export default connect(mapStateToProps, initialProps, null, { forwardRef: true })(LeftPanelClass);


























