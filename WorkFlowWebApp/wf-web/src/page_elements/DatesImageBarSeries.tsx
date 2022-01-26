import React from 'react';
import { connect } from "react-redux";
import { ClientApplicationState } from '../redux/State';
import { ApplicationThunkDispatch, dispatchGetAllDatesOfImages, ApplicationEvent, getAllDatesOfImages } from '../redux/Actions';
import withStyles from '@mui/styles/withStyles';
import { FloatProperty, DisplayInside, TableLayoutProperty } from 'csstype';
import Grid from '@mui/material/Grid';
import RefreshIcon from '@mui/icons-material/Refresh';
import DeleteIcon from '@mui/icons-material/Delete';
import CloseIcon from '@mui/icons-material/Close';
import IconButton from '@mui/material/IconButton';

import {
    FlexibleWidthXYPlot,
    XYPlot,
    XAxis,
    VerticalBarSeries,
    VerticalBarSeriesPoint,
    LabelSeries,
    GradientDefs,
    HorizontalGridLines
} from 'react-vis';
import { MinMaxDatesDto, MomentType } from '../model/DataModel';
import { ImageList, ImageListItem, Button } from '@mui/material';


export interface DatesImageBarSeriesProps {
    data?: MinMaxDatesDto[];
    loadDatesOfImages?(url?: string, urlToGetfirstPage?: string): ApplicationEvent;
    thunkActionToGetDatesOfImages?: (x: ApplicationEvent) => Promise<ApplicationEvent>;

}

export interface DatesImageBarSeriesState {
    width: number;
    height: number;
    data: MinMaxDatesDto[];
    minMaxDates?: MinMaxDatesDto;
    totalImages?: number;
    datesPath?: Set<MinMaxDatesDto>;
};


export const styles = {
    '@global': {

        '.rv-xy-plot__series--label-text': {
            fontSize: '0.5rem',
            fontWeight: 'bold'
        },
        '.rv-xy-plot__axis--horizontal': {
            backgroundColor: 'white'
        },
        '.MuiButton-root': {
            fontSize: '0.5rem'
        }

    }
}


class DatesImageBarSeries extends React.Component<DatesImageBarSeriesProps, DatesImageBarSeriesState> {

    constructor(props: DatesImageBarSeriesProps) {
        super(props);
        this.handleClickOnSerie = this.handleClickOnSerie.bind(this);
        this.handleClickOnPreviousDate = this.handleClickOnPreviousDate.bind(this);
        this.handleClickOnReset = this.handleClickOnReset.bind(this);
        this.handleDeleteselectedDate = this.handleDeleteselectedDate.bind(this);
        this.updateWindowDimensions = this.updateWindowDimensions.bind(this);
    }

    componentDidMount() {
        this.updateWindowDimensions();
        window.addEventListener('resize', this.updateWindowDimensions);
    }

    componentWillUnmount() {
        window.removeEventListener('resize', this.updateWindowDimensions);
    }

    updateWindowDimensions() {
        this.setState({ width: window.innerWidth, height: window.innerHeight, datesPath: this.state != null ? this.state.datesPath : new Set<MinMaxDatesDto>() });
    }

    static getDerivedStateFromProps(props: DatesImageBarSeriesProps, state: DatesImageBarSeriesState): DatesImageBarSeriesState {
        const data = props.data != null ? props.data : [];
        const datesPath = state == null ? new Set<MinMaxDatesDto>() : state.datesPath;

        return {
            width: state != null ? state.width : 0,
            height: state != null ? state.height : 0,
            datesPath: datesPath,
            data: data
        }
    }

    isCurrentSerie(c: MinMaxDatesDto, v: string): boolean {
        switch (c.intervallType) {
            case 'year': {
                return v === c.minDate?.format('YYYY') ?? 'unset';
            }
            case 'month': {
                return v === c.minDate?.format('YYYY-MM') ?? 'unset'
            }
            case 'day': {
                return v === c.minDate?.format('YYYY-MM-DD') ?? 'unset'
            }
            default: {
                console.log('  ' + c.minDate?.format('HH:mm')  + ', ' + v);
                return v === c.minDate?.format('HH:mm') ?? 'unset'
            }
        }

    }

    handleClickOnPreviousDate(c: MinMaxDatesDto): void {
        if (this.props.thunkActionToGetDatesOfImages != null && this.props.loadDatesOfImages != null) {
            this.props.thunkActionToGetDatesOfImages(this.props.loadDatesOfImages(
                c._links?._subinterval?.href,
                c._links?._imgs?.href
            ));
        }
    }

    toString(c: MinMaxDatesDto): string {
        switch (c.intervallType) {
            case 'year': {
                return c.minDate?.format('YYYY') ?? 'unset';
            }
            case 'month': {
                return c.minDate?.format('YYYY-MM') ?? 'unset';
            }
            case 'day': {
                return c.minDate?.format('YYYY-MM-DD') ?? 'unset';
            }
            default: {
                return c.minDate?.format('HH:mm:ss') ?? 'unset';
            }
        }

    }

    handleClickOnSerie(x: any): void {
        const v: string = x;
        this.state.data.filter(c =>
            this.isCurrentSerie(c, v)
        ).forEach(c => {
            if (this.props.thunkActionToGetDatesOfImages != null && this.props.loadDatesOfImages != null) {
                this.state.datesPath?.add(c);

                this.props.thunkActionToGetDatesOfImages(this.props.loadDatesOfImages(
                    c._links?._subinterval?.href,
                    c._links?._imgs?.href
                ));
            }
        });
    }

    handleClickOnReset(): void {
        if (this.props.thunkActionToGetDatesOfImages != null && this.props.loadDatesOfImages != null) {
            this.props.thunkActionToGetDatesOfImages(this.props.loadDatesOfImages());
        }
    }

    handleDeleteselectedDate(e: MinMaxDatesDto): void {
        this.state.datesPath?.delete(e);
        this.setState(this.state);
    }

    render() {

        const height = this.state.height * 0.080;
        const width = this.state.width * 0.80;
        const widthLeft = this.state.width * 0.19 + 'px';

        var data: VerticalBarSeriesPoint[] = this.state.data.map(c => {
            var retValue: VerticalBarSeriesPoint;

            switch (c.intervallType) {
                case 'year': {
                    retValue = {
                        x: c.minDate?.format('YYYY') ?? 'unset',
                        y: c.countNumber
                    }
                    break;
                }
                case 'month': {
                    retValue = {
                        x: c.minDate?.format('YYYY-MM') ?? 'unset',
                        y: c.countNumber
                    }
                    break;
                }
                case 'day': {
                    retValue = {
                        x: c.minDate?.format('YYYY-MM-DD') ?? 'unset',
                        y: c.countNumber
                    }
                    break;
                }
                default: {
                    retValue = {
                        x: c.minDate?.format('HH:mm') ?? 'unset',
                        y: c.countNumber
                    }
                }
            }
            return retValue;
        });
        var labelData: any[] = data.map((c) => {
            var retValue: any = {
                x: c.x,
                y: c.y,
                label: c.y
            }
            return retValue;
        });
        const left: FloatProperty = "left";
        const datesPath = this.state.datesPath == null ? new Set<MinMaxDatesDto>() : this.state.datesPath;
        const currentMinMax = this.state.minMaxDates;
        const totalImages = this.state.totalImages;
        const tableStyle = {
            float: left,
            width: widthLeft
        }
        const styles = {

            largeIcon: {
                width: 60,
                height: 60,
            },

        };



        return (
            <Grid container  >
                <Grid item xs={3} style={{ backgroundColor: '#606060', borderTop: 'inset', color: 'black' }}>
                    <div style={{ flexGrow: 1, width: '100%' }}>
                        <Grid container style={{ flexWrap: 'wrap' }} >
                            <IconButton
                                onClick={() => { this.handleClickOnReset() }}
                                style={{ width: 5, height: 10 }}
                                size="large">
                                <RefreshIcon fontSize="small" htmlColor="#202020" style={{ width: 20, height: 20 }} />
                            </IconButton>
                            {
                                Array.from(datesPath.values()).map(e =>
                                (
                                    <div style={{ marginBottom: '2px', marginRight: '2px', backgroundColor: '#222200', paddingRight: 2, minWidth: 0 }}>
                                        <Button variant="contained" color="primary" onClick={() => { this.handleClickOnPreviousDate(e) }}
                                            style={{ paddingLeft: 6,paddingRight: 2, minWidth: 0 }}>
                                            {this.toString(e)}
                                        </Button>
                                        <IconButton
                                            onClick={() => { this.handleDeleteselectedDate(e) }}
                                            style={{ paddingLeft: 4, paddingRight: 6, width: 2, height: 2, fontSize: '12px' }}
                                            size="large">
                                            <CloseIcon fontSize="inherit" />
                                        </IconButton>
                                    </div>
                                )
                                )
                            }
                        </Grid>
                    </div>
                </Grid>
                <Grid item xs={9}>
                    <FlexibleWidthXYPlot style={{ backgroundColor: '#606060', borderTop: 'inset' }}
                        xType="ordinal"
                        height={height}
                    >
                        <GradientDefs>
                            <linearGradient id='CoolGradient' x1='0' x2='0' y1='0' y2='1'>
                                <stop offset='0%' stopColor='white' stopOpacity={1.0} />
                                <stop offset='100%' stopColor='#333333' stopOpacity={1.0} />
                            </linearGradient>
                        </GradientDefs>
                        <HorizontalGridLines height={1} />
                        <XAxis style={{ backgroundColor: 'white', fontSize: '0.8em' }} />
                        <VerticalBarSeries
                            onValueClick={(datapoint: any, event: any) => {
                                this.handleClickOnSerie(datapoint.x);
                            }}

                            fill={'url(#CoolGradient)'} opacity={0.25} barWidth={0.95} data={data} style={{ stroke: '#A0A0A0' }} />
                        <LabelSeries data={labelData} animation={true} labelAnchorX={'middle'} labelAnchorY={'central'} />
                    </FlexibleWidthXYPlot>
                </Grid>
            </Grid >
        );
    }

}

const mapStateToProps = (state: ClientApplicationState): DatesImageBarSeriesProps => {
    if (state.reducerDatesOfImages.datesOfImages != null)
        return {
            data: state.reducerDatesOfImages.datesOfImages.datesOfImages
        };
    return {};
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {

    return {
        loadDatesOfImages: getAllDatesOfImages,
        thunkActionToGetDatesOfImages: (x: ApplicationEvent) => {
            const r = dispatchGetAllDatesOfImages(x);
            return dispatch(r);
        },

    };
};

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(DatesImageBarSeries));
