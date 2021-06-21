
import React from 'react';
import Rating from '@material-ui/lab/Rating';
import { connect } from "react-redux";
import { toMap } from '../model/DataModel'
import { ClientApplicationState } from '../redux/State';
import { ApplicationThunkDispatch, ApplicationEvent, updateImage, dispatchLoadRatings, loadingRatings } from '../redux/Actions';
import { withStyles } from '@material-ui/core/styles';
import { Client, Message, IMessage } from '@stomp/stompjs';


interface RatingsProp {
    ratings?: Map<string, number> | null;
    loadingRatings?(): ApplicationEvent;
    thunkActionToLoadAllRatings?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
}

interface RatingsStat {
    ratings?: Map<string, number> | null;
    updateByWebsocket: boolean
};


export const styles = {
    '@global': {
        '.MuiRating-sizeSmall': {
            fontSize: '0.6rem'
        }
    }
}

class RatingsList extends React.Component<RatingsProp, RatingsStat> {

    static getDerivedStateFromProps(props: RatingsProp, state: RatingsStat): RatingsStat {
        if (props != null && props.ratings != null && !state.updateByWebsocket) {
            return {
                ratings: props.ratings,
                updateByWebsocket: false
            }
        } else {
            return state;
        }
    }

    constructor(props: RatingsProp) {
        super(props);
        this.state = { updateByWebsocket: false };
        const client = new Client({
            brokerURL: "ws://localhost/app/websocket",
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
            var subscription = client.subscribe("/topic/ratingsStatus", handler);
        };
        client.onStompError = function (frame) {
            console.log('Broker reported error: ' + frame.headers['message']);
            console.log('Additional details: ' + frame.body);
        };
        //         client.activate();

    }

    handler(msg: IMessage) {
        const obj = JSON.parse(msg.body);

        const elements = toMap(obj);
        this.setState(
            {
                ratings: elements,
                updateByWebsocket: true
            }
        )
    };

    componentDidMount() {
        if (this.props.loadingRatings != null && this.props.thunkActionToLoadAllRatings != null) {
            this.props.thunkActionToLoadAllRatings(this.props.loadingRatings())
        }
    }

    render() {
        if (this.state.ratings != null) {
            return (
                <React.Fragment>
                    {Array.from(this.state.ratings.entries()).map((row: [string, number], index: number) => (
                        <div style={{ margin: '5px', display: 'table', fontSize: '0.5rem', float: 'left', border: 'solid 1px rgba(255, 255, 255, 0.23)', borderRadius: '7px' }}>
                            <Rating readOnly size="small" value={Number(row[0])} /><div style={{ display: 'table-cell', padding: '5px' }} >{row[1]}</div>
                        </div>
                    ))
                    }
                </ React.Fragment >
            );
        }
        else {
            return (<React.Fragment></React.Fragment>);
        }
    }
}

const mapStateToProps = (state: ClientApplicationState, previousState: RatingsProp): RatingsProp => {

    if (state.reducerDisplayRatings.displayedRatings.ratings.size > 0) {
        return {
            ratings: state.reducerDisplayRatings.displayedRatings.ratings
        };
    }
    return previousState;
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        loadingRatings: loadingRatings,
        thunkActionToLoadAllRatings: (x: ApplicationEvent) => {
            const r = dispatchLoadRatings(x);
            return dispatch(r);
        }
    }
};

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(RatingsList));














