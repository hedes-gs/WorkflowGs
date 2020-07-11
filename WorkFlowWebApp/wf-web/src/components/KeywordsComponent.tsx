
import React from 'react';
import Rating from '@material-ui/lab/Rating';
import { connect } from "react-redux";
import Chip from '@material-ui/core/Chip';
import Paper from '@material-ui/core/Paper';

import { ClientApplicationState } from '../redux/State';
import {
    ApplicationThunkDispatch,
    ApplicationEvent,
    loadAllKeywords,
    dispatchLoadAllKeywords,
    loadImagesOfKeyword,
    dispatchLoadImagesOfKeyword
} from '../redux/Actions';
import { withStyles } from '@material-ui/core/styles';


interface KeywordsCmpProp {
    keywords?: string[] | null;
    loadAllKeywords?(): ApplicationEvent;
    loadImagesOfKeyword?(): ApplicationEvent;
    thunkActionToLoadAllKeywords?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToLoadImagesOfKeyword?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
}

interface KeywordsCmpStat {
};


export const styles = {
    '@global': {
        '.MuiRating-sizeSmall': {
            fontSize: '0.6rem'
        }
    }
}

class KeywordsCmpList extends React.Component<KeywordsCmpProp, KeywordsCmpStat> {


    constructor(props: KeywordsCmpProp) {
        super(props);
        this.state = {};
        this.handleChipClick = this.handleChipClick.bind(this)
    }

    handleChipClick(keyword: string) {
        console.log('Search for keyword ' + keyword)
    }

    componentDidMount() {
        if (this.props.loadAllKeywords != null && this.props.thunkActionToLoadAllKeywords != null) {
            this.props.thunkActionToLoadAllKeywords(this.props.loadAllKeywords())
        }
    }
    render() {
        if (this.props.keywords != null) {
            return (
                <React.Fragment>

                    <Paper component="ul" style={{
                        display: 'flex',
                        justifyContent: 'center',
                        flexWrap: 'wrap',
                        listStyle: 'none',
                        maxWidth: '10em'
                    }}>
                        {this.props.keywords.map((data) => {
                            return (
                                <li key={data}>
                                    <Chip
                                        onClick={(e) => { this.handleChipClick(data) }}
                                        style={{ margin: '5px' }}
                                        label={data}
                                    />
                                </li>
                            );
                        })}
                    </Paper>
                </ React.Fragment >
            );
        }
        else {
            return (<React.Fragment></React.Fragment>);
        }
    }
}

const mapStateToProps = (state: ClientApplicationState, previousState: KeywordsCmpProp): KeywordsCmpProp => {

    if (state.reducerDisplayKeywords.displayKeywords.keywords.length > 0) {
        return {
            keywords: state.reducerDisplayKeywords.displayKeywords.keywords
        };
    }
    return previousState;
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        loadAllKeywords: loadAllKeywords,
        thunkActionToLoadAllKeywords: (x: ApplicationEvent) => {
            const r = dispatchLoadAllKeywords(x);
            return dispatch(r);
        }
    }
};

export default connect(mapStateToProps, mapDispatchToProps)(withStyles(styles)(KeywordsCmpList));














