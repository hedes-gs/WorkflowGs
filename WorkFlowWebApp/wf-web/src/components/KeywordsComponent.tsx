
import React from 'react';
import Rating from '@mui/material/Rating';
import { connect } from "react-redux";
import Chip from '@mui/material/Chip';
import Paper from '@mui/material/Paper';
import { CSSProperties } from '@mui/styles';

import { ClientApplicationState } from '../redux/State';
import { Metadata } from '../model/DataModel'
import {
    ApplicationThunkDispatch,
    ApplicationEvent,
    loadAllKeywords,
    dispatchLoadAllKeywords,
    loadAllPersons,
    dispatchLoadAllPersons,
    dispatchLoadImagesOfMetadata,
    loadImagesOfMetadata
} from '../redux/Actions';
import withStyles from '@mui/styles/withStyles';
import { Divider } from '@mui/material';


interface MetadataCmpProp {
    title: string;
    Metadata?: Metadata[] | null;
    loadAllMetadata?(): ApplicationEvent;
    loadImagesOfMetadata?(url: string, title: string): ApplicationEvent;
    thunkActionToLoadAllMetadata?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
    thunkActionToLoadImagesOfMetadata?: (x: ApplicationEvent) => Promise<ApplicationEvent>;
}

interface MetadataCmpStat {
};


export const styles = {
    '@global': {
        '.MuiRating-sizeSmall': {
            fontSize: '0.6rem'
        }
    }
}

class MetadataCmpList extends React.Component<MetadataCmpProp, MetadataCmpStat> {


    constructor(props: MetadataCmpProp) {
        super(props);
        this.state = {};
        this.handleChipClick = this.handleChipClick.bind(this)
    }

    handleChipClick(metadata: Metadata) {
        console.log('Search for keyword ' + metadata)
        if (this.props.loadImagesOfMetadata != null &&
            this.props.thunkActionToLoadImagesOfMetadata != null &&
            this.props.loadImagesOfMetadata != null &&
            metadata._links != null &&
            metadata._links._page) {
            this.props.thunkActionToLoadImagesOfMetadata(this.props.loadImagesOfMetadata(
                metadata._links._page.href, "Dernières images pour " + metadata.content))
        }
    }

    componentDidMount() {
        if (this.props.loadAllMetadata != null && this.props.thunkActionToLoadAllMetadata != null) {
            this.props.thunkActionToLoadAllMetadata(this.props.loadAllMetadata())
        }
    }
    render() {
        if (this.props.Metadata != null) {
            const aroundKeywords: CSSProperties = {
                padding: '8px',
                marginTop: '15px',
                marginLeft: '5px',
                marginRight: '5px',
                border: 'solid 1px rgba(255,255,255,0.5)',
                borderRadius: '7px',
                position: 'relative'
            }
            return (
                <React.Fragment>
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
                        }}>{this.props.title}</div>
                        <Paper component="ul" style={{
                            display: 'flex',
                            justifyContent: 'center',
                            flexWrap: 'wrap',
                            listStyle: 'none',
                            maxWidth: '10em',
                            marginBlockEnd: '0'
                        }}>
                            {this.props.Metadata.map((data) => {
                                return (
                                    <li >
                                        <Chip
                                            onClick={(e) => { this.handleChipClick(data) }}
                                            style={{ margin: '5px' }}
                                            label={data.content}
                                        />
                                    </li>
                                );
                            })}
                        </Paper>
                    </div>
                </ React.Fragment >
            );
        }
        else {
            return (<React.Fragment></React.Fragment>);
        }
    }
}

const mapStateToProps = (state: ClientApplicationState, previousState: MetadataCmpProp): MetadataCmpProp => {

    if (state.reducerDisplayKeywords.displayKeywords.keywords.length > 0) {
        return {
            Metadata: state.reducerDisplayKeywords.displayKeywords.keywords,
            title: "Mots-clé"
        };
    }
    return previousState;
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        loadAllMetadata: loadAllKeywords,
        loadImagesOfMetadata: loadImagesOfMetadata,
        thunkActionToLoadImagesOfMetadata: (x: ApplicationEvent) => {
            const r = dispatchLoadImagesOfMetadata(x);
            return dispatch(r);
        },
        thunkActionToLoadAllMetadata: (x: ApplicationEvent) => {
            const r = dispatchLoadAllKeywords(x);
            return dispatch(r);
        }
    }
};

const mapStateToPropsForPerson = (state: ClientApplicationState, previousState: MetadataCmpProp): MetadataCmpProp => {

    if (state.reducerDisplayPersons.displayPersons.persons.length > 0) {
        return {
            Metadata: state.reducerDisplayPersons.displayPersons.persons,
            title: "Personnes"
        };
    }
    return previousState;
};

const mapDispatchToPropsforPerson = (dispatch: ApplicationThunkDispatch) => {
    return {
        loadAllMetadata: loadAllPersons,
        loadImagesOfMetadata: loadImagesOfMetadata,
        thunkActionToLoadImagesOfMetadata: (x: ApplicationEvent) => {
            const r = dispatchLoadImagesOfMetadata(x);
            return dispatch(r);
        },
        thunkActionToLoadAllMetadata: (x: ApplicationEvent) => {
            const r = dispatchLoadAllPersons(x);
            return dispatch(r);
        }
    }
};



function fetch() {
    return MetadataCmpList;
}

const KeywordsComponent = connect(mapStateToProps, mapDispatchToProps)(fetch());
const PersonsComponent = connect(mapStateToPropsForPerson, mapDispatchToPropsforPerson)(fetch());

export { KeywordsComponent, PersonsComponent }
