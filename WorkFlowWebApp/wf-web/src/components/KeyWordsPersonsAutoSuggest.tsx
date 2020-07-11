
import React, { MouseEvent } from 'react';
import { connect } from "react-redux";


import { SuggestionsFetchRequestedParams, RenderSuggestionParams, ChangeEvent } from 'react-autosuggest';
import ChipInput, { Props } from 'material-ui-chip-input';
// import match from 'autosuggest-highlight/match'
// import parse from 'autosuggest-highlight/parse'
import Paper from '@material-ui/core/Paper'
import MenuItem from '@material-ui/core/MenuItem'
import ElementAutosuggest, { InputProps } from "react-autosuggest";
import * as Autosuggest from "react-autosuggest";

import match from "autosuggest-highlight/match";
import parse from "autosuggest-highlight/parse";
import KeywordsServiceImpl, { KeywordsService } from '../services/KeywordsServices'
import {
    ApplicationThunkDispatch,
    ApplicationEvent,
    addKeywords,
    deleteKeywords,
    dispatchAddKeywordEvent,
    dispatchDeleteKeywordEvent
} from '../redux/Actions';
import { ClientApplicationState } from '../redux/State';
import { toArrayOfString, ImageDto } from '../model/ImageDto';

interface ReactAutosuggestRemoteProp {
    id?: number,
    image?: ImageDto,
    addKeywords?(img: ImageDto, keyword: string): ApplicationEvent,
    deleteKeywords?(img: ImageDto, keyword: string): ApplicationEvent,
    thunkActionForAddKeyword?: (x: ApplicationEvent) => Promise<ApplicationEvent>
    thunkActionForDeleteKeyword?: (x: ApplicationEvent) => Promise<ApplicationEvent>

}

interface ReactAutosuggestRemoteStat {
    suggestions: string[],
    value: string[],
    textFieldInput: string
};

var globalId: number;


class ReactAutosuggestRemote extends React.Component<ReactAutosuggestRemoteProp, ReactAutosuggestRemoteStat> {

    keywordsService: KeywordsService;

    constructor(props: ReactAutosuggestRemoteProp) {
        super(props);
        this.state = {
            suggestions: [],
            value: [],
            textFieldInput: ''
        };
        this.handleChipInputChange = this.handleChipInputChange.bind(this);
        this.renderInput = this.renderInput.bind(this);
        this.handleSuggestionsFetchRequested = this.handleSuggestionsFetchRequested.bind(this);
        this.handletextFieldInputChange = this.handletextFieldInputChange.bind(this);
        this.handleAddChip = this.handleAddChip.bind(this);
        this.handleDeleteChip = this.handleDeleteChip.bind(this);
        this.renderSuggestion = this.renderSuggestion.bind(this);
        this.handleSuggestionsClearRequested = this.handleSuggestionsClearRequested.bind(this);
        this.keywordsService = new KeywordsServiceImpl();
    }

    static getDerivedStateFromProps(props: ReactAutosuggestRemoteProp, state: ReactAutosuggestRemoteStat): ReactAutosuggestRemoteStat {
        if (props != null && props.image != null) {
            return {
                suggestions: state.suggestions,
                value: props.image.keywords.flatMap((s) => s),
                textFieldInput: state.textFieldInput
            }
        } else {
            return state;
        }
    }

    handleChipInputChange(e: any, onChange: any) {
        onChange(e);
    }

    renderInput(inputProps: any) {
        const { value, onChange, ref, ...other } = inputProps
        return (
            <ChipInput
                clearInputValueOnChange
                onUpdateInput={(e) => this.handleChipInputChange(e, inputProps.onChange)}
                value={this.state.value}
                inputRef={inputProps.ref}
                onAdd={inputProps.onAdd}
                onDelete={this.handleDeleteChip}
                fullWidthInput={true}
                style={{ width: '100%', paddingTop: '0.6em' }}

                {...other}
            />
        )
    }

    handleSuggestionsFetchRequested(request: SuggestionsFetchRequestedParams) {
        if (request.value.length >= 3) {
            this.keywordsService.getKeywordsLike(request.value).then(json => {
                var listOfCountries = toArrayOfString(json);
                listOfCountries.push(request.value)
                this.setState({
                    suggestions: listOfCountries,
                })
            })
        } else {
            const listOfCountries = [request.value];
            this.setState({
                suggestions: listOfCountries,
            })

        }
    };

    handleSuggestionsClearRequested() {
        this.setState({
            suggestions: [],
            textFieldInput: ''
        })
    };

    handletextFieldInputChange(event: React.FormEvent<any>, params: ChangeEvent) {
        this.setState({
            textFieldInput: params.newValue
        })
    };

    handleAddChip(chip: string) {
        if (this.props.thunkActionForAddKeyword != null && this.props.image != null) {
            this.props.thunkActionForAddKeyword(addKeywords(this.props.image, chip));
        }
        // the value in state object should be updated when the value is returned by the server
        /*        this.setState({
                    value: this.state.value.concat([chip]),
                    textFieldInput: ''
                })
        */
    }


    renderSuggestionsContainer(options: { containerProps: any; children: any; }) {
        const { containerProps, children } = options

        return (
            <Paper {...containerProps} style={{ zIndex: 100 }}>
                {children}
            </Paper>
        )
    }

    getSuggestionValue(suggestion: string) {
        return suggestion
    }

    renderSuggestion(suggestion: string,
        params: RenderSuggestionParams) {
        const matches = match(suggestion, params.query)
        const parts = parse(suggestion, matches)

        return (
            <MenuItem
                style={{ fontSize: '10px' }}
                selected={params.isHighlighted}
                onMouseDown={(e: MouseEvent) => e.preventDefault()} // prevent the click causing the input to be blurred
            >
                <div>
                    {parts.map((part, index) => {
                        return part.highlight ? (
                            <span key={String(index)} style={{ fontWeight: 500 }}>
                                {part.text}
                            </span>
                        ) : (
                                <span key={String(index)}>
                                    {part.text}
                                </span>
                            )
                    })}
                </div>
            </MenuItem>
        )

    }

    handleDeleteChip(chip: string, index: number) {
        if (this.props.thunkActionForDeleteKeyword != null && this.props.image != null) {
            this.props.thunkActionForDeleteKeyword(deleteKeywords(this.props.image, chip));
        }

    };


    render() {

        return (
            <ElementAutosuggest
                theme={{
                    container: {
                        flexGrow: 1,
                        position: 'relative'
                    },
                    suggestionsContainerOpen: {
                        position: 'absolute',
                        marginTop: 1,
                        marginBottom: 1 * 3,
                        left: 0,
                        right: 0,
                        zIndex: 1
                    },
                    suggestion: {
                        display: 'block'
                    },
                    suggestionsList: {
                        margin: 0,
                        padding: 0,
                        listStyleType: 'none'
                    },
                    textField: {
                        width: '100%'
                    }
                }}
                renderInputComponent={this.renderInput}
                suggestions={this.state.suggestions}
                onSuggestionsFetchRequested={this.handleSuggestionsFetchRequested}
                onSuggestionsClearRequested={this.handleSuggestionsClearRequested}
                renderSuggestionsContainer={this.renderSuggestionsContainer}
                getSuggestionValue={this.getSuggestionValue}
                renderSuggestion={this.renderSuggestion}
                onSuggestionSelected={(e, { suggestionValue }) => { this.handleAddChip(suggestionValue); e.preventDefault() }}
                focusInputOnSuggestionClick
                inputProps={{
                    onChange: this.handletextFieldInputChange,
                    value: this.state.textFieldInput,
                }}
            />
        )
    }
}

const mapStateToProps = (state: ClientApplicationState, previousState: ReactAutosuggestRemoteProp): ReactAutosuggestRemoteProp => {

    if (!state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading && state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        return {
            id: globalId++,
            image: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image,
            addKeywords: addKeywords,

        };
    }
    return previousState;
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        thunkActionForAddKeyword: (x: ApplicationEvent) => {
            const r = dispatchAddKeywordEvent(x);
            return dispatch(r);
        },
        thunkActionForDeleteKeyword: (x: ApplicationEvent) => {
            const r = dispatchDeleteKeywordEvent(x);
            return dispatch(r);
        }

    }
};

export default connect(mapStateToProps, mapDispatchToProps)(ReactAutosuggestRemote);
