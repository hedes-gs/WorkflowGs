
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
import PersonsServiceImpl, { PersonsService } from "../services/PersonsServices";

import {
    ApplicationThunkDispatch,
    ApplicationEvent,
    addKeywords,
    deleteKeywords,
    addPerson,
    deletePerson,
    dispatchAddKeywordEvent,
    dispatchDeleteKeywordEvent,
    dispatchAddPersonEvent,
    dispatchDeletePersonEvent,
    selectedImageIsLoading
} from '../redux/Actions';
import { ClientApplicationState } from '../redux/State';
import { toArrayOfString, ImageDto } from '../model/ImageDto';

interface ReactAutosuggestRemoteProp {
    id?: number,
    image?: ImageDto,
    elements?: string[],
    addElement?(img: ImageDto, keyword: string): ApplicationEvent,
    deleteElement?(img: ImageDto, keyword: string): ApplicationEvent,
    thunkActionForAddElement?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDeleteElement?: (x: ApplicationEvent, loadingEvent: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForGetElement?: (x: string) => Promise<string>
}

interface ReactAutosuggestRemoteStat {
    suggestions: string[],
    value: string[],
    textFieldInput: string
};

var globalId: number;
const keywordsService: KeywordsService = new KeywordsServiceImpl();
const personsService: PersonsService = new PersonsServiceImpl();



class ReactAutosuggestRemote extends React.Component<ReactAutosuggestRemoteProp, ReactAutosuggestRemoteStat> {


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
    }

    static getDerivedStateFromProps(props: ReactAutosuggestRemoteProp, state: ReactAutosuggestRemoteStat): ReactAutosuggestRemoteStat {
        if (props != null && props.image != null && props.elements != null) {
            return {
                suggestions: state.suggestions,
                value: props.elements,
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
            <div style={{ marginTop: '0.8em', marginBottom: '0.2em' }}>
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
            </div>
        )
    }

    handleSuggestionsFetchRequested(request: SuggestionsFetchRequestedParams) {
        if (request.value.length >= 3 && this.props.thunkActionForGetElement != null) {
            this.props.thunkActionForGetElement(request.value).then(json => {
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
        if (this.props.thunkActionForAddElement != null && this.props.image != null && this.props.addElement != null) {
            this.props.thunkActionForAddElement(this.props.addElement(this.props.image, chip));
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
        if (this.props.thunkActionForDeleteElement != null &&
            this.props.image != null &&
            this.props.image._links != null &&
            this.props.image._links.self != null &&
            this.props.image._links._exif != null &&
            this.props.deleteElement != null) {
            this.props.thunkActionForDeleteElement(
                this.props.deleteElement(this.props.image, chip),
                selectedImageIsLoading(this.props.image));
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
            elements: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image.keywords.flatMap((s) => s),

        };
    }
    return previousState;
};

const mapDispatchToProps = (dispatch: ApplicationThunkDispatch) => {
    return {
        addElement: addKeywords,
        deleteElement: deleteKeywords,
        thunkActionForAddElement: (x: ApplicationEvent) => {
            const r = dispatchAddKeywordEvent(x);
            return dispatch(r);
        },
        thunkActionForDeleteElement: (x: ApplicationEvent, loadingEvent: ApplicationEvent) => {
            dispatch(loadingEvent);
            const r = dispatchDeleteKeywordEvent(x);
            return dispatch(r);
        },
        thunkActionForGetElement: (x: string) => {
            return keywordsService.getKeywordsLike(x);
        }
    }
};

const mapStateToPropsForPerson = (state: ClientApplicationState, previousState: ReactAutosuggestRemoteProp): ReactAutosuggestRemoteProp => {

    if (!state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading && state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        return {
            id: globalId++,
            image: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image,
            elements: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image.persons.flatMap((s) => s),

        };
    }
    return previousState;
};

const mapDispatchToPropsForPerson = (dispatch: ApplicationThunkDispatch) => {
    return {
        addElement: addPerson,
        deleteElement: deletePerson,
        thunkActionForAddElement: (x: ApplicationEvent) => {
            const r = dispatchAddPersonEvent(x);
            return dispatch(r);
        },
        thunkActionForDeleteElement: (x: ApplicationEvent, loadingEvent: ApplicationEvent) => {
            dispatch(loadingEvent);
            const r = dispatchDeletePersonEvent(x);
            return dispatch(r);
        },
        thunkActionForGetElement: (x: string) => {
            return personsService.getPersonsLike(x);
        }
    }
};


function fetch() {
    return ReactAutosuggestRemote;
}

const KeywordsElement = connect(mapStateToProps, mapDispatchToProps)(fetch());
const PersonsElement = connect(mapStateToPropsForPerson, mapDispatchToPropsForPerson)(fetch());

export { KeywordsElement, PersonsElement }

