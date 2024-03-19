
import React, { MouseEvent } from 'react';
import { connect } from "react-redux";

import { styled } from '@mui/material/styles';

import { SuggestionsFetchRequestedParams, RenderSuggestionParams, ChangeEvent } from 'react-autosuggest';
// import match from 'autosuggest-highlight/match'
// import parse from 'autosuggest-highlight/parse'
import Paper from '@mui/material/Paper'
import MenuItem from '@mui/material/MenuItem'
import Chip from '@mui/material/Chip';
import Autocomplete, { AutocompleteChangeDetails, AutocompleteChangeReason, createFilterOptions } from '@mui/material/Autocomplete';
import TextField from '@mui/material/TextField';
import match from "autosuggest-highlight/match";
import parse from "autosuggest-highlight/parse";
import KeywordsServiceImpl, { KeywordsService } from '../services/KeywordsServices'
import PersonsServiceImpl, { PersonsService } from "../services/PersonsServices";
import AlbumsServiceImpl, { AlbumsService } from "../services/AlbumsServices";

import {
    ApplicationThunkDispatch,
    ApplicationEvent,
    addKeywords,
    deleteKeywords,
    addPerson,
    deletePerson,
    addAlbum,
    deleteAlbum,
    dispatchAddKeywordEvent,
    dispatchDeleteKeywordEvent,
    dispatchAddPersonEvent,
    dispatchDeletePersonEvent,
    dispatchAddAlbumEvent,
    dispatchDeleteAlbumEvent,
    selectedImageIsLoading
} from '../redux/Actions';
import { ClientApplicationState } from '../redux/State';
import { toArrayOfString, ExchangedImageDTO } from '../model/DataModel';
import { AutocompleteValue } from '@mui/material';
import is from 'date-fns/esm/locale/is/index';

interface ReactAutosuggestRemoteProp {
    id?: number,
    title?: string,
    image?: ExchangedImageDTO,
    elements?: string[],
    addElement?(img: ExchangedImageDTO, keyword: string): ApplicationEvent,
    deleteElement?(img: ExchangedImageDTO, keyword: string): ApplicationEvent,
    thunkActionForAddElement?: (x: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForDeleteElement?: (x: ApplicationEvent, loadingEvent: ApplicationEvent) => Promise<ApplicationEvent>,
    thunkActionForGetElement?: (x: string) => Promise<string>
}

interface optionsType {
    inputValue?: string,
    title?: string
};

interface ReactAutosuggestRemoteStat {
    suggestions: optionsType[],
    value?: optionsType[],
    textFieldInput: string
};

var globalId: number;
const keywordsService: KeywordsService = new KeywordsServiceImpl();
const personsService: PersonsService = new PersonsServiceImpl();
const albumsService: AlbumsService = new AlbumsServiceImpl();



class ReactAutosuggestRemote extends React.Component<ReactAutosuggestRemoteProp, ReactAutosuggestRemoteStat> {

    filter = createFilterOptions<optionsType>();


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
        this.onInputChange = this.onInputChange.bind(this);
    }

    static getDerivedStateFromProps(props: ReactAutosuggestRemoteProp, state: ReactAutosuggestRemoteStat): ReactAutosuggestRemoteStat {
        if (props != null && props.image != null && props.elements != null) {
            return {
                suggestions: state.suggestions,
                value: props.elements.map(
                    (x) => { return { inputValue: x, title: x } }),
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

            </div>
        )
    }

    handleSuggestionsFetchRequested(request: SuggestionsFetchRequestedParams) {
        if (request.value.length >= 3 && this.props.thunkActionForGetElement != null) {
            this.props.thunkActionForGetElement(request.value).then(json => {
                var listOfCountries = toArrayOfString(json);
                listOfCountries.push(request.value)
                this.setState({
                    suggestions: listOfCountries.map(
                        (x) => { return { inputValue: x, title: x } }),
                })
            })
        } else {
            const listOfCountries = [request.value];
            this.setState({
                suggestions: listOfCountries.map(
                    (x) => { return { inputValue: x, title: x } }),
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

    handleAddChip(chip?: string) {
        if (this.props.thunkActionForAddElement != null && this.props.image != null && this.props.addElement != null && chip != null) {
            this.props.thunkActionForAddElement(this.props.addElement(this.props.image, chip));
        }
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
            </MenuItem>
        )

    }


    onChange(event: React.SyntheticEvent,
        value: AutocompleteValue<optionsType[], boolean | undefined, boolean | undefined, boolean | undefined>,
        reason: AutocompleteChangeReason,
        details?: AutocompleteChangeDetails<optionsType>) {
        // if (typeof value === 'optionsType[]') 
        {
            console.log("onChange " + details + ', ' + typeof value);
            if (details != null && details.option != null) {
                if (reason == "removeOption") {
                    this.handleDeleteChip(details.option.inputValue);
                } else {
                    this.handleAddChip(details.option.inputValue);
                }
            }
        }

    }

    onInputChange(event: React.SyntheticEvent, value: string, reason: string) {
        console.log("onInputChange " + value);
    }

    handleDeleteChip(chip?: string) {
        if (this.props.thunkActionForDeleteElement != null &&
            this.props.image != null &&
            this.props.image._links != null &&
            this.props.deleteElement != null && 
            chip != null) {
            this.props.thunkActionForDeleteElement(
                this.props.deleteElement(this.props.image, chip),
                selectedImageIsLoading(this.props.image));
        }

    };


    render() {
        const label = this.props.title ?? '';
        return (

            <Autocomplete
                sx={{ width: 500 }}
                size="small"
                onInputChange={(a, b, c) => this.onInputChange(a, b, c)}
                onChange={(a, b, c, d) => this.onChange(a, b, c, d)}
                fullWidth={true}
                value={this.state.value}
                multiple
                id="tags-outlined"
                options={this.state.suggestions}
                defaultValue={[this.state.suggestions[0]]}
                filterOptions={(options, params) => {
                    const filtered = this.filter(options, params);

                    if (params.inputValue !== '') {
                        filtered.push(
                            {
                                inputValue: params.inputValue,
                                title: `Ajouter "${params.inputValue}"`,
                            }
                        );
                    }

                    return filtered;
                }}
                getOptionLabel={(option) => {
                    return option?.inputValue ?? 'Not def';
                }}
                renderOption={(props, option) => <li {...props}>{option.title}</li>}
                renderInput={(params) => (
                    <TextField
                        {...params}
                        placeholder="Ajouter.."
                    />
                )}
                noOptionsText="A remplir..."
            />



        )
    }
}

const mapStateToProps = (state: ClientApplicationState, previousState: ReactAutosuggestRemoteProp): ReactAutosuggestRemoteProp => {

    if (!state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading && state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        return {
            id: globalId++,
            title: 'Tags',
            image: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image,
            elements: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image?.image?.keywords.flatMap((s) => s),

        };
    }
    return previousState;
};

const mapStateToPropsForPerson = (state: ClientApplicationState, previousState: ReactAutosuggestRemoteProp): ReactAutosuggestRemoteProp => {

    if (!state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading && state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        return {
            id: globalId++,
            title: 'Personnes',
            image: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image,
            elements: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image?.image?.persons.flatMap((s) => s),

        };
    }
    return previousState;
};

const mapStateToPropsForAlbum = (state: ClientApplicationState, previousState: ReactAutosuggestRemoteProp): ReactAutosuggestRemoteProp => {

    if (!state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.isLoading && state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image != null) {
        return {
            id: globalId++,
            title: 'Album',
            image: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image,
            elements: state.reducerImageIsSelectedToBeDisplayed.imageIsSelectedToBeDisplayed.image?.image?.albums.flatMap((s) => s),

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
            const r = dispatchDeleteKeywordEvent(x);
            return dispatch(r);
        },
        thunkActionForGetElement: (x: string) => {
            return keywordsService.getKeywordsLike(x);
        }
    }
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
            const r = dispatchDeletePersonEvent(x);
            return dispatch(r);
        },
        thunkActionForGetElement: (x: string) => {
            return personsService.getPersonsLike(x);
        }
    }
};

const mapDispatchToPropsForAlbum = (dispatch: ApplicationThunkDispatch) => {
    return {
        addElement: addAlbum,
        deleteElement: deleteAlbum,
        thunkActionForAddElement: (x: ApplicationEvent) => {
            const r = dispatchAddAlbumEvent(x);
            return dispatch(r);
        },
        thunkActionForDeleteElement: (x: ApplicationEvent, loadingEvent: ApplicationEvent) => {
            const r = dispatchDeleteAlbumEvent(x);
            return dispatch(r);
        },
        thunkActionForGetElement: (x: string) => {
            return albumsService.getAlbumsLike(x);
        }
    }
};


function fetch() {
    return ReactAutosuggestRemote;
}

const KeywordsElement = connect(mapStateToProps, mapDispatchToProps)(fetch());
const PersonsElement = connect(mapStateToPropsForPerson, mapDispatchToPropsForPerson)(fetch());
const AlbumsElement = connect(mapStateToPropsForAlbum, mapDispatchToPropsForAlbum)(fetch());

export { KeywordsElement, PersonsElement, AlbumsElement }

