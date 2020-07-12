import * as React from 'react';
import './App.css';
import Header from './page_elements/header';
import thunk, { ThunkMiddleware } from 'redux-thunk';
import { createLogger } from 'redux-logger';

import { Provider } from "react-redux";
// import store from "./redux/store";
import { createStore, applyMiddleware } from 'redux';

import { createMuiTheme, ThemeProvider } from '@material-ui/core/styles';
import CenterPanel from './page_elements/CenterPanel';
import rootReducer from './redux/reducers';
import { ApplicationEvent, dispatchLastImages, loadLastImages } from './redux/Actions';
import ApplicationState from './redux/State';
import 'reflect-metadata';
import { MuiPickersUtilsProvider } from '@material-ui/pickers';

// pick a date util library
import MomentUtils from '@date-io/moment';

const loggerMiddleWare = createLogger();

const store = createStore(rootReducer,
    applyMiddleware(
        thunk as ThunkMiddleware<ApplicationState, ApplicationEvent>
    )

);



//store.subscribe(() => {
//    console.log(' ' + store.getState());
//});




function App() {

    const darkTheme = createMuiTheme({

        palette: {
            type: 'dark',
            primary: {
                // light: will be calculated from palette.primary.main,
                main: '#222200',
                // dark: will be calculated from palette.primary.main,
                // contrastText: will be calculated to contrast with palette.primary.main
            },

            // Used by `getContrastText()` to maximize the contrast between
            // the background and the text.
            contrastThreshold: 3,
            // Used by the functions below to shift a color's luminance by approximately
            // two indexes within its tonal palette.
            // E.g., shift from Red 500 to Red 300 or Red 700.
        },
        overrides: {
            MuiGridListTileBar: {
                title: {
                    fontFamily: 'arial',
                    fontSize: '10px'
                }
            },
            MuiGrid: {
                container: {
                    flexWrap: 'nowrap'
                }
            },

        },


    });


    return (

        <Provider store={store}>

            <ThemeProvider theme={darkTheme}>
                <div className="App">
                    <MuiPickersUtilsProvider utils={MomentUtils}>
                        <Header min={0} max={0} intervallType={'year'} />

                        <CenterPanel />
                    </MuiPickersUtilsProvider>
                </div>
            </ThemeProvider>

        </Provider>
    );
}

export default App;
