import * as React from 'react';
import './App.css';
import Header from './page_elements/header';
import thunk, { ThunkMiddleware } from 'redux-thunk';
import { createLogger } from 'redux-logger';

import { Provider } from "react-redux";
// import store from "./redux/store";
import { createStore, applyMiddleware } from 'redux';

import { createTheme, ThemeProvider, Theme, StyledEngineProvider, adaptV4Theme } from '@mui/material/styles';
import CenterPanel from './page_elements/CenterPanel';
import rootReducer from './redux/Reducers';
import { ApplicationEvent, dispatchLastImages, loadLastImages } from './redux/Actions';
import ApplicationState from './redux/State';
import 'reflect-metadata';
import AdapterDateFns from '@mui/lab/AdapterDateFns';
import LocalizationProvider from '@mui/lab/LocalizationProvider';
// pick a date util library
import MomentUtils from '@date-io/moment';
import Footer from './page_elements/Footer';


declare module '@mui/styles/defaultTheme' {
    // eslint-disable-next-line @typescript-eslint/no-empty-interface
    interface DefaultTheme extends Theme { }
}


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

    const darkTheme = createTheme(adaptV4Theme({

        palette: {
            mode: 'dark',
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
        
            
            MuiGrid: {
                container: {
                    flexWrap: 'nowrap'
                }
            },

        },


    }));


    return (
        <Provider store={store}>

            <StyledEngineProvider injectFirst>
                <ThemeProvider theme={darkTheme}>
                    <div className="App">
                        <LocalizationProvider dateAdapter={AdapterDateFns}>
                            <Header intervallType={'year'} />
                            <CenterPanel />
                            <Footer />
                        </LocalizationProvider>
                    </div>
                </ThemeProvider>
            </StyledEngineProvider>

        </Provider>
    );
}

export default App;
