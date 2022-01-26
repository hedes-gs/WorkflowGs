import React from 'react';
import Paper from '@mui/material/Paper';
import Tabs from '@mui/material/Tabs';
import Tab from '@mui/material/Tab';
import DatesImageBarSeries from './DatesImageBarSeries';
import withStyles from '@mui/styles/withStyles';
import { ParagraphTitle } from '../styles';
import { MinMaxDatesDto } from '../model/DataModel';



export interface FooterProps {
}

export interface FooterState {
    value: any

};


export const styles = {
    '@global': {

        '.MuiTab-root': {
            fontSize: '0.5rem',
            fontWeight: 'bold',
            minHeight: '24px'
        },
        '.MuiTabs-root': {
            minHeight: '1vh'
        }
    }
}
function TabPanel(props: any) {
    const { children, value, index, ...other } = props;

    return (
        <div
            role="tabpanel"
            hidden={value !== index}
            id={`simple-tabpanel-${index}`}
            aria-labelledby={`simple-tab-${index}`}
            {...other}
        >
            {value === index && (
                <div>{children}</div>
            )}
        </div>
    );
}

class Footer extends React.Component<FooterProps, FooterState> {

    constructor(props: FooterProps) {
        super(props);
        this.handleChange = this.handleChange.bind(this);
    }

    componentDidMount() {
        this.setState({ value: 0 });
    }

    handleChange(event: any, newValue: any): void {
        this.setState({ value: newValue });
    }

    render() {
        if (this.state != null) {

            return (
                <div >
                    <Paper square>
                        <Tabs
                            value={this.state.value}
                            onChange={this.handleChange}
                            indicatorColor="primary"
                            textColor="primary"
                        >
                            <Tab label="Dates" />
                            <Tab label="Albums" />
                            <Tab label="Personnes" />
                            <Tab label="Ratings" />
                            <Tab label="Mots-clés" />
                        </Tabs>
                        <TabPanel value={this.state.value} index={0}>
                            <DatesImageBarSeries />
                        </TabPanel>
                    </Paper>
                </div >);
        } else {
            return (<div></div>);
        }
    }
}

export default withStyles(styles)(Footer);
