import React from 'react';
import makeStyles from '@mui/styles/makeStyles';
import createStyles from '@mui/styles/createStyles';
import { string } from 'prop-types';

export type CommonProps = {
	text: string,
	size?: string,
	color?: string,
}

export type CommonState = {

}
const useParagraphTitleStyles = makeStyles({
	paragraphStyle: (props: CommonProps) => (
		{
            fontSize: props.size != null ? props.size : '12px',
            marginTop: '1em',
			color: props.color != null ? props.color : 'rgba(255, 255, 255, 0.7)'
		}),
});

const useParagraphTitleTreeStyles = makeStyles({
	paragraphStyle: (props: CommonProps) => (
		{
            fontSize: props.size != null ? props.size : '12px',
            marginTop: '1em'
		}),
});



export function ParagraphTitle(props: CommonProps) {
	const classes = useParagraphTitleStyles(props);

	return (
		<div className={classes.paragraphStyle}>
			{props.text}
		</div>);
}

export function ParagraphTitleTree(props: CommonProps) {
	const classes = useParagraphTitleTreeStyles(props);

	return (
		<div className={classes.paragraphStyle}>
			{props.text}
		</div>);
}


