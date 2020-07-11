import React from 'react';
import { makeStyles, createStyles } from '@material-ui/core/styles';
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
			color: props.color != null ? props.color : 'rgba(255, 255, 255, 0.7)'
		}),
});

const useParagraphTitleTreeStyles = makeStyles({
	paragraphStyle: (props: CommonProps) => (
		{
			fontSize: props.size != null ? props.size : '12px'
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


