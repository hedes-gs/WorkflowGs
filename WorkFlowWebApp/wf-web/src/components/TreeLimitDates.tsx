
import React from 'react';
import PhotoAlbumIcon from '@material-ui/icons/PhotoAlbum';
import { MinMaxDatesDto } from '../model/MinMaxDatesDto';
import LimitDatesServiceImpl, { LimitDatesService } from '../services/LimitDates';
import MomentTimeZone, { Moment } from 'moment-timezone';
import TreeItem, { TreeItemProps } from '@material-ui/lab/TreeItem';
import { ParagraphTitleTree } from '../styles';

interface TreeLimitDatesProp {
	parentNodeType: string;
	handleIntervalSelected(min: number, max: number, intervallType: string): void;
	min: number;
	max: number;
}

interface TreeLimitStat {
	nodeType: string;
	min?: Moment | null;
	max?: Moment | null;
	nodes?: MinMaxDatesDto[] | null;
};


class TreeLimitDates extends React.Component<TreeLimitDatesProp, TreeLimitStat> {

	protected dateNodeFormat: string;
	protected limitDatesService: LimitDatesService;
	protected refToTreeLimitDatesElement: React.RefObject<TreeLimitDates>;

	constructor(props: TreeLimitDatesProp) {
		super(props);
		var nodeType: string = '';
		this.limitDatesService = new LimitDatesServiceImpl();
		this.reload = this.reload.bind(this);
		this.refToTreeLimitDatesElement = React.createRef();
		this.handleClickOnIconTree = this.handleClickOnIconTree.bind(this);
		this.handleClickOnLabel = this.handleClickOnLabel.bind(this);
		this.dateNodeFormat = 'YYYY';
		switch (props.parentNodeType) {
			case 'year': nodeType = 'month'; this.dateNodeFormat = 'MMMM'; break;
			case 'month': nodeType = 'day'; this.dateNodeFormat = 'ddd DD'; break;
			case 'day': nodeType = 'hour'; this.dateNodeFormat = 'HH:00'; break;
			case 'hour': nodeType = 'minute'; this.dateNodeFormat = 'HH:mm:00'; break;
			case 'minute': nodeType = 'second'; this.dateNodeFormat = 'HH:mm:ss'; break;
			case 'second': nodeType = 'final'; this.dateNodeFormat = 'HH:mm:ss'; break;
		}

		this.state = { nodeType: nodeType, min: MomentTimeZone(props.min), max: MomentTimeZone(props.max) };
	}

	handleClickOnIconTree() {
		if (this.refToTreeLimitDatesElement.current != null) {
			this.refToTreeLimitDatesElement.current.reload();
		}
	}

	handleClickOnLabel(min: number, max: number) {
		var nodeType: string = '';
		switch (this.state.nodeType) {
			case 'year': nodeType = 'month';break;
			case 'month': nodeType = 'day'; break;
			case 'day': nodeType = 'hour'; break;
			case 'hour': nodeType = 'minute'; break;
			case 'minute': nodeType = 'second'; break;
			case 'second': nodeType = 'second'; break;
			case 'final': nodeType = 'second'; break;

		}
		this.props.handleIntervalSelected(min, max,nodeType);
	}

	reload() {
		if (this.state.min != null && this.state.max != null && this.state.nodeType != 'final') {
			this.limitDatesService.getLimitsByDate(
				this.state.min,
				this.state.max,
				this.state.nodeType, (lim?: MinMaxDatesDto[] | null) => {
					if (lim != null) {
						this.setState({
							nodeType: this.state.nodeType,
							min: this.state.min,
							max: this.state.max,
							nodes: lim
						});
					}
				});
		}
	}

	componentDidMount() {
		this.reload();
	}


	renderTree(node: MinMaxDatesDto) {
		const min = node.minDate.valueOf();
		const max = node.maxDate.valueOf();
		const id = min.toString() + ' - '
		max.toString();

		return (
			<TreeItem key={id != null ? id : 'unset'} nodeId={id != null ? id : 'unset'}
				onIconClick={this.handleClickOnLabel.bind(this, min, max)}
				onLabelClick={this.handleClickOnIconTree}
				icon={<PhotoAlbumIcon />}
				label={
					<ParagraphTitleTree text={node.minDate.format(this.dateNodeFormat)} />
				}>
				<TreeLimitDates
					handleIntervalSelected={this.props.handleIntervalSelected}
					min={min}
					max={max}
					ref={this.refToTreeLimitDatesElement}
					parentNodeType={this.state.nodeType} />
			</TreeItem >
		);
	}


	render() {
		const nodes = this.state.nodes;
		return (
			<div >
				{nodes != null ? nodes.map((node) => this.renderTree(node)) : ''}
			</ div>
		);

	}
}

export default TreeLimitDates;
