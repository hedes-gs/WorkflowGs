
export interface TreeNode{
	name: string;
	id: string ;
	children: Array<TreeNode> ;
};

export interface TreeModelListener{
	onUpdate() : void;
}

export interface TreeModel {
	root: TreeNode;
	loadChildFrom(id: string): void;
	addModelListener(listener: TreeModelListener ) : void ;
};