/*
 * BTree.cc
 *
 *  Created on: Nov 6, 2011
 *	CS222 - Principles of Database Systems
 *		Project 1 - Record Manager Interface (RM)
 *		Coded By: Dexter (#24)
 *
 *
 *	This class supports utility methods to manipulate the B+Tree index objects stored in the Paged Files
 *
 */

#include "btree.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>


using namespace std;

IX_Node::IX_Node()
{
	// Note these are just initial values
	noOfKeys=0;
	parentPtr.pageNum=0;
	parentPtr.slotNum=0;

}

IX_Node::IX_Node(IX_Node *iN)
{
	// Create a replica of the Leaf Node
	noOfKeys=iN->noOfKeys;
	parentPtr.pageNum=iN->parentPtr.pageNum;
	parentPtr.slotNum=iN->parentPtr.slotNum;
	childKeys = iN->childKeys;
	childPtrs = iN->childPtrs;
	cout << "New Node object created in memory." << endl;
}

void IX_Node::print()
{
	return; // Comment this line if you need more information RRAJ
	cout << "NODE ENTRY :-" <<endl;
	cout << "   NoOfKeys:- [" << noOfKeys << "]" << endl;
	cout << "   ParentPtr:- [" << parentPtr.pageNum << "," << parentPtr.slotNum << "]" <<endl;

	vector<RID>::iterator cPtrIter;
	vector<IX_KeyValue>::iterator cKeyIter;
	RID * ridPtr;
	IX_KeyValue * kvPtr;

	cPtrIter = childPtrs.begin();
	cKeyIter = childKeys.begin();
	ridPtr = cPtrIter.operator ->();
	cPtrIter++;
	//cout << "[" << ridPtr->pageNum << "," << ridPtr->slotNum << "]"<< endl;
	cout << "   ChildPtr " << 0 << " :-[" << ridPtr->pageNum << "," << ridPtr->slotNum << "]"<< endl;
	int i=1;
	for (; ((cPtrIter != childPtrs.end()) || (i<=IX_MAX_CHILDREN)); ++cPtrIter)
	{
		kvPtr = cKeyIter.operator ->();
		cout << "   KeyValue " << i << " :-[" << kvPtr->fValue << "|" << kvPtr->iValue << "|" << kvPtr->uData << "]"<< endl;
		ridPtr = cPtrIter.operator ->();
		cout << "   ChildPtr " << i << " :-[" << ridPtr->pageNum << "," << ridPtr->slotNum << "]"<< endl;
		cKeyIter++;
		i++;
	}
	cout << "END of NODE ENTRY"  << endl;
	cout << endl;

}


void IX_Leaf::print()
{
	return; // Comment this line if you need more information RRAJ
	cout << "LEAF ENTRY :-" << endl;
	cout << "   NoOfKeys:- [" << noOfKeys << "]" << endl;
	cout << "   ParentPtr:- [" << parentPtr.pageNum << "," << parentPtr.slotNum << "]" << endl;

	vector<IX_KeyData>::iterator cKDIter;
	IX_KeyData * kdPtr;
	int i=1;

	for (cKDIter = keyDataList.begin(); (cKDIter != keyDataList.end()) || (i<=IX_MAX_CHILDREN); ++cKDIter)
	{
		kdPtr = cKDIter.operator ->();
		cout << "   KeyData " << i << ":- {[" << kdPtr->val.fValue << "|" << kdPtr->val.iValue << "|" << kdPtr->val.uData << "]" << endl;
		cout << "   ChildPtr " << i << ":- [" << kdPtr->rid.pageNum << "," << kdPtr->rid.slotNum << "]}" << endl;
		i++;
	}

	cout << "   PrevPtr:- [" << prevPtr.pageNum << "," << prevPtr.slotNum << "]" << endl;
	cout << "   NextPtr:- [" << nextPtr.pageNum << "," << nextPtr.slotNum << "]" << endl;
	cout << "END of LEAF ENTRY"  << endl;

}

IX_Node::~IX_Node()
{
	childPtrs.clear();
	childKeys.clear();
}

IX_Leaf::IX_Leaf()
{
	// Note these are just initial values
	noOfKeys=0;
	parentPtr.pageNum=0;
	parentPtr.slotNum=0;
	nextPtr.pageNum=0;
	nextPtr.slotNum=0;
	prevPtr.pageNum=0;
	prevPtr.slotNum=0;
}

IX_Leaf::IX_Leaf(IX_Leaf *iL)
{
	// Create a replica of the Leaf Node
	noOfKeys=iL->noOfKeys;
	parentPtr.pageNum=iL->parentPtr.pageNum;
	parentPtr.slotNum=iL->parentPtr.slotNum;
	keyDataList = iL->keyDataList;
	nextPtr.pageNum=iL->nextPtr.pageNum;
	nextPtr.slotNum=iL->nextPtr.slotNum;
	prevPtr.pageNum=iL->prevPtr.pageNum;
	prevPtr.slotNum=iL->prevPtr.slotNum;
	cout << "New leaf object created in memory." << endl;
}

IX_Leaf::~IX_Leaf()
{
	keyDataList.clear();
}



