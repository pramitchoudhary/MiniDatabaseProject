/*
 * BTree.h
 *
 *  Created on: Nov 6, 2011
 *	CS222 - Principles of Database Systems
 *		Project 1 - Record Manager Interface (RM)
 *		Coded By: Dexter (#24)
 */

#include "../rm/rm.h"

#ifndef IXBTREE_H_
#define IXBTREE_H_

using namespace std;
#define IX_MAX_CHILDREN	4 // Rather than use the term 'order' which is inconsistently defined, use the term MAX_Children in all calculations
#define IX_MIN_CHILDREN	2 // Rather than use the term 'order' which is inconsistently defined, use the term MIN_Children in all calculations
#define IX_NODE_PAGES 200 // Number of pages reserved for non-leaf Node records.
#define IX_NON_LEAF 1 // Value indicating that the artifact is a non-leaf
#define IX_LEAF_NODE 2 // Value indicating that the artifact is a leaf node
#define IX_NOVALUE 111111111// Same as RM_NOVALUE // This value will be populated to indicate that no key/pointer value is populated Max Value for 32-bit unsigned is:  4,294,967,295
#define IX_HIFLOAT 9999999999.99 // This value will be populated to indicate that no floating point key value is populated
#define IX_TINY 0.001 // This value will be used in float value comparisons

#define IX_NODE_SIZE (20+IX_MAX_CHILDREN*12)
#define IX_LEAF_SIZE (12+IX_MAX_CHILDREN*12+16)


// These are the Return Codes thrown by various functions implemented.
// Each function will be allotted a range of return code values so that it becomes easy for maintenance.
// RC for Create Index
#define IX_RC_CI_001 -11
#define IX_RC_CI_002 -12
#define IX_RC_CI_003 -13
#define IX_RC_CI_004 -14

// RC for Delete Index
#define IX_RC_DI_001 -21
#define IX_RC_DI_002 -22
// RC for Open Index
#define IX_RC_OI_001 -31
// RC for Insert Entry
#define IX_RC_IE_001 -41
#define IX_RC_IE_002 -42
#define IX_RC_IE_003 -43
#define IX_RC_IE_004 -44
#define IX_RC_IE_005 -45
#define IX_RC_IE_006 -46
#define IX_RC_IE_007 -47
#define IX_RC_IE_008 -48
#define IX_RC_IE_009 -49
#define IX_RC_IE_010 -50
#define IX_RC_IE_011 -51
// RC for DeleteEntry
#define IX_RC_DE_001 -55
#define IX_RC_DE_002 -56
#define IX_RC_DE_003 -57
#define IX_RC_DE_004 -58
#define IX_RC_DE_005 -59
// RC for Index Scan
// RC for Open Scan
// RC for Close Scan
#define IX_RC_SCAN_01 -61
#define IX_RC_SCAN_02 -62
#define IX_RC_SCAN_03 -63
#define IX_RC_SCAN_04 -64
#define IX_RC_SCAN_05 -65
// RC for Get Next Entry
#define IX_RC_GNE_01 -71
#define IX_RC_GNE_02 -72
#define IX_RC_GNE_03 -73
#define IX_RC_GNE_04 -74
#define IX_RC_GNE_05 -75


extern union IX_KeyValue
{
	float 			fValue;
	int 			iValue;
	unsigned char 	uData[4];
} ikv;

typedef struct IX_KeyData
{
	IX_KeyValue		val;
	RID				rid;
} IX_KeyData;



class IX_Node;
class IX_Leaf;
class IX_BTree;



class IX_Node
{
	public:
		unsigned 			noOfKeys;
		vector<RID> 			childPtrs;
		vector<IX_KeyValue> 	childKeys;
		RID					parentPtr;

	    IX_Node();                                                    // Default constructor
	    IX_Node(IX_Node *iN);

	    ~IX_Node();                                                   // Destructor
	    void print();


};

class IX_Leaf
{
	public:
		unsigned 			noOfKeys;
		vector<IX_KeyData> 	keyDataList;
		RID					parentPtr;
		RID					prevPtr;
		RID					nextPtr;

		IX_Leaf();                                                    // Default constructor
	    IX_Leaf(IX_Leaf *iL);
	    ~IX_Leaf();                                                   // Destructor
	    void print();
};


class IX_BTree
{
	public:

		static IX_BTree* Instance(); // Access to the _btree instance
/*		
 *
 * The following B+Tree functions will be provided by this class...
 *
-	addKeyToLeaf()
-	removeKeyFromLeaf()
-	addKeyToNode()
-	removeKeyFromNode()
-	copyUpKey()
-	pushUpKey()
-	pushDownKey()
-	splitNode()
-	splitLeaf()
-	mergeNode()
-	mergeLeaf()
-	getRoot()
-	isNodeFull()
-	isLeafFull()
-	getNodeAtPointer()
-	getLeafAtPointer()
-	getSiblingsForNode()
-	getSiblingsForLeaf()
	*/	

	protected:
		IX_BTree(); // Constructor
		~IX_BTree(); // Destructor

	private:
		static IX_BTree *_btree;
};


#endif /* IXBTREE_H_ */

