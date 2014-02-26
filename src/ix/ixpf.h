/*
 * ixpf.h
 *
 *  Created on: Nov 7, 2011
 *	CS222 - Principles of Database Systems
 *		Project 2 - Index Manager Interface (IX)
 *		Coded By: Dexter (#24)
 *
 *	The IXPF is a singleton class with utility functions that encapsulate the PF layer details
 *	from the IX or upper layers. It supports a number of low-level functions like
 *	PagedFile lifecycle operations for index data.
 */

#include <stdio.h>
#include <string>
#include <cstring>
#include "../rm/rm.h"
#include "btree.h"


#ifndef IXPF_H_
#define IXPF_H_


using namespace std;

// This is the memory representation of an Index Definition in the database.
// It references the ColRef object defined in the catalog class
typedef struct IndexRef
{
	string indexName; // index name
	PF_FileHandle pfh; // PF FileHandle of the PagedFile corresponding to the table
	list<ColRef> colRefs; // Array of column References... allows you to directly access your columns by column position.
} IndexRef;


class IXPF
{
	public:

		static IXPF* Instance();                                      // Access to the _ixpf_intf instance


		static PF_Manager *_pf_mgr;
		IndexRef _indexRefs[10]; // This is the cached IndexReference objects


		RC openIndex(const string indexName, PF_FileHandle &fh);

		//  This call may be required only as a cleanup.
		RC closeIndex(PF_FileHandle pf);

		// This call encapsulates the calls to the PF Layer to destroy the PagedFile at the end of a
		// destroyIndex function called by IX.
		RC removeIndex(const string indexName);

		/* This methods looks for the first available page in a PagedFile that can accomodate the LEAF record
		 * It loops through all the pages one by one until it can find room on the page.
		 * The room on the page should account for the space required for the Slot in the directory
		 * as well as the record itself.
		 *
		 * If no existing pages can accomodate the record with the size specified, then a new page is
		 * added to the PagedFile and returned.
		 */
		int locateNextFreeLeafPage(PF_FileHandle pf, unsigned newRecSize);

		/* This methods looks for the first available page in a PagedFile that can accomodate the NODE record
		 * It loops through all the pages one by one until it can find room on the page.
		 * The room on the page should account for the space required for the Slot in the directory
		 * as well as the record itself.
		 *
		 * If no existing pages can accomodate the record with the size specified, then a new page is
		 * added to the PagedFile and returned.
		 */
		int locateNextFreeNodePage(PF_FileHandle pf, unsigned newRecSize);
		/*
		 * This checks if a PagedFile already exists on the disk.
		 * If so then the CREATE TABLE on that table should not proceed
		 */
		bool isIXPagedFileCreated(const string indexName);

		RC removeIndexEntry(const string indexName, RID rid);
		RC updateIndexEntry(const string indexName, const void *recordData, RID rid);
		RC insertIndexEntry(const bool bLeafEntry, const string indexName, const void *data, RID &rid);
		RC getIndexEntry(const string indexName, const RID rid, bool &bLeafEntry, IX_Node &indexNode, IX_Leaf &indexLeaf);

		void convertDataBufferToIndexNodeEntry(const void* dataBuffer, IX_Node &indexNode);
		void convertDataBufferToIndexLeafEntry(const void* dataBuffer, IX_Leaf &indexLeaf);
		void convertIndexNodeToDataBuffer(void* dataBuffer, const IX_Node &indexNode);
		void convertIndexLeafToDataBuffer(void* dataBuffer, const IX_Leaf &indexLeaf);

		RC insertKey(const string indexName, RID currChildPtr, const IX_KeyData inputKD, IX_KeyData &newChildEntry);
		RC deleteKey(const string indexName, unsigned parentKeyCount, RID currChildPtr, const IX_KeyData inputKD, RID &collapseRid);

		RID getRootRID(const string indexName);
		RC setRootRID(const string indexName, RID rid);
		bool isRootNull(string indexName);
		RC getRoot(const string indexName, IX_Node &rootNode);
		bool isLeaf(const RID rid);
		int compareKeys(IndexRef ixRef, IX_KeyValue key1, IX_KeyValue key2);
		RC addKeyDataToLeaf(IndexRef iRef, IX_Leaf &ixLeaf, IX_KeyData inputKD);
		void addKeyDataToNode(IndexRef iRef, IX_Node &ixNode, IX_KeyData inputKD);

		RC splitLeaf(IndexRef iRef, RID leafRid, IX_Leaf ixLeaf, IX_KeyData ixKD, IX_KeyData &newChildEntry);
		RC splitNode(IndexRef iRef, RID nodeRid, IX_Node ixNode, IX_KeyData &newChildEntry);
		RC collapseNode(const string indexName, RID currPtr, IX_Node ixNode, RID &collapseRid);

		RID createNewRoot(IndexRef iRef, RID oldRootRid, IX_Node &ixNode, IX_KeyData inputKD);


		RID	find(const string indexName, IX_KeyValue  Key);
		RID tree_search(RID &rid, IX_KeyValue  Key);
		string m_indexTableName;
		RC deleteKeyDataFromLeaf(const string indexName, unsigned parentKeyCount, RID currPtr, const IX_KeyData, IX_Leaf ixLeaf, RID &collapseRid);




		RC releaseIndexRef(const string indexName);
		IndexRef getIndexRef(const string indexName);


	protected:
		IXPF();                                                       // Constructor
		~IXPF   ();                                                   // Destructor

	private:
		static IXPF *_ixpf_intf;

};


//This converts a string to Uppercase
string _strToUpper(string str);

#endif /* IXPF_H_ */
