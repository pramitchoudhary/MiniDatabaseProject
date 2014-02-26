/*
 * ixpf.cc
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

#include "../rm/pf_interface.h"
#include "ixpf.h"
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <iostream>
#include <list>
#include <vector>
#include <sys/stat.h>


using namespace std;

IXPF* IXPF::_ixpf_intf = IXPF::Instance();
PF_Manager* IXPF::_pf_mgr = PF_Manager::Instance();



IXPF* IXPF::Instance()
{
    if(!_ixpf_intf)
    	_ixpf_intf = new IXPF();

    return _ixpf_intf;
}


IXPF::IXPF()
{
	for (int i=0; i<10; i++)
	{
		_indexRefs[i].indexName = "EMPTY";
	}

}

IXPF::~IXPF()
{
}

/*
 * This is a function that encapsulates some low-level calls to the PF layer.
 * It provides the IX layer an ability to operate on an abstraction called Index which in the PF layer is a Paged File.
 * However, the Creation and Opening of the PagedFile on the filesystem will need ot be done before any operation
 * This method takes care of sequencing such file lifecycle operations.
 *
 * This method creates a PagedFile on the disk if one does not exist.
 * If a file exists, it will use it.
 * OpenIndex is valid only after a index has been created. Therefore, this call checks if the index exists in the Catalog.
 * As a side-effect of this call, this method sets up a TableRef object for the table in the cache of TableRef objects held in memory.
 *
 */
RC IXPF::openIndex(const string indexName, PF_FileHandle &fh)
{
	RC rc = 0;
    PF_Interface *pfi = PF_Interface::Instance();
    string upperIndexName = _strToUpper(indexName);

    struct stat stFileInfo;
    if (stat(upperIndexName.c_str(), &stFileInfo) == 0)
    {
        cout << upperIndexName << "File located on disk. Reusing." << endl;
    }
    else
    {
        // No catalog file found. Create one.
        rc = _pf_mgr->CreateFile(upperIndexName.c_str());
        if(rc!=0)
        {
            cout << "File " << upperIndexName << " could not be created. Cannot proceed without a catalog." << endl;
            return rc;
        }

    }

    // At this point a file called upperIndexName should exist on the disk.
    rc = _pf_mgr->OpenFile(upperIndexName.c_str(), fh);
    if(rc!=0)
    {
        cout << "File " << upperIndexName << " could not be opened." << endl;
        return rc;
    }

    // Get the number of pages
    int count = fh.GetNumberOfPages();
    if (count>IX_NODE_PAGES)
    {
    	// Good so the Pagedfile has already been setup on the disk and you should go ahead with it.
    }
    else
    {
    	cout << "Index File contains invalid number of pages. Found " << count << "." << endl;
    	cout << "Reinitializing Index File." << endl;
        // You need to add at least one page so that we can start writing or modifying content in here.
        // Append the first page
        // Write blank ASCII characters
        void *data = malloc(PF_PAGE_SIZE);
        // Initialize the page data with blanks
        for(unsigned i = 0; i < PF_PAGE_SIZE-8; i++)
        {
            *((char *)data+i) = ' ';
        }

        // This is the dummy entry for the 'root' node
        pfi->addIntToBuffer(data, 0, 0);
        pfi->addIntToBuffer(data, 4, 0);

        // This is the slot corresponding to the 'root' entry
        pfi->addIntToBuffer(data, PF_PAGE_SIZE-16, 0);
        pfi->addIntToBuffer(data, PF_PAGE_SIZE-12, 8);

        // Note that the free space pointer starts from 8 as we have a dummy root entry.
        pfi->addIntToBuffer(data, PF_PAGE_SIZE-8, 1); // The first slot is that root node pointer
        pfi->addIntToBuffer(data, PF_PAGE_SIZE-4, 8); // Free space pointer

        // Re-Write Page 0.
        if (fh.WritePage(0, data) != 0)
        	fh.AppendPage(data);

        // Add as many pages as necessary so that you have the required number of Node pages and atleast 1 data page.
        for(int i = 0; i < PF_PAGE_SIZE-8; i++)
        {
            *((char *)data+i) = ' ';
        }
        pfi->addIntToBuffer(data, PF_PAGE_SIZE-8, 0); // Number of Slots on Page
        pfi->addIntToBuffer(data, PF_PAGE_SIZE-4, 0); // Free space pointer
        for (int i=1; i<=IX_NODE_PAGES; i++)
		{
        	if (i<count)
        		fh.WritePage(i,data);
        	else
        		fh.AppendPage(data);
		}


        free(data);
    }

    // Hopefully all should have worked right and a success should be returned.
	return rc;

}



//  This call may be required only as a cleanup. We can release a TableRef object if one exists.
RC IXPF::closeIndex(PF_FileHandle pf)
{
	RC rc = 0;
	rc = _pf_mgr->CloseFile(pf);

    if (rc!=0)
    {
        //cout << "File could not be closed properly." << endl;
        return rc;
    }

	return rc;
}

/*
 * This call encapsulates the calls to the PF Layer to destroy the PagedFile at the end
 * of a destroyIndex function called by IX.
 *
 */
RC IXPF::removeIndex(const string indexName)
{
	RC rc;

	string upperIndexName = _strToUpper(indexName);

    rc = _pf_mgr->DestroyFile(upperIndexName.c_str());
    struct stat stFileInfo;
    if (stat(upperIndexName.c_str(), &stFileInfo) == 0)
    {
        cout << "Failed to destroy file! RetCode=" << rc << "." << endl;
        return -1;
    }
    else
    {
        cout << "File " << upperIndexName << " has been destroyed." << endl;
        return rc;
    }
    return rc;
}


/* This methods looks for the first available page in a PagedFile that can accomodate the Leaf record
 * It loops through all the pages one by one until it can find room on the page.
 * The room on the page should account for the space required for the Slot in the directory
 * as well as the record itself.
 *
 * If no existing pages can accomodate the record with the size specified, then a new page is
 * added to the PagedFile and returned.
 */
int IXPF::locateNextFreeLeafPage(PF_FileHandle pf, unsigned newRecSize)
{
	RC rc;
	unsigned freePtr = 0;
	unsigned noOfSlots = 0;
    PF_Interface *pfi = PF_Interface::Instance();
    int freeSpaceOnPage = 0;

    unsigned iMaxPages= pf.GetNumberOfPages();
    void *buffer = malloc(PF_PAGE_SIZE);


    // Leaf nodes will start from IX_NODE_PAGES
    for (unsigned i=IX_NODE_PAGES;i<iMaxPages;i++)
    {
        rc = pfi->getPage(pf, i, buffer);
    	if (rc != 0)
    	{
    		cout << "ERROR - Unable to read Page from Table. RC=" << rc << endl;
    		return -1;
    	}

    	// Locate the Free Pointer
    	freePtr = pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-4);
    	// Find the number of slots in the slot directory
    	noOfSlots = pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);


    	// Look for space on the page for a record of size=newRecSize
    	// compute the value of newSlotPtr
    	freeSpaceOnPage = PF_PAGE_SIZE - 8 // accounting for the free space pointer & no of slots
    			- 8 * noOfSlots // each slot occupies 4 bytes for offset and 4 more for size
    			- 8 // accounting for 1 new slot for the record to beinserted
    			- freePtr;
    	if (freeSpaceOnPage>=(int)newRecSize)
    	{
    		free(buffer);
    		return i;
    	}
    }
	free(buffer);

    // If it exited the loop then there is no space in all existing pages.
    // Simply add a new page and return the page id of the new page.
    // You need to add atleast one page so that we can start writing or modifying content in here.
    // Append the first page
    // Write blank ASCII characters
    void *data = malloc(PF_PAGE_SIZE);
    // Initialize the page data with blanks
    for(unsigned i = 0; i < PF_PAGE_SIZE-8; i++)
    {
        *((char *)data+i) = ' ';
    }

    pfi->addIntToBuffer(data, PF_PAGE_SIZE-8, 0);
    pfi->addIntToBuffer(data, PF_PAGE_SIZE-4, 0);
    rc = pf.AppendPage(data);
    if (rc !=0)
    {
    	cout << "ERROR - Unable to Append Page on Table. RC=" << rc << endl;
        free(data);
    	return -1;
    }

    iMaxPages = pf.GetNumberOfPages();
    if (iMaxPages >0)
    {
    	free(data);
    	return iMaxPages-1; // PageId is always 0-based
    }
    else
    {
    	cout << "ERROR - Unable to verify NoOfPages for the Table. RC=" << rc << endl;
        free(data);
    	return -1;
    }
}

/* This methods looks for the first available page in a PagedFile that can accomodate the non-Leaf, ie. Node record
 * It loops through all the pages one by one until it can find room on the page.
 * The room on the page should account for the space required for the Slot in the directory
 * as well as the record itself.
 *
 * If no existing pages can accomodate the record with the size specified, then a new page is
 * added to the PagedFile and returned.
 */
int IXPF::locateNextFreeNodePage(PF_FileHandle pf, unsigned newRecSize)
{
	RC rc;
	unsigned freePtr = 0;
	unsigned noOfSlots = 0;
    PF_Interface *pfi = PF_Interface::Instance();
    int freeSpaceOnPage = 0;

    void *buffer = malloc(PF_PAGE_SIZE);


    // Non-leaf NODES will start from Page 0 upto Page IX_NODE_PAGES-1
    for (unsigned i=0;i<IX_NODE_PAGES;i++)
    {
        rc = pfi->getPage(pf, i, buffer);
    	if (rc != 0)
    	{
    		cout << "ERROR - Unable to read Page from Table. RC=" << rc << endl;
    		return -1;
    	}

    	// Locate the Free Pointer
    	freePtr = pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-4);
    	// Find the number of slots in the slot directory
    	noOfSlots = pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);


    	// Look for space on the page for a record of size=newRecSize
    	// compute the value of newSlotPtr
    	freeSpaceOnPage = PF_PAGE_SIZE - 8 // accounting for the free space pointer & no of slots
    			- 8 * noOfSlots // each slot occupies 4 bytes for offset and 4 more for size
    			- 8 // accounting for 1 new slot for the record to beinserted
    			- freePtr;
    	if (freeSpaceOnPage>=(int)newRecSize)
    	{
    		free(buffer);
    		return i;
    	}
    }
	free(buffer);

    // If it exited the loop then there is no space in all existing pages,
	// then there is no room on the Index table for these entries and therefore the
	// index will need to be reorganized with more pages allotted for Node pages.
    // Index reorg is not provided in this implementation.
	cout << "ERROR - No more room to store Index Node records. Reorganize Index." << endl;
	return -1;
}

/*
 * This checks if a PagedFile already exists on the disk.
 * If so then the CREATE TABLE on that table should not proceed
 */
bool IXPF::isIXPagedFileCreated(const string indexName)
{
    string upperIndexName = _strToUpper(indexName);
    struct stat stFileInfo;
    if (stat(upperIndexName.c_str(), &stFileInfo) == 0)
    {
        cout << upperIndexName << " exists on disk. INDEX HAS ALREADY BEEN CREATED." << endl;
    	return true;
    }
    else
    {
    	return false;
    }
}

/*
 * This method removes the entry from the Index Reference Cache held in the memory.
 * This is typically important for us to refresh the IndexRef object when the index has been dropped.
 */
RC IXPF::releaseIndexRef(const string indexName)
{
	string upperIndexName = _strToUpper(indexName);
	IXPF *_ixpf = IXPF::Instance();
	//TableRef tRef;
	int i = 0;
	RC rc;
	if (upperIndexName.compare("CATALOG") == 0)
	{
		cout << "Catalog Reference Requested. This entry cannot be released. No action taken." << endl;
		return 0;
	}


	for (i=0; i<10; i++)
	{
		if ((_indexRefs[i].indexName.compare("EMPTY")) == 0)
		{
			continue;
		}

		// This means there is a Table Reference that exists and must be released.
		if (_indexRefs[i].indexName.compare(upperIndexName)==0)
		{
			rc = _ixpf->closeIndex(_indexRefs[i].pfh);

			//You need to erase the contents of _tableRefs[i].colRefs
			while (!_indexRefs[i].colRefs.empty())
			{
				_indexRefs[i].colRefs.pop_front();
			}

			_indexRefs[i].indexName.assign("EMPTY");
		}
	}
	return 0;
}

/*
 * This is a method that provides a memory reference to the index structure defined earlier.
 * The IndexRef object is used to recognize the constituents of the data records or to format them accordingly.
 */
IndexRef IXPF::getIndexRef(const string indexName)
{
	string upperIndexName = _strToUpper(indexName);
	//PF_Interface *_pfi = PF_Interface::Instance();
	IXPF *_ixpf = IXPF::Instance();
	Catalog *_cat = Catalog::Instance();
	IndexRef iRef;
	int i = 0;
	int iCacheEntry = -1;

	for (i=0; i<10; i++)
	{
		if ((_indexRefs[i].indexName.compare("EMPTY")) == 0)
		{
			iCacheEntry = i; // You can use this empty location to place the TableRef if this is the first time.
			continue;
		}

		// This means there is a Table Reference that can be used.
		if (_indexRefs[i].indexName.compare(upperIndexName)==0)
		{
			//cout << "Table Entry has been cached already. Returning cache entry for..." << _indexRefs[i].indexName << endl;
			return _indexRefs[i];
		}
	}

	// A new IndexRef object needs to be prepared...
	(iRef.indexName).assign(upperIndexName);
	_cat->scanCatalog(upperIndexName, iRef.colRefs);
	if (iRef.colRefs.size()>0)
		_ixpf->openIndex(upperIndexName,iRef.pfh);

	// update an empty cache entry
	if (iCacheEntry != -1)
	{
		_indexRefs[iCacheEntry] = iRef;
	}

	return iRef;
}

// This gets called in insertTree when the Leaf node has no room to accomodate the incoming KeyData
RC IXPF::splitLeaf(IndexRef iRef, RID leafRid, IX_Leaf ixLeaf, IX_KeyData ixKD, IX_KeyData &newChildEntry)
{
	IXPF *_ixpf = IXPF::Instance();
	IX_Leaf ixLeaf1;
	RC rc;

	bool foundSpot = false;
	int comp;
	//cout << "Before Adding, Leaf Node has " << ixLeaf.noOfKeys << " of " << ixLeaf.keyDataList.size() << " populated." << endl;
	for (unsigned i=0; i<ixLeaf.noOfKeys; i++)
	{
		comp = _ixpf->compareKeys(iRef, ixKD.val, ixLeaf.keyDataList.at(i).val);
		if (comp == -1)
		{
			// OK YOu finally found the position where the KeyData should be inserted.
			vector<IX_KeyData>::iterator kdIter;
			kdIter = ixLeaf.keyDataList.begin()+i;
			ixLeaf.keyDataList.insert(kdIter, ixKD);
			//cout << "During the split, Leaf Node has " << ixLeaf.noOfKeys << " of " << ixLeaf.keyDataList.size() << " populated." << endl;
			// Now go and perform the split.
			foundSpot=true;
			break;
		}
		else if (comp == 0)
		{
			cout << "Attempting to Insert a DUPLICATE Key Entry Value{fVal|iVal} = {" << ixKD.val.fValue << "|" << ixKD.val.iValue<< "} ... KeyData is [" << ixLeaf.keyDataList.at(i).rid.pageNum << "|" << ixLeaf.keyDataList.at(i).rid.slotNum<< "]." << endl;
			return -10;
		}
	}
	if (!foundSpot)
	{
		// Simply insert the DataList at the end.
		vector<IX_KeyData>::iterator kdIter;
		kdIter = ixLeaf.keyDataList.end();
		ixLeaf.keyDataList.insert(kdIter, ixKD);
		foundSpot = true;

	}

	if (ixLeaf.keyDataList.size() > IX_MAX_CHILDREN)
	{
		// COW BOOK - first d entries stay, the rest move to brand new Leaf L2

		// This is where the split operation is performed.
		// Duplicate ixLeaf as ixLeaf1
		ixLeaf1 = new IX_Leaf(&ixLeaf);
		// remove the last d+1 entries in the Leaf
		for (int i=IX_MIN_CHILDREN; i<IX_MAX_CHILDREN; i++) // erase the values in them
		{
//			ixLeaf.keyDataList.at(i).val.iValue=IX_NOVALUE;
			if (iRef.colRefs.front().colType == TypeInt)
				ixLeaf.keyDataList.at(i).val.iValue=IX_NOVALUE;
			else //(attType == TypeReal)
				ixLeaf.keyDataList.at(i).val.fValue=IX_HIFLOAT;


			ixLeaf.keyDataList.at(i).rid.pageNum=IX_NOVALUE;
			ixLeaf.keyDataList.at(i).rid.slotNum=IX_NOVALUE;
		}
		ixLeaf.keyDataList.erase(ixLeaf.keyDataList.begin()+IX_MAX_CHILDREN);
		ixLeaf.noOfKeys = IX_MIN_CHILDREN; // After the split.

		// remove the first d entries in the Leaf1 and add a dummy at the end to make the count = 4
		ixLeaf1.keyDataList.erase(ixLeaf1.keyDataList.begin(), ixLeaf1.keyDataList.begin()+IX_MIN_CHILDREN);
		IX_KeyData tempL;
		//tempL.val.iValue=IX_NOVALUE;
		if (iRef.colRefs.front().colType == TypeInt)
			tempL.val.iValue=IX_NOVALUE;
		else //(attType == TypeReal)
			tempL.val.fValue=IX_HIFLOAT;
		tempL.rid.pageNum=IX_NOVALUE;
		tempL.rid.slotNum=IX_NOVALUE;
		for (int i=ixLeaf1.keyDataList.size(); i<IX_MAX_CHILDREN; i++ ) ixLeaf1.keyDataList.push_back(tempL);

		ixLeaf1.prevPtr.pageNum = leafRid.pageNum;
		ixLeaf1.prevPtr.slotNum = leafRid.slotNum;
		ixLeaf1.noOfKeys = IX_MIN_CHILDREN+1; // After the split.

	    RID leaf1Rid;
	    cout << "   ---->Inserting a new LEAF Leaf1 to File: " << endl;
		void *dataBuffer = malloc(IX_LEAF_SIZE);
	    //cout << endl << "Converting Leaf1 to Buffer: " << endl;
	    //ixLeaf1.print();
		_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeaf1);
		rc = _ixpf->insertIndexEntry(true, iRef.indexName, dataBuffer, leaf1Rid);
		if (rc!=0)
		{
			cout << "ERROR - Cannot insert new LEAF Entry at PageNumber=" << leaf1Rid.pageNum << "SlotNumber=" << leaf1Rid.slotNum << endl;
		    free(dataBuffer);
			return -7;
		}
		cout << "   ---->new LEAF Entry inserted at PageNumber=" << leaf1Rid.pageNum << "SlotNumber=" << leaf1Rid.slotNum << endl;
	    ixLeaf1.print();

		ixLeaf.nextPtr.pageNum = leaf1Rid.pageNum;
		ixLeaf.nextPtr.slotNum = leaf1Rid.slotNum;
	    //cout << endl << "Converting Leaf to Buffer: " << endl;
		_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeaf);
		rc = _ixpf->updateIndexEntry(iRef.indexName, dataBuffer, leafRid);
		if (rc!=0)
		{
			cout << "ERROR - Cannot update existing LEAF Entry at PageNumber=" << leafRid.pageNum << "SlotNumber=" << leafRid.slotNum << endl;
		    free(dataBuffer);
			return -8;
		}
		cout << "   ---->existing LEAF Entry updated at PageNumber=" << leafRid.pageNum << "SlotNumber=" << leafRid.slotNum << endl;
	    ixLeaf.print();

		// Save the value of the Next Pointer of the current Leaf so that you can go there and update its prevPointer
		RID leaf2Rid;
		IX_Node iN; //Temp - will not be used anywhere.
		IX_Leaf ixLeaf2;
		leaf2Rid.pageNum = ixLeaf1.nextPtr.pageNum;
		leaf2Rid.slotNum = ixLeaf1.nextPtr.slotNum;
		bool bTemp = true;
		if ((leaf2Rid.pageNum==0) && (leaf2Rid.pageNum==0))
		{
			cout << "   ----> Next Pointer of Leaf2 Not updated as it is the end of the linked list." << endl;

		}
		else
		{
			if (_ixpf->getIndexEntry(iRef.indexName, leaf2Rid, bTemp, iN, ixLeaf2) == 0)
			{
				ixLeaf2.prevPtr.pageNum = leaf1Rid.pageNum;
				ixLeaf2.prevPtr.slotNum = leaf1Rid.slotNum;
				_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeaf2);
				rc = _ixpf->updateIndexEntry(iRef.indexName, dataBuffer, leaf2Rid);
				if (rc!=0)
				{
					cout << "ERROR - Cannot update existing LEAF Entry (for PrevPtr) at PageNumber=" << leaf2Rid.pageNum << "SlotNumber=" << leaf2Rid.slotNum << endl;
				    free(dataBuffer);
					return -9;
				}
				cout << "   ---->existing LEAF Entry updated at PageNumber=" << leaf2Rid.pageNum << "SlotNumber=" << leaf2Rid.slotNum << endl;
				ixLeaf2.print();
			}
		}


	    free(dataBuffer);

		memcpy(newChildEntry.val.uData, ixLeaf1.keyDataList.at(0).val.uData, 4);
	    newChildEntry.rid.pageNum=leaf1Rid.pageNum;
	    newChildEntry.rid.slotNum=leaf1Rid.slotNum;

	}

    return 0;
}

// This gets called in insertTree when the non-Leaf node has no room to accomodate the incoming KeyData
//RC IXPF::splitNode(IndexRef iRef, RID nodeRid, IX_Node ixNode, IX_KeyData ixKD, IX_KeyData &newChildEntry)
RC IXPF::splitNode(IndexRef iRef, RID nodeRid, IX_Node ixNode, IX_KeyData &newChildEntry)
{


	IXPF *_ixpf = IXPF::Instance();
	IX_Node ixNode1;

	bool foundSpot = false;
	int comp;
	//cout << "Before Adding, the Node has " << ixNode.noOfKeys << " of " << ixNode.childKeys.size() << " childkeys and " << ixNode.childPtrs.size() << "pointers populated." << endl;
	for (unsigned i=0; i<ixNode.noOfKeys; i++)
	{
		comp = _ixpf->compareKeys(iRef, newChildEntry.val, ixNode.childKeys.at(i));
		if (comp == -1)
		{
			// OK YOu finally found the position where the KeyData should be inserted.
			vector<IX_KeyValue>::iterator kvIter;
			kvIter = ixNode.childKeys.begin()+i;
			ixNode.childKeys.insert(kvIter, newChildEntry.val);

			vector<RID>::iterator ridIter;
			ridIter = ixNode.childPtrs.begin()+i;
			ixNode.childPtrs.insert(ridIter, newChildEntry.rid);

			//cout << "During the Adding, the Node has " << ixNode.noOfKeys << " of " << ixNode.childKeys.size() << " childkeys and " << ixNode.childPtrs.size() << "pointers populated." << endl;
			// Now go and perform the split.
			foundSpot = true;
			break;
		}
	}

	if(!foundSpot)
	{
		vector<IX_KeyValue>::iterator kvIter;
		kvIter = ixNode.childKeys.end();
		ixNode.childKeys.insert(kvIter, newChildEntry.val);

		vector<RID>::iterator ridIter;
		ridIter = ixNode.childPtrs.end();
		ixNode.childPtrs.insert(ridIter, newChildEntry.rid);
		foundSpot = true;
	}

	if (ixNode.childKeys.size() > IX_MAX_CHILDREN)
	{
		IX_KeyData tempL;
		// This is where the split operation is performed.
		// Duplicate ixNode as ixNode1
		ixNode1 = new IX_Node(&ixNode);

		// COW BOOK - first d key values and d+1 childpointers stay
		// remove the last d keys and d+1 pointers in the Node
		ixNode.childKeys.erase(ixNode.childKeys.begin()+IX_MIN_CHILDREN, ixNode.childKeys.end());
		ixNode.childPtrs.erase(ixNode.childPtrs.begin()+IX_MIN_CHILDREN+1, ixNode.childPtrs.end());
		// Fill up the remaining spaces with dummy values.
		//tempL.val.iValue=IX_NOVALUE;
		if (iRef.colRefs.front().colType == TypeInt)
			tempL.val.iValue=IX_NOVALUE;
		else //(attType == TypeReal)
			tempL.val.fValue=IX_HIFLOAT;

		tempL.rid.pageNum=IX_NOVALUE;
		tempL.rid.slotNum=IX_NOVALUE;
		for (int i=ixNode.childKeys.size(); i<IX_MAX_CHILDREN; i++ )
			ixNode.childKeys.push_back(tempL.val);
		for (int i=ixNode.childPtrs.size(); i<IX_MAX_CHILDREN+1; i++ )
			ixNode.childPtrs.push_back(tempL.rid);
		ixNode.noOfKeys = IX_MIN_CHILDREN;
		//ixNode.print();

		// COW BOOK - last d keys and d+1 childpointers move to new node N2
		// Remember... there were 2d+1 keys and 2d+2 pointers at the start.
		// You have kept d keys and d+1 pointers.
		// You plan to keep d keys and d+1 pointers.
		// So... that means we will eliminate 1 key somehow. That will be the middle key.

		// Costly error if you miss this - DO NOT DISCARD the Middle Key. Save it. That is the one you send up to the parent.
	    memcpy(newChildEntry.val.uData, ixNode1.childKeys.at(IX_MIN_CHILDREN).uData, 4);

		ixNode1.childKeys.erase(ixNode1.childKeys.begin(), ixNode1.childKeys.begin()+IX_MIN_CHILDREN+1);
		ixNode1.childPtrs.erase(ixNode1.childPtrs.begin(), ixNode1.childPtrs.begin()+IX_MIN_CHILDREN+1);
		//tempL.val.iValue=IX_NOVALUE;
		if (iRef.colRefs.front().colType == TypeInt)
			tempL.val.iValue=IX_NOVALUE;
		else //(attType == TypeReal)
			tempL.val.fValue=IX_HIFLOAT;
		
		tempL.rid.pageNum=IX_NOVALUE;
		tempL.rid.slotNum=IX_NOVALUE;
		for (int i=ixNode1.childKeys.size(); i<IX_MAX_CHILDREN; i++ )
			ixNode1.childKeys.push_back(tempL.val);
		for (int i=ixNode1.childPtrs.size(); i<IX_MAX_CHILDREN+1; i++ )
			ixNode1.childPtrs.push_back(tempL.rid);
		ixNode1.noOfKeys = IX_MIN_CHILDREN;
		//ixNode1.print();

	    RID node1Rid;
	    cout << "   --->Inserting New Node1 to File: " << endl;
		void *dataBuffer = malloc(IX_NODE_SIZE);
	    //cout << endl << "Converting Node1 to Buffer: " << endl;
		_ixpf->convertIndexNodeToDataBuffer(dataBuffer, ixNode1);
		_ixpf->insertIndexEntry(false, iRef.indexName, dataBuffer, node1Rid);
		cout << "   -->NODE Entry inserted at PageNumber=" << node1Rid.pageNum << "SlotNumber=" << node1Rid.slotNum << endl;
		ixNode1.print();

		//cout << endl << "Converting Node to Buffer: " << endl;
		_ixpf->convertIndexNodeToDataBuffer(dataBuffer, ixNode);
		_ixpf->updateIndexEntry(iRef.indexName, dataBuffer, nodeRid);
		cout << "   -->NODE Entry Updated at PageNumber=" << nodeRid.pageNum << "SlotNumber=" << nodeRid.slotNum << endl;
		ixNode.print();


		free(dataBuffer);
	    //memcpy(newChildEntry.val.uData, ixNode1.childKeys.at(0).uData, 4);
	    newChildEntry.rid.pageNum=node1Rid.pageNum;
	    newChildEntry.rid.slotNum=node1Rid.slotNum;



	}
    return 0;
}

// All this method will do is put a data buffer into the right place in the Index table.
// The data buffer should be formatted appropriately depending upon whether this is the Node or the leaf.
RC IXPF::insertIndexEntry(const bool bLeafEntry, const string indexName, const void *recordData, RID &rid)
{
//	Catalog *_ci = Catalog::Instance();
	PF_Interface *m_pfi = PF_Interface::Instance();
	IXPF *_ixpf = IXPF::Instance();

	string upperIndexName = _strToUpper(indexName);
	// Variable Initialization
	int newRecSize=0;
	int freePtr=0;
	int newSlotPtr=0;
	int noOfSlots=0;
	int newSlotSize=0;
	int pgNum=0;
	RC rc;
	void *buffer = malloc(PF_PAGE_SIZE);

	// Determine the size of the record to structure the record field
	IndexRef l_indexobj = _ixpf->getIndexRef(upperIndexName);

	// Locate the next free page
	if (bLeafEntry == true)
	{
		pgNum = _ixpf->locateNextFreeLeafPage(l_indexobj.pfh, IX_LEAF_SIZE);
		newRecSize = IX_LEAF_SIZE;
	}
	else
	{
		pgNum = _ixpf->locateNextFreeNodePage(l_indexobj.pfh, IX_NODE_SIZE);
		newRecSize = IX_NODE_SIZE;
	}


	if (pgNum < 0)
	{
		cout << "ERROR - Unable to locate next free page for Index." << endl;
		return -1;
	}

	// Read the Page and update the buffer
    rc = m_pfi->getPage(l_indexobj.pfh, pgNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to read Page from Table. RC=" << rc << endl;
		return -2;
	}

	//------------------------------------- Reading Slot directory-----------------------------
	// Locate the Free Pointer from the slot directory
	freePtr = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-4);
	// Find the number of slots in the slot directory
	noOfSlots = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	//------------------------------------------------------------------------------------------

	// Write the tuple into the buffer at freePtr
    memcpy((char *)buffer + freePtr, recordData, newRecSize);

    //--------------------------------------- Update the slot directory-----------------------------------------
	//1. Find the position of the new Slot
	//2. compute the value of newSlotPtr
	newSlotPtr = PF_PAGE_SIZE - 8 // accounting for the free space pointer & no of slots
			- 8 * noOfSlots // each slot occupies 4 bytes for offset and 4 more for size
			- 8; // start position for the new slot
	cout<< "NewSlotPointer=" << newSlotPtr << endl;

	newSlotSize = newRecSize;

	// copy current freePtr at location: newSlotPtr
	m_pfi->addIntToBuffer(buffer, newSlotPtr, freePtr);
	// copy newSlotSize at location: newSlotPtr+4
	m_pfi->addIntToBuffer(buffer, newSlotPtr+4, newSlotSize);
	noOfSlots += 1;
	freePtr += newRecSize;
	// Write freePtr at location: PF_PAGE_SIZE-4
	m_pfi->addIntToBuffer(buffer, PF_PAGE_SIZE-4, freePtr);
	// Write noOfSlots at location: PF_PAGE_SIZE-8
	m_pfi->addIntToBuffer(buffer, PF_PAGE_SIZE-8, noOfSlots);

	rid.slotNum = noOfSlots;
	rid.pageNum = pgNum;

    rc = m_pfi->putPage(l_indexobj.pfh, pgNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table. RC=" << rc << endl;
		return -3;
	}

	return 0;
}

// Use this method if a node or leaf exists in the database and so you only want to do is
// remove it completely from the index table.
RC IXPF::removeIndexEntry(const string indexName, RID rid)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	IXPF *_ixpf = IXPF::Instance();

	string upperIndexName = _strToUpper(indexName);
	// Variable Initialization
	int newRecSize=0;
	int noOfSlots=0;
	RC rc;
	// Determine the size of the record to structure the record field
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);

	if (iRef.pfh.GetNumberOfPages() <= rid.pageNum)
	{
		cout << "ERROR - Invalid Page Number Specified in removeIndexEntry. rid=[" << rid.pageNum << "," << rid.slotNum << "]." << endl;
		return -3;
	}

	// Read the Page and update the buffer
	void *buffer = malloc(PF_PAGE_SIZE);
    rc = m_pfi->getPage(iRef.pfh, rid.pageNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to read Page from Table... in removeIndexEntry RC=" << rc << endl;
		free(buffer);
		return -2;
	}

	//------------------------------------- Reading Slot directory-----------------------------
	// Find the number of slots in the slot directory
	noOfSlots = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	//------------------------------------------------------------------------------------------
	if (noOfSlots < (int)rid.slotNum)
	{
		cout << "ERROR - Invalid Slot Number Specified in removeIndexEntry. rid=[" << rid.pageNum << "," << rid.slotNum << "]." << endl;
		free(buffer);
		return -4;
	}

	// Index entries are always the same size and so don't worry about any sizes or tombstones.
	if (_ixpf->isLeaf(rid) == true)
		newRecSize = IX_LEAF_SIZE;
	else
		newRecSize = IX_NODE_SIZE;

	int slotPtr = PF_PAGE_SIZE - 8 - 8*rid.slotNum;
	int recordOffset = m_pfi->getIntFromBuffer(buffer, slotPtr);

	// Write the tuple into the buffer at freePtr
	// copying byte by byte into the buffer
    for(int k = 0; k < newRecSize; k++) *((char *)buffer+recordOffset+k) = '-';

    // Update the slot directory about the deleted record<-1(offset); 0(record size)>
	m_pfi->addIntToBuffer(buffer, slotPtr, -1);
	m_pfi->addIntToBuffer(buffer, slotPtr+4, 0);

	// No other fields need to change.
    rc = m_pfi->putPage(iRef.pfh, rid.pageNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table in removeIndexEntry. RC=" << rc << endl;
		free(buffer);
		return -1;
	}

	free(buffer);
	return 0;
}

// Use this method if a node or leaf exists in the database and so you only want to update the key values or pointers as appropriate
RC IXPF::updateIndexEntry(const string indexName, const void *recordData, RID rid)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	IXPF *_ixpf = IXPF::Instance();

	string upperIndexName = _strToUpper(indexName);
	// Variable Initialization
	int newRecSize=0;
	int noOfSlots=0;
	RC rc;
	// Determine the size of the record to structure the record field
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);

	if (iRef.pfh.GetNumberOfPages() <= rid.pageNum)
	{
		cout << "ERROR - Invalid Page Number Specified in updateIndexEntry. rid=[" << rid.pageNum << "," << rid.slotNum << "]." << endl;
		return -3;
	}

	// Read the Page and update the buffer
	void *buffer = malloc(PF_PAGE_SIZE);
    rc = m_pfi->getPage(iRef.pfh, rid.pageNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to read Page from Table... in updateIndexEntry RC=" << rc << endl;
		free(buffer);
		return -2;
	}

	//------------------------------------- Reading Slot directory-----------------------------
	// Find the number of slots in the slot directory
	noOfSlots = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	//------------------------------------------------------------------------------------------
	if (noOfSlots < (int)rid.slotNum)
	{
		cout << "ERROR - Invalid Slot Number Specified in updateIndexEntry. rid=[" << rid.pageNum << "," << rid.slotNum << "]." << endl;
		free(buffer);
		return -4;
	}

	// Index entries are always the same size and so don't worry about any sizes or tombstones.
	if (_ixpf->isLeaf(rid) == true)
		newRecSize = IX_LEAF_SIZE;
	else
		newRecSize = IX_NODE_SIZE;

	int slotPtr = PF_PAGE_SIZE - 8 - 8*rid.slotNum;
	int recordOffset = m_pfi->getIntFromBuffer(buffer, slotPtr);

	// Write the tuple into the buffer at freePtr
    memcpy((char *)buffer + recordOffset, recordData, newRecSize);

    // Do not touch anything else
    // Go an write the page back.
    rc = m_pfi->putPage(iRef.pfh, rid.pageNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table in updateIndexEntry. RC=" << rc << endl;
		free(buffer);
		return -1;
	}

	free(buffer);
	return 0;
}

RC IXPF::getIndexEntry(const string indexName, const RID rid, bool &bLeafEntry, IX_Node &indexNode, IX_Leaf &indexLeaf)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);
	RC rc;
	unsigned uTemp = 0;
	AttrType attrType;


	if (iRef.colRefs.size()!=1)
	{
		cout << "ERROR - Invalid Catalog information found for the index. Aborting." << endl;
		cout << "ERROR - Catalog should contain just 1 row for the index. " << iRef.colRefs.size() << " rows were returned." << endl;
		return -5;
	}
	else
	{
		attrType = iRef.colRefs.front().colType;
	}

	if (iRef.pfh.GetNumberOfPages()<rid.pageNum)
	{
		if (rid.pageNum != IX_NOVALUE)
				cout << "ERROR - RID contains a Page that does not exist. Aborting. Page#:" << rid.pageNum << "." << endl;
		return -4;
	}


	// You need to allocate for the page being read and clean it up at the end.
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = m_pfi->getPage(iRef.pfh, rid.pageNum, buffer);
	if (rc !=0)
	{
		cout << "ERROR - Read Page#" << rid.pageNum << " from PagedFile " << upperIndexName << " errored out. RC=" << rc << "." << endl;
		free(buffer);
		return -3;
	}

	//Since the table has atleast Page 0. Let us make sure that we do not proceed if there are no rows.
	uTemp = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	if (uTemp < rid.slotNum) // The Slot# you are looking for does not exist.
	{
		cout << "ERROR - Page#" << rid.pageNum << " Slot#" << rid.slotNum << " from PagedFile " << upperIndexName << " does not exist." << endl;
		free(buffer);
		return -2;
	}


	// On this page locate the slot pointer of Slot# in RID
	uTemp = PF_PAGE_SIZE - 8 - 8*rid.slotNum;
	int tuplePtr = m_pfi->getIntFromBuffer(buffer, uTemp);
	int tupleSize = m_pfi->getIntFromBuffer(buffer, uTemp+4);

	void *tempRecBuffer = malloc(tupleSize);
	memcpy((char *)tempRecBuffer, (char *)buffer+tuplePtr, tupleSize);

	// breakup tempRecData into its constituents and update indexEntry
	if (tupleSize == IX_NODE_SIZE)
	{
		_ixpf->convertDataBufferToIndexNodeEntry(tempRecBuffer, indexNode);
		//indexNode.print();
		bLeafEntry=false;
	}
	else if (tupleSize == IX_LEAF_SIZE)
	{
		_ixpf->convertDataBufferToIndexLeafEntry(tempRecBuffer, indexLeaf);
		//indexLeaf.print();
		bLeafEntry = true;
	}
	else
	{
		cout << "ERROR - Page#" << rid.pageNum << " Slot#" << rid.slotNum << " from PagedFile " << upperIndexName << " contains invalid tuple size=" << tupleSize << "." << endl;
		free(buffer);
		free(tempRecBuffer);
		return -1;
	}


	free(tempRecBuffer);
	free(buffer);

	return 0;
}

void IXPF::convertDataBufferToIndexNodeEntry(const void* dataBuffer, IX_Node &indexNode)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	// breakup dataBuffer into its constituents and update indexEntry
	IX_KeyValue keyval;
	RID tempRid;
	// No of Child keys populated
	indexNode.noOfKeys = m_pfi->getIntFromBuffer(dataBuffer,0);
	// Parent Pointer
	indexNode.parentPtr.pageNum = m_pfi->getIntFromBuffer(dataBuffer,4);
	indexNode.parentPtr.slotNum = m_pfi->getIntFromBuffer(dataBuffer,8);
	// First Child Pointer
	tempRid.pageNum = m_pfi->getIntFromBuffer(dataBuffer,12);
	tempRid.slotNum = m_pfi->getIntFromBuffer(dataBuffer,16);
	indexNode.childPtrs.push_back(tempRid);
	int currRecPtr = 20;
	for (int i=0; i<IX_MAX_CHILDREN; i++)
	{
		// This should work whether you have int or float data
		memcpy(keyval.uData, (char *)dataBuffer+currRecPtr, 4);
		indexNode.childKeys.push_back(keyval);
		currRecPtr +=4;
		// Copy the Child Pointer
		tempRid.pageNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
		currRecPtr +=4;
		tempRid.slotNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
		currRecPtr +=4;
		indexNode.childPtrs.push_back(tempRid);
	}

	//indexNode.print();
	//cout << "CurreRecPtr should be " << IX_NODE_SIZE << " check - " << currRecPtr << endl;
}

void IXPF::convertDataBufferToIndexLeafEntry(const void* dataBuffer, IX_Leaf &indexLeaf)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	// No of Child keys populated
	indexLeaf.noOfKeys = m_pfi->getIntFromBuffer(dataBuffer,0);
	// Parent Pointer
	indexLeaf.parentPtr.pageNum = m_pfi->getIntFromBuffer(dataBuffer,4);
	indexLeaf.parentPtr.slotNum = m_pfi->getIntFromBuffer(dataBuffer,8);
	int currRecPtr = 12;
	// For each KeyData Value
	IX_KeyData tempKeyData;
	for (int i=0; i<IX_MAX_CHILDREN; i++)
	{
		// This should work whether you have int or float data
		memcpy(tempKeyData.val.uData, (char *)dataBuffer+currRecPtr, 4);
		currRecPtr +=4;

		// Copy the Child Pointer
		tempKeyData.rid.pageNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
		currRecPtr +=4;
		tempKeyData.rid.slotNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
		currRecPtr +=4;


		indexLeaf.keyDataList.push_back(tempKeyData);
	}

	indexLeaf.prevPtr.pageNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
	currRecPtr +=4;
	indexLeaf.prevPtr.slotNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
	currRecPtr +=4;
	indexLeaf.nextPtr.pageNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
	currRecPtr +=4;
	indexLeaf.nextPtr.slotNum = m_pfi->getIntFromBuffer(dataBuffer,currRecPtr);
	currRecPtr +=4;


	//indexLeaf.print();
	//cout << "CurreRecPtr should be " << IX_LEAF_SIZE << " check - " << currRecPtr << endl;
}

void IXPF::convertIndexNodeToDataBuffer(void* dataBuffer, const IX_Node &indexNode)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	int currPtr = 0;

	// Update the No of Keys populated.
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexNode.noOfKeys);
	currPtr+=4;
	// Update the Parent Pointer
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexNode.parentPtr.pageNum);
	currPtr+=4;
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexNode.parentPtr.slotNum);
	currPtr+=4;

	// At this point, currPtr = 12.
	// We will need to remember this so first we insert all Child Pointers
	// Then we reset the currPtr and insert the key values

	// Copy as many pointers as there are populated.
	vector<RID>::const_iterator cPtrs;
	int i = 1;
	const RID *tempRid;
	for (cPtrs = indexNode.childPtrs.begin(); cPtrs != indexNode.childPtrs.end(); ++cPtrs)
	{
		if (i>(IX_MAX_CHILDREN+1))
		{
			cout << "ERROR - Something is not quite right. You should never have more child pointers than..." << (IX_MAX_CHILDREN+1) << endl;
			break;
		}
		tempRid = cPtrs.operator ->();
		m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, tempRid->pageNum);
		m_pfi->addUnsignedToBuffer(dataBuffer, currPtr+4, tempRid->slotNum);
		currPtr+=12; // Remember, you are skipping 4 bytes here as you will have a key in between!
	}

	// Reset the counters
	i=1;
	currPtr = 20;

	// Copy as many child keys as there are populated.
	vector<IX_KeyValue>::const_iterator cKeys;
	const IX_KeyValue *tempKV;
	for (cKeys = indexNode.childKeys.begin(); cKeys != indexNode.childKeys.end(); ++cKeys)
	{
		tempKV = cKeys.operator ->();
		if (i>IX_MAX_CHILDREN)
		{
			cout << "ERROR - Something is not quite right. You should never have more child entries than..." << IX_MAX_CHILDREN << endl;
			break;
		}
		// Copy the uData... this way you don't need to worry what the values are.
		memcpy((char *)dataBuffer+currPtr, tempKV->uData, 4);
		currPtr+=12; // Remember, you are skipping 8 more bytes here as you will have a ChildPointer in between!
	}

	//cout << "CurreRecPtr should be " << IX_NODE_SIZE << " check - " << currPtr << endl;
}

void IXPF::convertIndexLeafToDataBuffer(void* dataBuffer, const IX_Leaf &indexLeaf)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	int currPtr = 0;

	// Update the No of Keys populated.
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexLeaf.noOfKeys);
	currPtr+=4;
	// Update the Parent Pointer
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexLeaf.parentPtr.pageNum);
	currPtr+=4;
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexLeaf.parentPtr.slotNum);
	currPtr+=4;

	// At this point, currPtr = 12.
	// We will need to remember this so first we insert all Key*Data values
	// Then we insert the PrevPtr and the NextPtr

	// Copy as many KeyData as there are populated.
	vector<IX_KeyData>::const_iterator keyDataIter;
	const IX_KeyData *kdTemp;
	int i = 1;
	for (keyDataIter = indexLeaf.keyDataList.begin(); keyDataIter != indexLeaf.keyDataList.end(); ++keyDataIter)
	{
		if (i>(IX_MAX_CHILDREN))
		{
			cout << "ERROR - Something is not quite right. You should never have more child entries than..." << (IX_MAX_CHILDREN) << endl;
			break;
		}
		kdTemp = keyDataIter.operator ->();
		// Copy the uData... this way you don't need to worry what the values are.
		memcpy((char *)dataBuffer+currPtr, kdTemp->val.uData, 4);
		m_pfi->addUnsignedToBuffer(dataBuffer, currPtr+4, kdTemp->rid.pageNum);
		m_pfi->addUnsignedToBuffer(dataBuffer, currPtr+8, kdTemp->rid.slotNum);
		currPtr+=12; // Position on the next KeyData position!
	}

	// Update the Prev Pointer
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexLeaf.prevPtr.pageNum);
	currPtr+=4;
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexLeaf.prevPtr.slotNum);
	currPtr+=4;
	// Update the Next Pointer
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexLeaf.nextPtr.pageNum);
	currPtr+=4;
	m_pfi->addUnsignedToBuffer(dataBuffer, currPtr, indexLeaf.nextPtr.slotNum);
	currPtr+=4;
	//cout << "CurreRecPtr should be " << IX_LEAF_SIZE << " check - " << currPtr << endl;


}

RC IXPF::getRoot(const string indexName, IX_Node &rootNode)
{
	RC rc;
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);

	rc = -1;
	IX_Leaf iL; // This will not be used.
	bool bL;
	rc = _ixpf->getIndexEntry(upperIndexName, _ixpf->getRootRID(indexName), bL, rootNode, iL);
	if (rc != 0)
	{
		cout << "ERROR - Root Entry fetch has failed... rc=" << rc << endl;
	}

	// sanity check
	if (bL)
		cout << "ERROR - Root can never be a Leaf" << endl;

	return rc;
}


RID IXPF::getRootRID(const string indexName)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);
	RC rc;

	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;


	if (iRef.pfh.GetNumberOfPages()<1)
	{
		cout << "ERROR - No Pages in the Indexed PagedFile. Aborting." << endl;
		return rid;
	}


	// You need to allocate for the page being read and clean it up at the end.
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = m_pfi->getPage(iRef.pfh, 0, buffer);
	if (rc !=0)
	{
		cout << "ERROR - Read Page# " << 0 << " from PagedFile " << upperIndexName << " errored out. RC=" << rc << "." << endl;
		free(buffer);
		return rid;
	}

	//Since the table has atleast Page 0. Let us make sure that we do not proceed if there are no rows.
	rid.pageNum = m_pfi->getIntFromBuffer(buffer, 0);
	rid.slotNum = m_pfi->getIntFromBuffer(buffer, 4);

	free(buffer);
	return rid;

}


RC IXPF::setRootRID(const string indexName, RID rid)
{
	PF_Interface *m_pfi = PF_Interface::Instance();
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);
	RC rc;

	if (iRef.pfh.GetNumberOfPages()<1)
	{
		cout << "ERROR - No Pages in the Indexed PagedFile. Aborting." << endl;
		return -3;
	}


	// You need to allocate for the page being read and clean it up at the end.
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = m_pfi->getPage(iRef.pfh, 0, buffer);
	if (rc !=0)
	{
		cout << "ERROR - Read Page# " << 0 << " from PagedFile " << upperIndexName << " errored out. RC=" << rc << "." << endl;
		free(buffer);
		return -2;
	}

	//Since the table has atleast Page 0.
	m_pfi->addIntToBuffer(buffer, 0, rid.pageNum);
	m_pfi->addIntToBuffer(buffer, 4, rid.slotNum);

    rc = m_pfi->putPage(iRef.pfh, 0, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table. RC=" << rc << endl;
		free(buffer);
		return -1;
	}

	free(buffer);
	return 0;

}

bool IXPF::isLeaf(const RID rid)
{
	if (rid.pageNum < IX_NODE_PAGES)
		return false;
	else
		return true;
}

int IXPF::compareKeys(IndexRef iRef, IX_KeyValue key1, IX_KeyValue key2)
{
	ColRef *cRef;
	list<ColRef>::iterator it;
	it = iRef.colRefs.begin();
	cRef = it.operator ->();

	if (cRef->colType == TypeInt)
	{
		if (key1.iValue == key2.iValue)
			return 0;
		else
			if (key1.iValue > key2.iValue)
				return +1;
			else
				return -1;
	}
	else if (cRef->colType == TypeReal)
	{
		if (fabs(key1.fValue - key2.fValue) <= IX_TINY) // The keys are identical.
			return 0;
		else
			if (key1.fValue > key2.fValue)
				return +1;
			else
				return -1;
	}
	else // We can implement the logic for key comparisons on varchar here.
	{

	}
	return 0;
}

RC IXPF::addKeyDataToLeaf(IndexRef iRef, IX_Leaf &ixLeaf, IX_KeyData inputKD)
{
	IXPF *_ixpf = IXPF::Instance();
	//cout << "Before Adding, Leaf Node has " << ixLeaf.noOfKeys << " of " << ixLeaf.keyDataList.size() << " populated." << endl;
	ixLeaf.noOfKeys++; // This will allow you to call the logic even if noOfKeys - 0

	if (ixLeaf.noOfKeys > IX_MAX_CHILDREN)
	{
		cout << "ERROR - Cannot Add key to a full Leaf" << endl;
		return -2;
	}
	else // If a Leaf comes in new, likely it does not have all dummy slots populated
	{
		IX_KeyData tempKD;
		//tempKD.val.iValue=IX_NOVALUE;
		if (iRef.colRefs.front().colType == TypeInt)
			tempKD.val.iValue=IX_NOVALUE;
		else //(attType == TypeReal)
			tempKD.val.fValue=IX_HIFLOAT;

		tempKD.rid.pageNum=IX_NOVALUE;
		tempKD.rid.slotNum=IX_NOVALUE;
		for (int i=ixLeaf.keyDataList.size(); i<IX_MAX_CHILDREN; i++)
			ixLeaf.keyDataList.push_back(tempKD);
	}
	// We now have a fully populated leaf now.

	int comp;

	for (unsigned i=0; i<ixLeaf.noOfKeys; i++)
	{
		comp = _ixpf->compareKeys(iRef, inputKD.val, ixLeaf.keyDataList.at(i).val);
		if (comp == -1)
		{
			// OK YOu finally found the position where the KeyData should be inserted.
			vector<IX_KeyData>::iterator kdIter;
			kdIter = ixLeaf.keyDataList.begin()+i;
			ixLeaf.keyDataList.insert(kdIter, inputKD);
			//cout << "During the Insert, Leaf Node has " << ixLeaf.noOfKeys << " of " << ixLeaf.keyDataList.size() << " populated." << endl;
			for (int i=ixLeaf.keyDataList.size(); i>IX_MAX_CHILDREN; i--) ixLeaf.keyDataList.pop_back(); // safer route
			//ixLeaf.keyDataList.pop_back();
			break; // That is it.
		}
		else if (comp == 0)
		{
			cout << "Attempting to Insert a DUPLICATE Key Entry Value{fVal|iVal} = {" << inputKD.val.fValue << "|" << inputKD.val.iValue<< "} ... KeyData is [" << ixLeaf.keyDataList.at(i).rid.pageNum << "|" << ixLeaf.keyDataList.at(i).rid.slotNum<< "]." << endl;
			return -10;
		}
	}
	//cout << "After Adding, Leaf Node has " << ixLeaf.noOfKeys << " of " << ixLeaf.keyDataList.size() << " populated." << endl;
	return 0;

}

// This method can be used to create a setup a fresh Node entry to be inserted into the database.
// Give a blank node and this will setup the node properly with just the input from inputKD.
void IXPF::addKeyDataToNode(IndexRef iRef, IX_Node &ixNode, IX_KeyData inputKD)
{
	IXPF *_ixpf = IXPF::Instance();
	//cout << "Before Adding, the Node has " << ixNode.noOfKeys << " of " << ixNode.childKeys.size() << " childkeys and " << ixNode.childPtrs.size() << "pointers populated." << endl;
	ixNode.noOfKeys++; // This will allow you to call the logic even if noOfKeys - 0

	if (ixNode.noOfKeys > IX_MAX_CHILDREN)
	{
		cout << "ERROR - Cannot Add key to a full Node" << endl;
		return;
	}
	else // If a Node comes in new, likely it does not have all dummy slots populated
	{
		IX_KeyValue tempKV;
		//tempKV.iValue=IX_NOVALUE;
		if (iRef.colRefs.front().colType == TypeInt)
			tempKV.iValue=IX_NOVALUE;
		else //(attType == TypeReal)
			tempKV.fValue=IX_HIFLOAT;
		
		for (int i=ixNode.childKeys.size(); i<IX_MAX_CHILDREN; i++) ixNode.childKeys.push_back(tempKV);
		RID tempRid;
		tempRid.pageNum=IX_NOVALUE;
		tempRid.slotNum=IX_NOVALUE;
		for (int i=ixNode.childPtrs.size(); i<IX_MAX_CHILDREN+1; i++) ixNode.childPtrs.push_back(tempRid);
	}
	// We now have a fully populated node now.

	int comp;

	// Locate the position to insert the key
	for (unsigned i=0; i<ixNode.noOfKeys; i++)
	{
		comp = _ixpf->compareKeys(iRef, inputKD.val, ixNode.childKeys.at(i));
		if (comp == -1)
		{
			// OK YOu finally found the position where the KeyData should be inserted.
			vector<IX_KeyValue>::iterator kvIter;
			vector<RID>::iterator ridIter;
			kvIter = ixNode.childKeys.begin()+i;
			ixNode.childKeys.insert(kvIter, inputKD.val);

			// If you do insert then insert a Child Pointer also here and populate the RID that was passed.
			// In the case of the Node, the algo is such that keyValue >= is always on the right pointer.
			// Therefore, we update the right pointer here wth the incoming RID
			ridIter = ixNode.childPtrs.begin()+i+1;
			ixNode.childPtrs.insert(ridIter, inputKD.rid);

			//cout << "During the Insert, Node has " << ixNode.noOfKeys << " of " << ixNode.childKeys.size() << " childkeys and " << ixNode.childPtrs.size() << "pointers populated." << endl;
			// Remove the last elements. Remember, you would do this only if there was room on the Node anyway.
			// There must be more elements then.
			for (int i=ixNode.childKeys.size(); i>IX_MAX_CHILDREN; i--)	ixNode.childKeys.pop_back();
			for (int i=ixNode.childPtrs.size(); i>IX_MAX_CHILDREN+1; i--)	ixNode.childPtrs.pop_back();
			break; // That is it.
		}
	}
	//cout << "After Adding, Node has " << ixNode.noOfKeys << " of " << ixNode.childKeys.size() << " childkeys and " << ixNode.childPtrs.size() << "pointers populated." << endl;

}

bool IXPF::isRootNull(string indexName)
{
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);

	RID rid = _ixpf->getRootRID(upperIndexName);
	if ((rid.pageNum==0) && (rid.slotNum==0))
		return true;
	else
		return false;

}

// An new root for a null tree has high values for all Keys and just 1 pointer (left-most) only used.
// inputKD shuld be all high values.
RID IXPF::createNewRoot(IndexRef iRef, RID oldRootRid, IX_Node &ixNode, IX_KeyData inputKD)
{
	RID newRootRid;
	newRootRid.pageNum = 0;
	newRootRid.slotNum = 0;

	IXPF *_ixpf = IXPF::Instance();
	//cout << "Before Adding, the Node has " << ixNode.noOfKeys << " of " << ixNode.childKeys.size() << " childkeys and " << ixNode.childPtrs.size() << "pointers populated." << endl;
	//ixNode.noOfKeys++; // This will allow you to call the logic even if noOfKeys - 0

	if (ixNode.noOfKeys > IX_MAX_CHILDREN)
	{
		cout << "ERROR - Cannot Add key to a full Node" << endl;
		return newRootRid;
	}
	else // If a Node comes in new, likely it does not have all dummy slots populated
	{
		IX_KeyValue tempKV;
		//tempKV.iValue=IX_NOVALUE;
		if (iRef.colRefs.front().colType == TypeInt)
			tempKV.iValue=IX_NOVALUE;
		else //(attType == TypeReal)
			tempKV.fValue=IX_HIFLOAT;

		for (int i=ixNode.childKeys.size(); i<IX_MAX_CHILDREN; i++) ixNode.childKeys.push_back(tempKV);
		RID tempRid;
		tempRid.pageNum=IX_NOVALUE;
		tempRid.slotNum=IX_NOVALUE;
		for (int i=ixNode.childPtrs.size(); i<IX_MAX_CHILDREN+1; i++) ixNode.childPtrs.push_back(tempRid);
	}
	// We now have a fully populated node now.

	// This is where we populate the left-most child pointer.
	// This will send all inserts to the leaf node even if there is just 1.
	ixNode.childPtrs.at(0).pageNum = oldRootRid.pageNum;
	ixNode.childPtrs.at(0).slotNum = oldRootRid.slotNum;
	ixNode.childPtrs.at(1).pageNum = inputKD.rid.pageNum;
	ixNode.childPtrs.at(1).slotNum = inputKD.rid.slotNum;
	memcpy((char*)ixNode.childKeys.at(0).uData, (char*)inputKD.val.uData,4);
	if ((inputKD.rid.pageNum==IX_NOVALUE) && (inputKD.rid.pageNum==IX_NOVALUE))
	{
		// This is an empty root.
	}
	else
	{
		ixNode.noOfKeys=1;
	}


	//cout << "After Adding, Node has " << ixNode.noOfKeys << " of " << ixNode.childKeys.size() << " childkeys and " << ixNode.childPtrs.size() << "pointers populated." << endl;

	void *dataBuffer;

	// insert the Node into the database.
	dataBuffer = malloc(IX_NODE_SIZE);
    // Initialize the page data with blanks
    for(unsigned i = 0; i < IX_NODE_SIZE; i++)   *((char *)dataBuffer+i) = ' ';
    //cout << endl << "Converting Node to Buffer: " << endl;
	_ixpf->convertIndexNodeToDataBuffer(dataBuffer, ixNode);
	//cout << endl;

    cout << "   ---->Inserting Node to File: " << endl;
	_ixpf->insertIndexEntry(false, iRef.indexName, dataBuffer, newRootRid);
	cout << "   ---->NEW ROOT Node Inserted at PageNumber=" << newRootRid.pageNum << "SlotNumber=" << newRootRid.slotNum << endl;
	//ixNode.print();

	free(dataBuffer);

	return newRootRid;


	/*
	IXPF *_ixpf = IXPF::Instance();
	void *dataBuffer;

	_ixpf->addKeyDataToNode(iRef, ixNode, inputKD);
	// Make sure you update pointer P1 (not P0)
	ixNode.childPtrs.at(0).pageNum = IX_NOVALUE;
	ixNode.childPtrs.at(0).slotNum = IX_NOVALUE;
	ixNode.childPtrs.at(1).pageNum = oldRootRid.pageNum;
	ixNode.childPtrs.at(1).slotNum = oldRootRid.slotNum;

	// insert the Node into the database.
	dataBuffer = malloc(IX_NODE_SIZE);
    // Initialize the page data with blanks
    for(unsigned i = 0; i < IX_NODE_SIZE; i++)   *((char *)dataBuffer+i) = ' ';
    cout << endl << "Converting Node to Buffer: " << endl;
	_ixpf->convertIndexNodeToDataBuffer(dataBuffer, ixNode);
	ixNode.print();
	cout << endl;

	RID newRootRid;

    cout << "Inserting Node to File: " << endl;
	_ixpf->insertIndexEntry(false, iRef.indexName, dataBuffer, newRootRid);
	cout << "NEW ROOT Node Inserted at PageNumber=" << newRootRid.pageNum << "SlotNumber=" << newRootRid.slotNum << endl;
	free(dataBuffer);

	return newRootRid;

	 */
}

// Inserts entry into a IndexTable (gets the root from the indexName somehow)
// newChildPtr will be returned with non-null only if the child is split.
RC IXPF::insertKey(const string indexName, RID currChildPtr, const IX_KeyData inputKD, IX_KeyData &newChildEntry)
{
	RC rc;
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);

	IX_Node iN;
	IX_Leaf iL;
	bool tempBool;
	cout << "Calling INSERT KEY with RID= " << currChildPtr.pageNum << "|" << currChildPtr.slotNum << endl;

	//if RootRID is 0,0 then you will need to do some special processing.
	if (_ixpf->isRootNull(upperIndexName))
	{
		// create a Leaf wth Key*
		IX_Leaf ixLeaf;
		void *dataBuffer;
	    RID leafRid;
	    RID nodeRid;
		rc = _ixpf->addKeyDataToLeaf(iRef, ixLeaf, inputKD); // returns -2(Node Full) or -100(Duplicate Key On Insert)
		if (rc != 0)
		{
			cout << "ERROR - Unable to insert data into the Index." << endl;
			return rc;
		}

		cout << "    THIS IS THE FIRST ENTRY INTO THE INDEX FILE...." << endl;
		// insert the Leaf into the database.
		dataBuffer = malloc(IX_LEAF_SIZE);
	    // Initialize the page data with blanks
	    for(unsigned i = 0; i < IX_LEAF_SIZE; i++)
	    {
	        *((char *)dataBuffer+i) = ' ';
	    }
	    //cout << endl << "Converting Leaf to Buffer: " << endl;
		_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeaf);
		//ixLeaf.print();
		//cout << endl;

	    //cout << "Inserting Leaf to File: " << endl;
		_ixpf->insertIndexEntry(true, upperIndexName, dataBuffer, leafRid);
		cout << "Leaf Inserted at PageNumber=" << leafRid.pageNum << "SlotNumber=" << leafRid.slotNum << endl;
		free(dataBuffer);

		//CREATE  ANEW ROOT NODE and SET ROOT
		IX_Node ixNode;
		IX_KeyData dummyKD;
		dummyKD.rid.pageNum=IX_NOVALUE;
		dummyKD.rid.slotNum=IX_NOVALUE;
		//dummyKD.val.iValue=IX_NOVALUE;
		if (iRef.colRefs.front().colType == TypeInt)
			dummyKD.val.iValue=IX_NOVALUE;
		else //(attType == TypeReal)
			dummyKD.val.fValue=IX_HIFLOAT;

		nodeRid = _ixpf->createNewRoot(iRef, leafRid, ixNode, dummyKD);
		ixNode.print();

		_ixpf->setRootRID(indexName, nodeRid);


		// Update the ParentPtr on the Leaf
		ixLeaf.parentPtr.pageNum = nodeRid.pageNum;
		ixLeaf.parentPtr.slotNum = nodeRid.slotNum;
		dataBuffer = malloc(IX_LEAF_SIZE);
	    // Initialize the page data with blanks
	    for(unsigned i = 0; i < IX_LEAF_SIZE; i++)	        *((char *)dataBuffer+i) = ' ';
	    //cout << endl << "Converting Leaf to Buffer: " << endl;
		_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeaf);
		//ixLeaf.print();
		//cout << endl;

		//cout << "Updating Leaf to File: " << endl;
		_ixpf->updateIndexEntry(upperIndexName, dataBuffer, leafRid);
		cout << "Leaf Updated at PageNumber=" << leafRid.pageNum << "SlotNumber=" << leafRid.slotNum << endl;
		ixLeaf.print();
		free(dataBuffer);

		return 0;

	}

	if (!_ixpf->isLeaf(currChildPtr)) // if *nodepointer is a non-leaf node, say N
	{
		// COW BOOK - find i such that K(i) <= entry's key value < K(i+1); // choose subtree
		rc = _ixpf->getIndexEntry(upperIndexName, currChildPtr, tempBool, iN, iL);
		if (rc != 0)
		{
			cout << "ERROR - getIndexEntry call inside insertKey [" << upperIndexName << "] RID=<" << currChildPtr.pageNum << "-" << currChildPtr.slotNum << ">." << endl;
			return -11;
		}

		// You will need to check just iN.noOfKeys

		int comp = -99;
		bool keyMatched = false;

		//Check where the key matches.
		comp = _ixpf->compareKeys(iRef, inputKD.val, iN.childKeys.at(0));
		if (comp == -1)
		{
			// COW BOOK - recursively, insert entry
			cout << "****Incoming Key is Less than the lowest key on NODE." << endl;
			rc = insertKey(indexName, iN.childPtrs.at(0), inputKD, newChildEntry);
			if (rc<0) return rc;
			keyMatched = true;
		}
		else
		{
			unsigned i = 0;
			// COW BOOK - choose subtree
			for (i=0; (iN.noOfKeys>0) && (i<iN.noOfKeys-1); i++)
			{
				int comp1;
				int comp2;
				comp1 = _ixpf->compareKeys(iRef, iN.childKeys.at(i), inputKD.val);
				comp2 = _ixpf->compareKeys(iRef, inputKD.val, iN.childKeys.at(i+1));
				if (((comp1 == 0) || (comp1 == -1)) && ((comp2 == -1)))
				{
					// COW BOOK - recursively, insert entry
					cout << "****Incoming Key is between " << i << " and "<< i+1 <<" on NODE." << endl;
					rc = insertKey(indexName, iN.childPtrs.at(i+1), inputKD, newChildEntry);
					if (rc<0) return rc;
					keyMatched = true;
					break;
				}
			}
			if (!keyMatched)
			{
				//comp = _ixpf->compareKeys(iRef, inputKD.val, iN.childKeys.at(IX_MAX_CHILDREN-1));
				//if (comp >= 1)
				{
					// COW BOOK - recursively, insert entry
					cout << "****Incoming Key is greater than KeyValue at# " << iN.childKeys.size() <<" on NODE." << endl;
					rc = insertKey(indexName, iN.childPtrs.at(iN.noOfKeys), inputKD, newChildEntry);
					if (rc<0) return rc;
					keyMatched = true;
				}
			}
		}
		// The key should have matched one of the keys in the node. If not, this is an error.

		if (!keyMatched)
		{
			cout << "!!!!  ERROR - This Code should never be reached!!!!" << endl;
			cout << "ERROR - Key match has failed to match any key. Rebuild Index recommended." << endl;
			return -3;
		}

		if ((newChildEntry.rid.pageNum==0) && (newChildEntry.rid.slotNum==0))
		{
			// COW BOOK - usual case; didn't split child
			return 0;
		}
		else // COW BOOK - we split child, must insert *newchildentry in N
		{
			cout << "   ----> Updating Node Tree to add Value {intVal|floatVal}= {" << newChildEntry.val.iValue << "|" << newChildEntry.val.fValue << "}"<< endl;
			if (iN.noOfKeys < IX_MAX_CHILDREN) // COW BOOK - if N has space, usual case
			{
				// COW BOOK - put *newChildEntry on it, set newChildEntry to null, return
				_ixpf->addKeyDataToNode(iRef, iN, newChildEntry);
				void *nodeData = malloc(IX_NODE_SIZE);
				//RID leafRid;
				_ixpf->convertIndexNodeToDataBuffer(nodeData, iN);
				rc = _ixpf->updateIndexEntry(indexName, nodeData, currChildPtr);
				if (rc != 0)
				{
					cout << "ERROR - Update Node Data has Failed." << endl;
					rc = -4;
					free(nodeData);
					return rc;
				}
				free(nodeData);

				newChildEntry.rid.pageNum=0;
				newChildEntry.rid.slotNum=0;
				newChildEntry.val.iValue=0;
				iN.print();
				return 0;
			}
			else
			{
				// COW BOOK - split N: // 2d+1 key values and 2d+2 pointers
				// first d key values and d+1 pointers stay,
				// last d keys and d+1 pointers move to new Node N2
				_ixpf->splitNode(iRef, currChildPtr, iN, newChildEntry);

				cout << " New Node added at: <" << newChildEntry.rid.pageNum << "|" << newChildEntry.rid.slotNum << ">" << endl;

				// COW BOOK - if N is the root, the root was just split
				// create new node with <pointer to *N, *newChildEntry>;
				// make the tree's root-node pointer point to the new node.

				IX_Node ixNode;
				RID currRootRid = _ixpf->getRootRID(iRef.indexName);
				if ((currChildPtr.pageNum == currRootRid.pageNum) && (currChildPtr.slotNum == currRootRid.slotNum) )
				{
					RID newRootRid;

					newRootRid = _ixpf->createNewRoot(iRef, currChildPtr, ixNode, newChildEntry);
					_ixpf->setRootRID(indexName, newRootRid);

					IX_Node tempRootNode;
					_ixpf->getRoot(iRef.indexName,tempRootNode);
					cout << "   ----> NEW ROOT contains...." << endl;
					tempRootNode.print();
				}

				return 0;
			}
		}
	}

	if (_ixpf->isLeaf(currChildPtr)) // if *nodepointer is a leaf node, say L
	{
		rc = _ixpf->getIndexEntry(upperIndexName, currChildPtr, tempBool, iN, iL);
		if (rc!=0)
		{
			cout << "Index Leaf Entry not found at [" << currChildPtr.pageNum << "|" << currChildPtr.slotNum << "]"  << endl;
			return -5;
		}

		if (iL.noOfKeys < IX_MAX_CHILDREN) // COW BOOK - if L has space, usual case
		{
			// COW BOOK - put entry on it, set newchildentry to null, and return;
			rc = _ixpf->addKeyDataToLeaf(iRef, iL, inputKD);  // returns -2(Node Full) or -100(Duplicate Key On Insert)
			if (rc != 0)
			{
				cout << "ERROR - Unable to insert Leaf data into the Index." << endl;
				return rc;
			}
			void *leafData = malloc(IX_LEAF_SIZE);
			//RID leafRid;
			_ixpf->convertIndexLeafToDataBuffer(leafData, iL);
			rc = _ixpf->updateIndexEntry(indexName, leafData, currChildPtr);
			if (rc != 0)
			{
				free(leafData);
				cout << "Index Leaf Entry Update Failed at [" << currChildPtr.pageNum << "|" << currChildPtr.slotNum << "]"  << endl;
				return -6;
			}
			cout << "   ----> LEAF ENTRY updated here...RID=<" << currChildPtr.pageNum << "|" << currChildPtr.slotNum << ">" << endl;
			iL.print();

			newChildEntry.rid.pageNum=0;
			newChildEntry.rid.pageNum=0;
			newChildEntry.val.iValue=0;
			return 0;
		}
		else // COW BOOK - once in a while, the leaf is full
		{
			// COW BOOK - Split L:
			rc = _ixpf->splitLeaf(iRef, currChildPtr, iL, inputKD, newChildEntry);
			if (rc != 0)
				return rc;
			return 0;
		}


	}


	m_indexTableName = indexName;
	return 0;
}

// This is called only when the bCollapse flag set at the Leaf level gets propagated.
// Note that it gets reset here under some conditions.
RC IXPF::collapseNode(const string indexName, RID nodePtr, IX_Node ixNode, RID &collapseRid)
{
	//RC rc;
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);


	// cout << "   ----> Entered CollapseNode function." << endl; --Pramit
	if (ixNode.noOfKeys>1)
	{
		// Go through each of the Node Pointers...
		// If the pointer matches the collapseRid then delete the corresponding Key and Pointer
		// Then add a new Key and Pointer with High Values at the end.
		//cout << "   ----> Removing Node Pointer and corresponding Node Key." << endl; -- Pramit

		bool bMatch = false;
		int iAtPtr = -1;

		for (unsigned i=0; i<ixNode.noOfKeys; i++)
		{
			if ((collapseRid.pageNum==ixNode.childPtrs.at(i).pageNum)
				&& (collapseRid.slotNum==ixNode.childPtrs.at(i).slotNum)	)
			{
				// OK The i-th Pointer is deleted then let us delete the i-th key
				iAtPtr = i;
				bMatch=true;

				break;
			}
		}

		if(!bMatch)
		{
			if ((collapseRid.pageNum==ixNode.childPtrs.at(IX_MAX_CHILDREN).pageNum)
				&& (collapseRid.slotNum==ixNode.childPtrs.at(IX_MAX_CHILDREN).slotNum)	)
			{
				bMatch=true;
				iAtPtr = IX_MAX_CHILDREN;
			}
			else
			{
				bMatch=false;
				//cout << " Could not locate the CollapseRid at all." << endl; -- Pramit
			}
		}

		// Actual collapse happens here
		if(bMatch)
		{
			if (iAtPtr==IX_MAX_CHILDREN)
				ixNode.childKeys.erase(ixNode.childKeys.begin()+iAtPtr-1); // Last Key
			else
				ixNode.childKeys.erase(ixNode.childKeys.begin()+iAtPtr);

			ixNode.childPtrs.erase(ixNode.childPtrs.begin()+iAtPtr);
			ixNode.noOfKeys--; // Safe here as the value has to be >1 anyway

			IX_KeyData tempKD;
			//tempKD.val.iValue=IX_NOVALUE;
			if (iRef.colRefs.front().colType == TypeInt)
				tempKD.val.iValue=IX_NOVALUE;
			else //(attType == TypeReal)
				tempKD.val.fValue=IX_HIFLOAT;

			tempKD.rid.pageNum=IX_NOVALUE;
			tempKD.rid.slotNum=IX_NOVALUE;

			ixNode.childKeys.push_back(tempKD.val);
			ixNode.childPtrs.push_back(tempKD.rid);

			// Update the database
		   // cout << "   --->Updating NODE entry to File after deleting: " << endl; --Pramit
			void *dataBuffer = malloc(IX_NODE_SIZE);
			_ixpf->convertIndexNodeToDataBuffer(dataBuffer, ixNode);
			_ixpf->updateIndexEntry(iRef.indexName, dataBuffer, nodePtr);
			cout << "   -->NODE Entry Updated AFTER Delete at PageNumber=" << nodePtr.pageNum << "SlotNumber=" << nodePtr.slotNum << endl;
			ixNode.print();

			collapseRid.pageNum = 0;
			collapseRid.slotNum = 0;

		}




	}
	else
	{
		// Simply ignore what was set in collapseRid.
		collapseRid.pageNum=0;
		collapseRid.slotNum=0;
		//cout << "   ----> Returned from CollapseNode." << endl; -- Pramit
		return 0;
	}






	return 0;

}



RC IXPF::deleteKeyDataFromLeaf(const string indexName, unsigned parentKeyCount, RID currPtr, const IX_KeyData inputKD, IX_Leaf ixLeaf, RID &collapseRid)
{
	//RC rc;
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);

	IX_Node iN;
	IX_Leaf iL;
	int comp;

	RID prevPtr;
	RID nextPtr;

	bool bCollapse=false;
	if ((collapseRid.pageNum==0) && (collapseRid.slotNum==0))
		bCollapse = false;
	else
		bCollapse = true;


	for (unsigned i=0; i<ixLeaf.noOfKeys; i++)
	{
		comp = -99;
		comp = _ixpf->compareKeys(iRef, inputKD.val, ixLeaf.keyDataList.at(i).val);
		if (comp == 0)
		{
			// OK You finally found the position where the KeyData should be deleted.

			// Just be sure that the RID in the KeyData also matches.

			if ((inputKD.rid.pageNum == ixLeaf.keyDataList.at(i).rid.pageNum) &&
					(inputKD.rid.slotNum == ixLeaf.keyDataList.at(i).rid.slotNum))
			{
				cout << "   ---->LEAF Entry before DELETE at PageNumber=" << currPtr.pageNum << "SlotNumber=" << currPtr.slotNum << endl;
			    ixLeaf.print();

				ixLeaf.keyDataList.erase(ixLeaf.keyDataList.begin()+i);
				if (ixLeaf.noOfKeys>0) // <-- This is to prevent underflow!
					ixLeaf.noOfKeys--; // You removed the KDentry.
				bCollapse = false;

				cout << "   ---->Deleted KeyData from Leaf. No other entries are affected." << endl;
				collapseRid.pageNum = 0;
				collapseRid.slotNum = 0;
				bCollapse = false;
				IX_KeyData tempKD;
				// Just add a dummy entry at the back
				//tempKD.val.iValue=IX_NOVALUE;
				if (iRef.colRefs.front().colType == TypeInt)
					tempKD.val.iValue=IX_NOVALUE;
				else //(attType == TypeReal)
					tempKD.val.fValue=IX_HIFLOAT;

				tempKD.rid.pageNum=IX_NOVALUE;
				tempKD.rid.slotNum=IX_NOVALUE;
				ixLeaf.keyDataList.push_back(tempKD);

				// Save the entry to the database.
				void *dataBuffer = malloc(IX_LEAF_SIZE);
				_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeaf);
				_ixpf->updateIndexEntry(iRef.indexName, dataBuffer, currPtr);
				cout << "   ---->existing LEAF Entry updated at PageNumber=" << currPtr.pageNum << "SlotNumber=" << currPtr.slotNum << endl;
				free(dataBuffer);
				ixLeaf.print();

			}
			break; // As soon as you find a match, process it and leave.
		}
	}

	if  (parentKeyCount==1)
	{
		// Deleting a Node with just 1 key populated can be very tricky.
		// Node entries will need to be redistributed.
		// Therefore, just don't delete the leaf. You will technically have an empty leaf node.
		// This is our implementation.

		collapseRid.pageNum = 0;
		collapseRid.slotNum = 0;

		bCollapse = false; // Do not propagate the Collapse flag.
	}

	// At this point, the index LEAF entry has been deleted. Just one bit left...
	// You need to update the Prev and Next sibling's sibling pointers.
	// Since you are falling through, do this only if bCollapse is true.

	if (bCollapse)
	{
		IX_Node iN;
		IX_Leaf ixLeafP;
		bool bTemp=true;
		// Update sibling pointers.

		if (_ixpf->getIndexEntry(iRef.indexName, prevPtr, bTemp, iN, ixLeafP) == 0)
		{
			if ((prevPtr.pageNum == 0) && (prevPtr.slotNum == 0) )
			{
				cout << "   ---->existing LEAF PREVIOUS Entry was a NULL Pointer" << endl;
			}
			else
			{
				ixLeafP.nextPtr.pageNum = nextPtr.pageNum;
				ixLeafP.nextPtr.slotNum = nextPtr.slotNum;
				void *dataBuffer = malloc(IX_LEAF_SIZE);
				_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeafP);
				_ixpf->updateIndexEntry(iRef.indexName, dataBuffer, prevPtr);
				cout << "   ---->existing LEAF PREVIOUS Entry updated at PageNumber=" << prevPtr.pageNum << "SlotNumber=" << prevPtr.slotNum << endl;
				free(dataBuffer);
				ixLeafP.print();
			}
		}

		IX_Leaf ixLeafN;
		if (_ixpf->getIndexEntry(iRef.indexName, nextPtr, bTemp, iN, ixLeafN) == 0)
		{
			if ((nextPtr.pageNum == 0) && (nextPtr.slotNum == 0) )
			{
				cout << "   ---->existing LEAF NEXT Entry was a NULL Pointer" << endl;
			}
			else
			{
				ixLeafN.prevPtr.pageNum = prevPtr.pageNum;
				ixLeafN.prevPtr.slotNum = prevPtr.slotNum;
				void *dataBuffer = malloc(IX_LEAF_SIZE);
				_ixpf->convertIndexLeafToDataBuffer(dataBuffer, ixLeafN);
				_ixpf->updateIndexEntry(iRef.indexName, dataBuffer, nextPtr);
				cout << "   ---->existing LEAF NEXT Entry updated at PageNumber=" << nextPtr.pageNum << "SlotNumber=" << nextPtr.slotNum << endl;
				free(dataBuffer);
				ixLeafN.print();
			}
		}
	}
	// Pass back bCollapse so that necessary action can be completed here.

	return 0;
}

// By default, bCollapse should be false. However, in the recursive loops, some invocations can result in bCollapse being set.
// This will be the first place it will be set. All others will use it and pass it.
// At some levels of the node, under most conditions, it will be reset. Under some conditions, it can be passed back to the call for handling at the higher levels.
RC IXPF::deleteKey(const string indexName, unsigned parentKeyCount, RID currChildPtr, const IX_KeyData inputKD, RID &collapseRid)
{
	RC rc;
	IXPF *_ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(indexName);
	IndexRef iRef = _ixpf->getIndexRef(upperIndexName);

	IX_Node iN;
	IX_Leaf iL;
	bool tempBool;
	//if ((collapseRid.pageNum==0) && (collapseRid.slotNum==0))


	//cout << "Calling DELETE KEY with RID= " << currChildPtr.pageNum << "|" << currChildPtr.slotNum << endl; --Pramit

	//if RootRID is 0,0 then you will need to do some special processing.
	if (_ixpf->isRootNull(upperIndexName))
	{
		cout << "********    The Index File Is Empty. No records to delete. ******* RC=1" << endl;
		return -1;
	}

	if (!_ixpf->isLeaf(currChildPtr)) // if *nodepointer is a non-leaf node, say N
	{
		// COW BOOK - find i such that K(i) <= entry's key value < K(i+1); // choose subtree
		rc = _ixpf->getIndexEntry(upperIndexName, currChildPtr, tempBool, iN, iL);
		if (rc != 0)
		{
			cout << "ERROR - getIndexEntry call inside deleteKey [" << upperIndexName << "] RID=<" << currChildPtr.pageNum << "-" << currChildPtr.slotNum << ">." << endl;
			return -2;
		}

		// You will need to check just iN.noOfKeys

		int comp = -99;
		bool keyMatched = false;

		//Check where the key matches.
		comp = _ixpf->compareKeys(iRef, inputKD.val, iN.childKeys.at(0));
		if (comp == -1)
		{
			// COW BOOK - recursively, delete entry
			cout << "****Incoming Key is Less than the lowest key on NODE." << endl;
			rc = deleteKey(upperIndexName, iN.noOfKeys, iN.childPtrs.at(0), inputKD, collapseRid);
			if (((collapseRid.pageNum==0) && (collapseRid.slotNum==0)) && (rc==0))
			{
				//cout << " Key 0 and Pointer 0 on the Node should be removed." << endl; -- Pramit
				//cout << "CALL CRUNCH NODE ENTRY FROM LEAF HERE..(set bCollapse)." << endl;
				rc = _ixpf->collapseNode(upperIndexName, currChildPtr, iN, collapseRid);
			}
			keyMatched = true;
			return rc;
		}
		else
		{
			unsigned i = 0;
			// COW BOOK - choose subtree
			for (i=0; i<iN.noOfKeys-1; i++)
			{
				int comp1;
				int comp2;
				comp1 = _ixpf->compareKeys(iRef, iN.childKeys.at(i), inputKD.val);
				comp2 = _ixpf->compareKeys(iRef, inputKD.val, iN.childKeys.at(i+1));
				if (((comp1 == 0) || (comp1 == -1)) && ((comp2 == -1)))
				{
					// COW BOOK - recursively, insert entry
					cout << "****Incoming Key is between " << i << " and "<< i+1 <<" on NODE." << endl;
					rc = deleteKey(upperIndexName, iN.noOfKeys, iN.childPtrs.at(i+1), inputKD, collapseRid);
					if (((collapseRid.pageNum==0) && (collapseRid.slotNum==0)) && (rc==0))
					{
						//cout << " Key " << i << " and Pointer " << i+1 << " on the Node should be removed." << endl; -- Pramit
						//cout << "CALL CRUNCH NODE ENTRY FROM LEAF HERE..(set bCollapse)." << endl;
						rc = _ixpf->collapseNode(upperIndexName, currChildPtr, iN, collapseRid);
					}
					keyMatched = true;
					return rc; // or return the result code from the previous call
//					break;
				}
			}
			if (!keyMatched)
			{
				//if (comp >= 1)
				{
					// COW BOOK - recursively, insert entry
					cout << "****Incoming Key is greater than KeyValue at# " << iN.childKeys.size() <<" on NODE." << endl;
					rc = deleteKey(upperIndexName, iN.noOfKeys, iN.childPtrs.at(iN.noOfKeys), inputKD, collapseRid);
					if (((collapseRid.pageNum==0) && (collapseRid.slotNum==0)) && (rc==0))
					{
						//cout << " Key " << IX_MAX_CHILDREN << " and Pointer " << IX_MAX_CHILDREN+1 << " on the Node should be removed." << endl; --Pramit
						//cout << "CALL CRUNCH NODE ENTRY FROM LEAF HERE..(set bCollapse)." << endl;
						rc = _ixpf->collapseNode(upperIndexName, currChildPtr, iN, collapseRid);
					}
					keyMatched = true;
					return rc; // or return the result code from the previous call
				}
			}
		}
		// The key should have matched one of the keys in the node. If not, this is an error.

		if (!keyMatched)
		{
			cout << "!!!!  ERROR - This Code should never be reached!!!!" << endl;
			cout << "ERROR - Key match has failed to match any key. Rebuild Index recommended." << endl;
			return -3;
		}
	}

	if (_ixpf->isLeaf(currChildPtr)) // if *nodepointer is a leaf node, say L
	{
		rc = _ixpf->getIndexEntry(upperIndexName, currChildPtr, tempBool, iN, iL);

		if (rc != 0)
		{
			cout << " GetIndex Entry returned error. Did not find entry at: " << currChildPtr.pageNum << "|" << currChildPtr.slotNum << endl;
			return -4;
		}
		else
		{
			// iL should contain the entire leaf.

			// Check that the inputKD's RID matches the data value that you are planning to delete.
			// We are not allowing duplicates entries in this installation.
			int compK;
			bool bMatch=false;

			for (unsigned i=0; i<iL.keyDataList.size(); i++)
			{
				compK = _ixpf->compareKeys(iRef, iL.keyDataList.at(i).val, inputKD.val);
				if (compK == 0) // ie. the key values match. Now check if the RID also matches.
				{
					bMatch=true;
					if ((inputKD.rid.pageNum == iL.keyDataList.at(i).rid.pageNum) &&
							(inputKD.rid.slotNum == iL.keyDataList.at(i).rid.slotNum))
					{
						// Yes, this is a perfect match to be deleted.
						//IX_Leaf ixLeaf;
						cout << " Deleting Leaf entry for Key={" << inputKD.val.fValue << "|" << inputKD.val.iValue << "} at: " << currChildPtr.pageNum << "|" << currChildPtr.slotNum << endl;
						rc = _ixpf->deleteKeyDataFromLeaf(upperIndexName, parentKeyCount, currChildPtr, inputKD, iL, collapseRid);
						return rc; // or return the result code from the previous call
					}
					else
					{
						// Yes, The Key Value ha matched but not the RID.
						cout << " Deleting Leaf entry for Key={" << inputKD.val.fValue << "|" << inputKD.val.iValue << "} at: " << currChildPtr.pageNum << "|" << currChildPtr.slotNum << endl;
						cout << "--- KEY VALUE has matched but the RID MUST also MATCH FOR A DELETE." << endl;
						return 1;
					}

				}
			}
			if (!bMatch)
			{
				// Neither the Key Value nor the RID has matched.
				cout << " Unable to delete Leaf entry for Key ={" << inputKD.val.fValue << "|" << inputKD.val.iValue << "} at: " << currChildPtr.pageNum << "|" << currChildPtr.slotNum << endl;
				//cout << "--- KEY VALUE and RID both MUST MATCH FOR A DELETE." << endl;
				return 2;
			}


		}


	}



	return 0;
}


RID IXPF::find(const string indexname, IX_KeyValue Key)
{
	m_indexTableName = indexname;
	RID rootInfo = getRootRID(m_indexTableName);
	return tree_search(rootInfo, Key);
}


RID IXPF::tree_search(RID &rid, IX_KeyValue key)
{
	//int searchForkey = *(int *)key;
	RC rc;
	RID childPtr;
	childPtr.pageNum =0;
	childPtr.slotNum =0;
	RID resultRID;
	resultRID.pageNum = 0;
	resultRID.slotNum = 0;
	bool found = false;
	bool bLeafEntry = false;;
	IndexRef iRef = getIndexRef(m_indexTableName);
	IX_Node indexNode;
	IX_Leaf indexLeaf;
	int comp= -99;

	// get the Node value for the RID of the root
	if(isLeaf(rid))
	{
		return rid;

	}
	else  //Case k<k1
	{
		rc = getIndexEntry(m_indexTableName, rid, bLeafEntry, indexNode, indexLeaf);
		vector<IX_KeyValue>::const_iterator nodeItr;
		vector<RID>::const_iterator ridItr;
		nodeItr = indexNode.childKeys.begin();
		cout << "Checking:" << indexNode.childKeys.at(0).iValue<< endl;
		if(key.iValue != 0)
			comp = compareKeys(iRef, key, indexNode.childKeys.at(0));
		else
			comp = -1; //NULL passed as value
		if(comp ==-1)  // case when k<K1
		{
			found = true;
			childPtr.pageNum = indexNode.childPtrs.at(0).pageNum;
			childPtr.slotNum = indexNode.childPtrs.at(0).slotNum;
			//ridItr = indexNode.childPtrs.begin();
			return tree_search(childPtr, key);

		}
		else // case k> k(m) and k(i)<= k < k(i+1) m # max entries
		{

			for (int i=0; i<IX_MAX_CHILDREN-1; i++)
			{
				int comp1;
				int comp2;
				comp1 = compareKeys(iRef, key, indexNode.childKeys.at(i));
				comp2 = compareKeys(iRef, key, indexNode.childKeys.at(i+1));
				if (((comp1 == 0) || (comp1 == 1)) && ((comp2 == -1)))
				{
					childPtr.pageNum = indexNode.childPtrs.at(i+1).pageNum;
					childPtr.slotNum = indexNode.childPtrs.at(i+1).slotNum;
					found = true;
					return tree_search(childPtr, key);
				}
			}

			if(!found)
			{
				childPtr.pageNum =   indexNode.childPtrs.at(IX_MAX_CHILDREN).pageNum; //indexNode.childPtrs.end()->pageNum; //
				childPtr.slotNum = indexNode.childPtrs.at(IX_MAX_CHILDREN).slotNum; //indexNode.childPtrs.at(IX_MAX_CHILDREN).slotNum; //indexNode.childPtrs.end()->slotNum
				found = true;
				return tree_search(childPtr, key);
			}

		}

	}

	return resultRID;
}
