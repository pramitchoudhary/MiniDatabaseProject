/*
 * rm.cc
 *
 *  Created on: Oct 16, 2011
 *	CS222 - Principles of Database Systems
 *		Project 1 - Record Manager Interface (RM)
 *		Coded By: Dexter (#24)
 *
 *	RM - The Record Management infrastructure provides the basic File Access Methods to the data storage.
 *	It supports many end-user operations directly via APIs and many other functions that can be
 *	easily combined to perform complete query or update operations.
 *
 *	TheRM implementation is a singleton class in the application.
 *
 */

#include "rm.h"
#include <fstream>
#include <stdio.h>
#include <iostream>
#include <cassert>
#include <cassert>
#include <stdlib.h>
#include <string.h>
#include <cctype>       // std::toupper
#include <vector>

// defining Constants
const unsigned SLOTSPACE = 8; // stores <no. of entries, freeSpacePtr>
const unsigned SLOTSIZE = 8; // accounts for <recordoffsetposition,recorsSize>

using namespace std;


RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

RM::RM()
{
	m_pfi = PF_Interface::Instance();
}

RM::~RM()
{
}

/* This function returns all the column attributes defined for a table as stored in the Catalog
 * The columns belonging to the table as provided in tableName are queried and
 * returned in a vector of objects of the type Attribute
 * This method takes the tableName as input
 * The vector of Attribute class objects is the output.
 */
RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{

	Catalog *cat = Catalog::Instance(); // This opens the catalog also if necessary
	list<ColRef> colRefs;

	cat->scanCatalog(_strToUpper(tableName), colRefs);

	list<ColRef>::iterator itColRef; // get the iterator for ColRef

	ColRef *cr;
	Attribute att; //This is the format that we are required to populate for the API.

	for (itColRef = colRefs.begin(); itColRef != colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();
		if (cr->colDelInd==0)
		{
			att.name.assign(cr->colName);
			att.type = cr->colType;
			att.length = cr->colLength;
			cout << cr->colName << " | " << cr->colPosition << " | " << cr->colType << " | " << cr->colLength << " | " << cr->colDelInd << " | " << cr->recordOffset << endl;
			attrs.push_back(att);
		}
	}

	return 0;

}

/*
 * This method provides the ability to create a table with attributes specified in a vector
 * The Attribute details are specified as elements of the vector.
 * Both the parameters are input.
 */
RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{

	// You get the input in the form of a vector of attributes.
	// You need to create the column entries in the Catalog table
	// Convert the vector information into a data record and call insertCatalogTuple()
	RC rc;
	PF_Interface *pfi = PF_Interface::Instance();
	string upperTableName = _strToUpper(tableName);
	Catalog *cat = Catalog::Instance(); // This opens the catalog also

	if (pfi->isPagedFileCreated(upperTableName))
	{
		cout << "ERROR - Table exists on the disk already. Cannot create again." << endl;
		return -1;
	}

	void *data = malloc(400); // Give enough room for TableName, ColumnName and the other data items

	vector<Attribute>::const_iterator itAttrs; // get the iterator for Attributes
	const Attribute *attr;

	unsigned i = 0;
	unsigned fldPtr = 0; // FieldPointer
	unsigned currRecordEnd = 36; // CurrentRecordEnd is the end of the fixed portion of the record.

	for (itAttrs = attrs.begin(); itAttrs != attrs.end(); ++itAttrs)
	{
		i++;
		attr = itAttrs.operator ->();
		string upperColumnName = _strToUpper(attr->name);

		cout << attr->name << " | " << attr->type << " | " << attr->length << endl;
	    pfi->addIntToBuffer(data, fldPtr, 6);//0-3 - For the Field Count
	    fldPtr+=4;

	    pfi->addIntToBuffer(data, fldPtr, currRecordEnd);//4-7 - For the column TABLENAME
	    fldPtr+=4;
	    pfi->addIntToBuffer(data, fldPtr, upperTableName.length());//8-11 - Size of the Varchar TABLENAME
	    fldPtr+=4;
	    pfi->addVarcharToBuffer((char *)data, currRecordEnd, (void *)upperTableName.c_str(), upperTableName.length());// In the Variable Length portion
	    currRecordEnd += upperTableName.length(); // Next spot where you can add Varchar data

	    pfi->addIntToBuffer(data, fldPtr, currRecordEnd);//12-15 - For the column COLUMNNAME
	    fldPtr+=4;
	    pfi->addIntToBuffer(data, fldPtr, upperColumnName.length());//16-19 - Size of the Varchar COLUMNNAME
	    fldPtr+=4;
	    pfi->addVarcharToBuffer((char *)data, currRecordEnd, (void *)upperColumnName.c_str(), upperColumnName.length());//In the Variable Length portion
	    currRecordEnd += upperColumnName.length(); // Next spot where you can add Varchar data

	    pfi->addIntToBuffer(data, fldPtr, i);//20-23 - For the POSITION
	    fldPtr+=4;

	    pfi->addIntToBuffer(data, fldPtr, attr->type);//24-27 - For the TYPE
	    fldPtr+=4;

	    int l;
	    if (attr->type != TypeVarChar) l=4; else l=attr->length;
	    pfi->addIntToBuffer(data, fldPtr, l);//28-31 - For the LENGTH
	    fldPtr+=4;

	    pfi->addIntToBuffer(data, fldPtr, 0);//32-35 - For the Deleted Ind column
	    fldPtr+=4;

	    rc = cat->addCatalogTuple(data);
	    if (rc!=0)
	    {
	    	cout << "ERROR - Catalog Record Update Failed." << endl;
	    }

	    // Reset the pointers
		fldPtr = 0; // FieldPointer
		currRecordEnd = 36; // CurrentRecordEnd is the end of the fixed portion of the record.

	}

	cat->releaseTableRef(upperTableName); // Really may not be necessary

	cat->getTableRef(upperTableName); // This will create an empty table


	free(data);
	return 0;
}

/*
 * This method adds an additional column to the table definition.
 * Additional columns are always positioned towards the end of the table of columns.
 * Both the parameters are input.
 */
RC RM::addAttribute(const string tableName, const Attribute attr)
{
	RC rc;
	PF_Interface *pfi = PF_Interface::Instance();
	string upperTableName = _strToUpper(tableName);
	string upperAttributeName = _strToUpper(attr.name);
	unsigned fldPtr = 0; // FieldPointer
	unsigned currRecordEnd = 36; // CurrentRecordEnd is the end of the fixed portion of the record. We know this for CATALOG.

	Catalog *cat = Catalog::Instance(); // This opens the catalog also

	TableRef tRef = cat->getTableRef(upperTableName);
	if (tRef.colRefs.size()==0)
	{
		cout << "ERROR - No Catalog information found for the table. Aborting." << endl;
		return -1;
	}

	bool colFound = false;
	ColRef *cr;
	list<ColRef>::iterator itColRef;
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();

		if (upperAttributeName.compare(cr->colName) == 0) // This will tell you if the column exists by this name.
		{
			cout << "  Column : " << cr->colName << "exists with details <Pos:" << cr->colPosition << " |Type:" << cr->colType << " |Length:" << cr->colLength << " |DeletedInd:" << cr->colDelInd << ">." ;
			colFound = true;
		}
	}

	if (colFound)
	{
		cout << "ERROR - Catalog seems to have a column by this name already in the table. Aborting." << endl;
		return -1;
	}

	void *data = malloc(400); // Give enough room for TableName, ColumnName and the other data items
	// If itcame here then the column is unique and so go ahead and update the Catalog
	cout << "Adding.... " << attr.name << " | " << attr.type << " | " << attr.length << endl;
    pfi->addIntToBuffer(data, fldPtr, 6);//0-3 - For the Field Count
    fldPtr+=4;

    pfi->addIntToBuffer(data, fldPtr, currRecordEnd);//4-7 - For the column TABLENAME
    fldPtr+=4;
    pfi->addIntToBuffer(data, fldPtr, upperTableName.length());//8-11 - Size of the Varchar TABLENAME
    fldPtr+=4;
    pfi->addVarcharToBuffer((char *)data, currRecordEnd, (void *)upperTableName.c_str(), upperTableName.length());// In the Variable Length portion
    currRecordEnd += upperTableName.length(); // Next spot where you can add Varchar data

    pfi->addIntToBuffer(data, fldPtr, currRecordEnd);//12-15 - For the column COLUMNNAME
    fldPtr+=4;
    pfi->addIntToBuffer(data, fldPtr, upperAttributeName.length());//16-19 - Size of the Varchar COLUMNNAME
    fldPtr+=4;
    pfi->addVarcharToBuffer((char *)data, currRecordEnd, (void *)upperAttributeName.c_str(), upperAttributeName.length());//In the Variable Length portion
    currRecordEnd += upperAttributeName.length(); // Next spot where you can add Varchar data

    //A new column is always positioned at the end of the table definition
    pfi->addIntToBuffer(data, fldPtr, tRef.colRefs.size()+1);//20-23 - For the POSITION
    fldPtr+=4;

    pfi->addIntToBuffer(data, fldPtr, attr.type);//24-27 - For the TYPE
    fldPtr+=4;

    int l;
    if (attr.type != TypeVarChar) l=4; else l=attr.length;
    pfi->addIntToBuffer(data, fldPtr, l);//28-31 - For the LENGTH
    fldPtr+=4;

    pfi->addIntToBuffer(data, fldPtr, 0);//32-35 - For the Deleted Ind column
    fldPtr+=4;

    rc = cat->addCatalogTuple(data);
    if (rc!=0)
    {
    	cout << "ERROR - Catalog Record Update Failed." << endl;
    }

	cat->releaseTableRef(upperTableName); // This will allow you to refresh the cache with the latest table definition

	cat->getTableRef(upperTableName); // This will re-create a TableRef cache entry


	free(data);
	return 0;
}

/*
 * This method effectively drops a column from the table definition.
 * In reality, this method marks a column as Deleted. This designation would then be read by
 * other methods which use the Table Definitions to process the data ie. read/update/insert as the case may be.
 */
RC RM::dropAttribute(const string tableName, const string attributeName)
{
	RC rc = -1;
	PF_Interface *pfi = PF_Interface::Instance();
	Catalog *cat = Catalog::Instance(); // This opens the catalog also
	string upperTableName = _strToUpper(tableName);
	string upperAttributeName = _strToUpper(attributeName);

	TableRef tRef = cat->getTableRef(upperTableName);
	int tnOffset, tnSz = 0;
	int cnOffset, cnSz = 0;
	int diOffset = 32; // This is where the DelInd is stored

	if (tRef.colRefs.size()==0)
	{
		cout << "ERROR - No Catalog information found for the table. Aborting." << endl;
		return -1;
	}

	bool colFound = false;
	ColRef *cr;
	list<ColRef>::iterator itColRef;
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();

		if (upperAttributeName.compare(cr->colName) == 0) // This will tell you if the column exists by this name.
		{
			cout << "  Column : " << cr->colName << "exists with details <Pos:" << cr->colPosition << " |Type:" << cr->colType << " |Length:" << cr->colLength << " |DeletedInd:" << cr->colDelInd << ">."  << endl;
			if (cr->colDelInd == 1)
				cout << "Column is already deleted. No action taken." << endl;
			else
				colFound = true;
		}
	}

	if (!colFound)
	{
		cout << "ERROR - Catalog has no column by this name. Aborting." << endl;
		return -1;
	}

	void *buffer = malloc(PF_PAGE_SIZE);

	// Scan the catalog for the record to delete and then update the record.
	// Loop through eachpage...
	int noOfSlotsInPage = 0;
	int currSlotPtr = 0;
	int currRecordPtr = 0;
	int noOfPages = cat->_catalogRef.pfh.GetNumberOfPages();
	int pgNum = 0; // Page where the column was found to mark as deleted
	if (noOfPages < 1) // You need atleast 1 page in the catalog
	{
		// It should never come here and so clean upand exit gracefully
		cout << "ERROR - Unable to read any page from Catalog Table. RC=" << noOfPages << endl;
		free(buffer);
		return -1;
	}

	// TableName is always the first column and starts at Position 4 in the record
	// Column name is the second column in the catalog and starts at Position 12 in the record

	for(int i=0; i<noOfPages; i++)
	{
	    rc = pfi->getPage(cat->_catalogRef.pfh, i, buffer);
		if (rc != 0)
		{
			// It should never come here and so clean upand exit gracefully
			cout << "ERROR - Unable to read Page " << i << " from Catalog Table. RC=" << rc << endl;
			free(buffer);
			return -1;
		}

		// OK now you found a page.
		// Next go through each data tuple in the page and determine its content

		noOfSlotsInPage = pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
		currSlotPtr = PF_PAGE_SIZE-8; // Position of the first slot pointer
		currRecordPtr = 0;
		string tn;
		string cn;

		for (int j=1; j<=noOfSlotsInPage; j++)
		{
			currSlotPtr =PF_PAGE_SIZE- 8 - 8*j; // Position on to the next Slot
			currRecordPtr = pfi->getIntFromBuffer(buffer, currSlotPtr); // This is topositionon where to read from the buffer
			cout<< "Checking...  Page# " << i << "  Slot# " << j+1 << " at " << currRecordPtr << " ."  << endl;

			if (currRecordPtr < 0)	continue; // The slot has been deleted and so skip.

			//This slot belongs to an entry that is not deleted
			tnOffset = currRecordPtr + pfi->getIntFromBuffer(buffer,currRecordPtr+4); //Rel address of TableName entry in this record
			tnSz = pfi->getIntFromBuffer(buffer,currRecordPtr+4+4); // ie. 4 bytes after the tnOffset
			cnOffset = currRecordPtr + pfi->getIntFromBuffer(buffer,currRecordPtr+12); //Rel address of ColumnName entry in this record is 4 more bytes after tnSize
			cnSz = pfi->getIntFromBuffer(buffer,currRecordPtr+12+4); // Its size is 4 bytes after its offset
			cout << tnOffset << "-" << tnSz << "-"  << cnOffset << "-"  << cnSz << endl;
		    tn = pfi->getVarcharFromBuffer(buffer,tnOffset, tnSz);
		    cn = pfi->getVarcharFromBuffer(buffer,cnOffset, cnSz);
		    cout<< tn << "-" << cn << endl;
			if (tn.compare(upperTableName) == 0)
			{
				// Table Name matches,now check col name
				if (cn.compare(upperAttributeName) == 0)
				{
					// Column Name also matches, then update the DelInd
					pfi->addIntToBuffer(buffer, currRecordPtr+diOffset, 1);
					cout<< "Updating...  Page# " << i << "  Slot# " << j+1 << " at " << currRecordPtr+diOffset << " ."  << endl;
					rc = 0;
					pgNum = i; // Remember the page where the columnn was found.
					break;
				}
				else
				{
					// Check the next rec in catalog
					continue;
				}
			}
			else
			{
				// Check the next rec in catalog
				continue;
			}
			// Check the TableName and attributename.
			//If it matches then simply update the DelInd and WritePage
		} // Looping for all slots in the page
	} // Looping for all pages

    rc = pfi->putPage(cat->_catalogRef.pfh, pgNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table. RC=" << rc << endl;
		rc = -1;
	}

	return rc;
}

/*
 * This method removes the table completely from the system.
 * This includes erasing the PagedFile from the disk as well as removing the CATALOG entries for that table.
 */
RC RM::deleteTable(const string tableName)
{
	string upperTableName = _strToUpper(tableName);
	RC rc;
	Catalog *_ci = Catalog::Instance();
	PF_Interface* _pfi = PF_Interface::Instance();

	_ci->releaseTableRef(upperTableName); // This removes the table from the cache
	_pfi->removeTable(upperTableName); // This removes the physical table from the disk


	void *buffer = malloc(PF_PAGE_SIZE);

	// First find the number of pages to loop through
	unsigned noOfPages = _ci->_catalogRef.pfh.GetNumberOfPages();
	unsigned noOfSlotsInPage = 0;
	unsigned recordSize = 0;
	unsigned currSlotPtr = 0;
	int currRecordPtr = 0;
	ColRef currColRef;

	// Technically the largest recordsizecanbe PF_PAGE_SIZE-16. Make sure you have room for this.
	void *recordData = malloc(PF_PAGE_SIZE);

	// Loop through eachpage...
	for(unsigned i=0; i<noOfPages; i++)
	{
	    rc = _pfi->getPage(_ci->_catalogRef.pfh, i, buffer);
		if (rc != 0)
		{
			// It should never come here and so clean upand exit gracefully
			cout << "ERROR - Unable to read Page " << i << " from Catalog Table. RC=" << rc << endl;
			free(buffer);
			return -1;
		}

		// OK now you found a page.
		// Next go through each data tuple in the page and determine its content

		noOfSlotsInPage =_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
		//cout<< "Found " << noOfSlotsInPage << " in the page " << i <<"." << endl;
		currSlotPtr = PF_PAGE_SIZE-8-8; // Position of the first slot pointer
		currRecordPtr = 0;
		for (unsigned j=0; j<noOfSlotsInPage; j++)
		{
			currRecordPtr = _pfi->getIntFromBuffer(buffer, currSlotPtr); // This is topositionon where to read from the buffer
			recordSize = _pfi->getIntFromBuffer(buffer, currSlotPtr+4); // Now you know how many bytes toread from the buffer
			cout<< "Processing entry:   Page# " << i << "  Slot# " << j+1 << " has " << recordSize <<" bytes at " << currRecordPtr << "."  << endl;
			if (currRecordPtr<0) // Deleted Catalog records can exist in the table.
			{
				cout<< "  This is a DELETED ENTRY... so skipping." << endl;
				currSlotPtr -=8; // Position on to the next Slot
				continue;
			}

			// Do a byte-byte copy from buffer to recordData upto the size of the record
			// copying byte by byte into the buffer
		    for(unsigned k = 0; k < recordSize; k++)
		    {
		    	*((char *)recordData+k) = *((char *)buffer+currRecordPtr+k);
		    }

			// Now convert data to ColRef object
			// In case of regular tables, I would have to convert this to Attribute but since this is Catalog data
			// I straight away doit this way
			currColRef = _ci->convertCatalogRecordDataToColRef(_strToUpper(tableName), recordData,recordSize);
			if (currColRef.colName.compare("SKIP") == 0) // meaning that the column does not belong to this table.
			{
				cout<< "  Catalog entry does not belong to this table.Skipping." << endl;
				// This catalog record does not belong to the table. Skip it.
			}
			else
			{
				cout<< "  DELETING Catalog entry for :" <<_strToUpper(tableName) << " | " << currColRef.colName << endl;
				// You anyway have all the info at hand...Simply delete the record
			    for(unsigned k = 0; k < recordSize; k++)
			    {
			    	*((char *)buffer+currRecordPtr+k) = '-';
			    }
			    _pfi->addIntToBuffer(buffer,currSlotPtr,-1); // Marking the slot as a deleted record
			    _pfi->addIntToBuffer(buffer,currSlotPtr+4,0); // Marking the slotsize to 0

			}
			currSlotPtr -=8; // Position on to the next Slot
		}
		free(recordData);
	    rc = _pfi->putPage(_ci->_catalogRef.pfh, i, buffer);
		if (rc != 0)
		{
			// It should never come here and so clean upand exit gracefully
			cout << "ERROR - Unable to write Page " << i << " from Catalog Table. RC=" << rc << endl;
			free(buffer);
			return -1;
		}

	}


	free(buffer);
	return 0;

}


/*
 * This method inserts a row into a table. The input parameters are the tableName and a void * buffer.
 * At the end of a successful insert, the system passes back the RID of the record inserted.
 */
RC RM::insertTuple(const string tableName, const void *data, RID &rid)
{
	Catalog *_ci = Catalog::Instance();
	PF_Interface *m_pfi = PF_Interface::Instance();

	string upperTableName = _strToUpper(tableName);
	// Variable Initialization
	int newRecSize=0;
	int freePtr=0;
	int newSlotPtr=0;
	int noOfSlots=0;
	int newSlotSize=0;
	int pgNum=0;
	int result=0;
	RC rc;
	void *buffer = malloc(PF_PAGE_SIZE);

	// Determine the size of the record to structure the record field
	TableRef l_tableobj = _ci->getTableRef(upperTableName);

	void *recordData = malloc(PF_PAGE_SIZE); // to accomodate the largest record.

	_ci->convertTupleFormatToRecordFormat(l_tableobj, data, recordData);

	newRecSize = _ci->getRecSizeFromRecordBuffer(l_tableobj, recordData);

	// Locate the next free page
	pgNum = m_pfi->locateNextFreePage( l_tableobj.pfh, newRecSize);


	if (pgNum < 0)
	{
		cout << "ERROR - Unable to locate next free page from Table." << endl;
		result =-1;
	}

	// Read the Page and update the buffer
    rc = m_pfi->getPage(l_tableobj.pfh, pgNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to read Page from Table. RC=" << rc << endl;
		result = -1;
	}

	//------------------------------------- Reading Slot directory-----------------------------
	// Locate the Free Pointer from the slot directory
	freePtr = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-4);
	// Find the number of slots in the slot directory
	noOfSlots = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	//------------------------------------------------------------------------------------------

	// Write the tuple into the buffer at freePtr
	// General Record format
	//|<4, number of fields>|<4,offset of variable><4, size of the variable>|<4, fixed data type>|<x, variable data type>
    memcpy((char *)buffer + freePtr, recordData, newRecSize);

    //string temp = (char *)buffer;
    //cout << "Data:"<< temp;

    //--------------------------------------- Update the slot directory-----------------------------------------
	//1. Find the position of the new Slot
	//2. compute the value of newSlotPtr
	newSlotPtr = PF_PAGE_SIZE - 8 // accounting for the free space pointer & no of slots
			- 8 * noOfSlots // each slot occupies 4 bytes for offset and 4 more for size
			- 8; // start position for the new slot
	//cout<< "NewSlotPointer=" << newSlotPtr << endl;

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

    rc = m_pfi->putPage(l_tableobj.pfh, pgNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table. RC=" << rc << endl;
		result = -1;
	}
	free(buffer);
	return result;
}
/*
 * This method returns the complete record containing the data formatted in a prearranged format for readability.
 * The two parameters sent as input include the tableName and RID
 */
RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
	string upperTableName = _strToUpper(tableName);
	RC rc;
	Catalog *_ci = Catalog::Instance();
	PF_Interface *_pfi = PF_Interface::Instance();
	unsigned uTemp = 0;


	TableRef tRef = _ci->getTableRef(upperTableName);
	if (tRef.colRefs.size()==0)
	{
		cout << "ERROR - No Catalog information found for the table. Aborting." << endl;
		return -5;
	}

	if (tRef.pfh.GetNumberOfPages()<rid.pageNum)
	{
		if (rid.pageNum != RM_NOVALUE)
			cout << "ERROR - RID contains a Page that does not exist. Aborting. Page#:" << rid.pageNum << "." << endl;
		return -4;
	}


	// You need to allocate for the page being read and clean it up at the end.
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = _pfi->getPage(tRef.pfh, rid.pageNum, buffer);
	if (rc !=0)
	{
		cout << "ERROR - Read Page#" << rid.pageNum << " from PagedFile " << upperTableName << " errored out. RC=" << rc << "." << endl;
		free(buffer);
		return -3;
	}

	//Since the table has atleast Page 0. Let us make sure that we do not proceed if there are no rows.
	uTemp = _pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	if (uTemp < rid.slotNum) // The Slot# you are looking for does not exist.
	{
		cout << "ERROR - Page#" << rid.pageNum << " Slot#" << rid.slotNum << " from PagedFile " << upperTableName << " does not exist." << endl;
		free(buffer);
		return -2;
	}


	// On this page locate the slot pointer of Slot# in RID
	uTemp = PF_PAGE_SIZE - 8 - 8*rid.slotNum;
	int tuplePtr = _pfi->getIntFromBuffer(buffer, uTemp);
	int tupleSize = _pfi->getIntFromBuffer(buffer, uTemp+4);

	//cout << "Tuple is at Slot " << rid.slotNum << " - PageOffset =" << tuplePtr << " - Size =" << tupleSize << endl;
	if (tuplePtr == -1) // The slot has been deleted. Return error
	{
		cout << "Deleted Records cannot be retrieved." << endl;
		free(buffer);
		return -1;
	}

	void *tempRecData = malloc(tupleSize);
	void *tempTupData = malloc(tupleSize);
	memcpy((char *)tempRecData, (char *)buffer+tuplePtr, tupleSize);

	int checkTombStone = _pfi->getIntFromBuffer(tempRecData,0);
	if (checkTombStone==0)
	{
		// You must send this request to readTuple with a different RID
		// Clean up before you leave.
		RID newRid;
		newRid.pageNum = _pfi->getIntFromBuffer(tempRecData,4);
		newRid.slotNum = _pfi->getIntFromBuffer(tempRecData,8);
		cout<< "Tombstone record found...RID=[" << newRid.pageNum << " | " << newRid.slotNum << "]" <<endl;
		free(tempRecData);
		free(tempTupData);
		free(buffer);
		return (readTuple(tableName, newRid, data));
	}

	_ci->convertRecordFormatToTupleFormat(tRef, tempRecData, tempTupData);
	memcpy((char *)data, (char *)tempTupData, tupleSize);
	free(tempRecData);
	free(tempTupData);
	free(buffer);


	return 0;
}

/*
 * Read attribute method provides the ability to return just one attribbute of a column.
 * TableName, RID and AttributeName are provided as inputs. The data is returned in a predetermined format.
 */
RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
	RC rc = -1;
	string upperTableName = _strToUpper(tableName);
	string upperAttributeName = _strToUpper(attributeName);
	Catalog *_ci = Catalog::Instance();
	PF_Interface *_pfi = PF_Interface::Instance();
	RM *rm = RM::Instance();
	TableRef tRef = _ci->getTableRef(upperTableName);


	void *recordData;

	if (tRef.colRefs.size()==0)
	{
		cout << "ERROR - No Catalog information found for the table. Aborting." << endl;
		return -1;
	}

	// Allocate memory enough for the tuple
	recordData = malloc(PF_PAGE_SIZE); // technically you can have a record of PF_PAGE_SIZE-8

	rm->readTuple(upperTableName, rid, recordData);
	// Now you should have the record in a tuple format.

	// Check the column type fromthe catalog for the attribute requested and present the info accordingly.
	ColRef *cr;
	unsigned fieldOffset = 0;
	unsigned dataOffset = 0;
	unsigned dataSize = 0;
	string str;
	float fVal = 0;
	int iVal = 0;
	bool colFound = false;
	list<ColRef>::iterator itColRef;
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();
		if (cr->colDelInd == 1) // skip a deleted column
		{
			continue;
		}

		if (upperAttributeName.compare(cr->colName) == 0) // This will tell you if the column should be printed.
		{
			colFound = true;
		}

		if (cr->colType==TypeVarChar)
		{
			dataSize = _pfi->getIntFromBuffer(recordData, fieldOffset);
			if (colFound)
			{
				str = _pfi->getVarcharFromBuffer(recordData, dataOffset, dataSize);
				cout << "Attribute: " << upperAttributeName << " value=" << str << endl;
				memcpy((char *)data + dataOffset, &dataSize, 4);
				dataOffset += 4;
				memcpy((char *)data + dataOffset, str.c_str(), dataSize);
				dataOffset += dataSize;
			}
			else // account for the size and move on.
			{
				fieldOffset += 4;    // account for the text size
				fieldOffset += dataSize; // account for the text string
			}
		}
		else if (cr->colType==TypeInt)
		{
			if (colFound)
			{
				iVal = _pfi->getIntFromBuffer(recordData, fieldOffset);
				cout << "Attribute: " << upperAttributeName << " value=" << iVal << endl;
				memcpy((char *)data + dataOffset, &iVal, 4);
			    dataOffset += 4;
			}
			else // account for the size and move on.
			{
				fieldOffset += 4;    // account for the field size
			}
		}
		else
		{
			if (colFound)
			{
				fVal = _pfi->getFloatFromBuffer(recordData, fieldOffset);
				cout << "Attribute: " << upperAttributeName << " value=" << fVal << endl;
				memcpy((char *)data + dataOffset, &fVal, 4);
			    dataOffset += 4;
			}
			else // account for the size and move on.
			{
				fieldOffset += 4;    // account for the field size
			}
		}

		if (colFound)
		{
			rc = 0;
			break;
		}

	}

	free(recordData);
	return rc;
}

/*
 * This initializes the content of a table on the system
 */
RC RM::deleteTuples(const string tableName)
{
	string upperTableName = _strToUpper(tableName);
	int result =-1;
	Catalog *_ci = Catalog::Instance();
	_ci->releaseTableRef(upperTableName);

	result = m_pfi->removeTable(upperTableName);
	TableRef l_tableobj = _ci->getTableRef(upperTableName);

	return result;
}

/*
 * This method takes a tableName and RID as input. Then locates the record in the table.
 * Finally, it marks the record as deleted.
 */
RC RM::deleteTuple(const string tableName, const RID &rid)
{
	RC rc = -1;
	string upperTableName = _strToUpper(tableName);

	// variable initialization
	unsigned noOfSlots, deleteSlotSize=0;
	int recordSlotPtr=0;
	int result=0;
	int deleteOffset = 0; // Can be negative.

	//1. Open the Table
	//2. Identify the page and the record to be deleted
	//3. Delete the record and update the slot directory
	void * buffer = malloc(PF_PAGE_SIZE);
	m_ci = Catalog::Instance();
	TableRef l_tableobj = m_ci->getTableRef(upperTableName);
	//m_pfi->openTable(upperTableName, l_tableobj.pfh); -- No need to Open. getTableRef will do it for you.
	rc = m_pfi->getPage(l_tableobj.pfh,rid.pageNum,buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to read Page to Table. RC=" << rc << endl;
		result = -1;
		return result; // Return here. Otherwise you will fallthru to logic that depends upon this info
	}

	// Stored the page info on Buffer, buffer
	// Need to read the slot directory to find the offset value for my record.

	noOfSlots = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	if(rid.slotNum <= noOfSlots)
	{
		recordSlotPtr = PF_PAGE_SIZE - 8 // accounting for the free space pointer & no of slots
					- 8 * rid.slotNum // each slot occupies 4 bytes for offset and 4 more for size
					//- 8; // start position of the slot be updated
					;

		// Beforegoing further ...
		// Check if this record is a tombstone.
		// If Tombstone then you will need to do the following:
		// i. Locate the RID in the tombstone and DELETE it.
		// ii. Fall thru to the rest of the logic here so that...
		//    the tombstone also will be deleted.
		// In our implementation, our tombstone will never point to another tombstone.

		int checkTombStone = m_pfi->getIntFromBuffer(buffer,m_pfi->getIntFromBuffer(buffer,recordSlotPtr));
		if (checkTombStone==0)
		{
			// You must send a request to deleteTuple to delete the RID
			RID newRid;
			newRid.pageNum = m_pfi->getIntFromBuffer(buffer,m_pfi->getIntFromBuffer(buffer,recordSlotPtr)+4);
			newRid.slotNum = m_pfi->getIntFromBuffer(buffer,m_pfi->getIntFromBuffer(buffer,recordSlotPtr)+8);
			cout<< "Tombstone record found...RID=[" << newRid.pageNum << " | " << newRid.slotNum << "]" <<endl;

			rc = _rm->deleteTuple(tableName, newRid); // Yes recursive call.
			if (rc != 0)
			{
				cout<< "Could not remove the RID found in the tombstone. RC=" << rc <<endl;
				result = -1;
				// At this stage you only need to deallocate buffer
				free(buffer);
				return result; // Return here. Otherwise you will fallthru to logic that depends upon this info
			}

			// Refresh the current page as there might have been updates in another thread due to the operation above
			rc = m_pfi->getPage(l_tableobj.pfh,rid.pageNum,buffer);
			if (rc != 0)
			{
				cout<< "Could not retrieve the page. RC=" << rc <<endl;
				result = -1;
				free(buffer);
				return result;
			}

		}

		deleteSlotSize = 0;
		deleteOffset =-1;

		// We technicallydo not need to do this but this visually initializes the record data by updating the value therein to '-'
		unsigned delRecOffset = m_pfi->getIntFromBuffer(buffer, recordSlotPtr);
		unsigned delRecSize = m_pfi->getIntFromBuffer(buffer, recordSlotPtr+4);
		// copying byte by byte into the buffer
	    for(unsigned k = 0; k < delRecSize; k++)
	    {
	    	*((char *)buffer+delRecOffset+k) = '-';
	    }


		// Update the slot directory about the deleted record<-1(offset); 0(record size)>
		m_pfi->addIntToBuffer(buffer, recordSlotPtr, deleteOffset);
		m_pfi->addIntToBuffer(buffer, recordSlotPtr+4, deleteSlotSize);

	    rc = m_pfi->putPage(l_tableobj.pfh, rid.pageNum, buffer);
		if (rc != 0)
		{
			cout << "ERROR - Unable to write Page to Table. RC=" << rc << endl;
			result = -1;
		}



	}
	else
	{
		cout<< "Delete Failed:Particular record is not present, better luck next time"<<endl;
		result = -1;
	}

	return result;
}

/*
 * This method takes the tableName, void * buffer and the RID as input. It locates the record by the RID.
 * Then updates the database entry for that RID with the values
 */
RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{

	RC rc;
	string upperTableName = _strToUpper(tableName);
	// Variable initialization
	unsigned noOfSlots=0,recordSlotPtr=0, newRecSize=0, oldRecSize=0;
	int result =0;

	void * buffer = malloc(PF_PAGE_SIZE);
	m_ci = Catalog::Instance();
	TableRef l_tableobj = m_ci->getTableRef(upperTableName);
	rc = m_pfi->getPage(l_tableobj.pfh,rid.pageNum,buffer);
	if (rc != 0)
	{
		cout<< "Could not retrieve the page. RC=" << rc <<endl;
		result = -5;
		free(buffer);
		return result;
	}

	// Read the slot directory to find info about the existing record
	noOfSlots = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
	if(rid.slotNum <= noOfSlots)
	{
		// Locate the Slot for the record
		recordSlotPtr = PF_PAGE_SIZE - 8 // accounting for the free space pointer & no of slots
					- 8 * rid.slotNum // each slot occupies 4 bytes for offset and 4 more for size
					//- 8; // start position of the slot be updated
					;

		// get the size of the record and compare it with the updated record coming in to be
		// sure that the updated record can fit in.
		void *recordData = malloc(PF_PAGE_SIZE);
		m_ci->convertTupleFormatToRecordFormat(l_tableobj, data, recordData);

		newRecSize = m_ci->getRecSizeFromRecordBuffer(l_tableobj, recordData);
		unsigned recordOffset =0;
		recordOffset = m_pfi->getIntFromBuffer(buffer,recordSlotPtr);
		oldRecSize = m_ci->getRecSizeFromRecordBuffer(l_tableobj, (char *)buffer+recordOffset);
		// Beforegoing further and determining whether to write here or not...
		// Check if this record is a tombstone.
		// If Tombstone then you will need to do the following:
		// i. Locate the RID in the tombstone and DELETE it. Yes Delete it.
		// ii. Fall thru to the rest of the logic here so that the update will create a tombstone right here for this new data if necessary.

		int checkTombStone = m_pfi->getIntFromBuffer(buffer,recordOffset);
		if (checkTombStone==0)
		{
			// You must send a request to deleteTuple to delete the RID
			RID newRid;
			newRid.pageNum = m_pfi->getIntFromBuffer(buffer,recordOffset+4);
			newRid.slotNum = m_pfi->getIntFromBuffer(buffer,recordOffset+8);
			cout<< "Tombstone record found...RID=[" << newRid.pageNum << " | " << newRid.slotNum << "]" <<endl;
			rc = _rm->deleteTuple(tableName, newRid);
			if (rc != 0)
			{
				cout<< "Could not remove the RID found in the tombstone. RC=" << rc <<endl;
				result = -1;
				// At this stage you only need to deallocate buffer
				free(buffer);
				return result;
			}
			// Refresh the current page as there might have been updates in another thread due to the operation above
			rc = m_pfi->getPage(l_tableobj.pfh,rid.pageNum,buffer);
			if (rc != 0)
			{
				cout<< "Could not retrieve the page. RC=" << rc <<endl;
				result = -5;
				free(buffer);
				return result;
			}

		}

		// Now you can process the update like usual and recreate the tombstone with fresh data as necessary.

		RID newRid;
		if(newRecSize<=oldRecSize)
		{
			// The new record will fit into the original space safely, so update the record
		    memcpy((char *)buffer + recordOffset, recordData, newRecSize);

		    // Update the record size in the slot directory to show the new size
			m_pfi->addIntToBuffer(buffer, recordSlotPtr+4, newRecSize );

			free(recordData);
		}
		else
		{
			// Insert the record in a place where it can safely fit and then get the RID of that location.
			rc = _rm->insertTuple(tableName, data, newRid); // pass the original data to insertTuple not your formatted one
			if (rc != 0)
			{
				cout<< "Could not insert the record in the new location. RC=" << rc <<endl;
				result = -4;
				free(recordData);
				free(buffer);
				return result;
			}
			cout<< "Record inserted at the new location. RID= " << "[" << newRid.pageNum << "|" << newRid.slotNum << "]" <<endl;

			// Refresh the current page as there might have been updates in another thread or due to the operation above
			rc = m_pfi->getPage(l_tableobj.pfh,rid.pageNum,buffer);
			if (rc != 0)
			{
				cout<< "Could not retrieve the page. RC=" << rc <<endl;
				result = -3;
				free(recordData);
				free(buffer);
				return result;
			}

			free(recordData);
			//Create a tomb stone with that RID and save it in this location.
			// TombstoneRecordFormat {0000x,RID-<PageNumber,SlotNumber> from the previous step} -
			// Note that the  Tombstone record is a total length of 12 bytes exactly.
			void *tombStone = malloc(12);
			m_pfi->addIntToBuffer(tombStone, 0, 0);
			m_pfi->addIntToBuffer(tombStone, 4, newRid.pageNum);
			m_pfi->addIntToBuffer(tombStone, 8, newRid.slotNum);
		    memcpy((char *)buffer + recordOffset, tombStone, 12);

		    // Update the record size in the slot directory to show the new size
			m_pfi->addIntToBuffer(buffer, recordSlotPtr+4, 12 ); // Tombstone size=12
			free(tombStone);
		}

		// write the updated page back
		rc = m_pfi->putPage(l_tableobj.pfh,rid.pageNum,buffer);
		if (rc != 0)
		{
			cout<< "Could not save the page. RC=" << rc <<endl;
			result = -2;
			free(buffer);
			return result;
		}

	}
	else
	{
		cout<< "Update Failed:Particular record is not present, better luck next time"<<endl;
		free(buffer);
		result = -1;
		return result;
	}

	free(buffer);
	return result;
}


/*
 * This reorganizes the content of a single Page without altering any of the RIDs.
 * At the end of the operation, all free space will be made contiguous
 */
RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
	Catalog *_ci = Catalog::Instance();
	PF_Interface *_pfi = PF_Interface::Instance();
	RC rc = -1;
	string upperTableName = _strToUpper(tableName);
	TableRef tRef = _ci->getTableRef(upperTableName);


	// Loop through the slots in the slot dir.
	// If deleted records found... move the next record after the last data record written
	// Then adjust the offset poointers in the slotsto move all free space to the end.

	void *buffer = malloc(PF_PAGE_SIZE);
	rc = _pfi->getPage(tRef.pfh, pageNumber, buffer);
	if (rc !=0)
	{
		cout << "ERROR - Read Page#" << pageNumber << " from PagedFile " << upperTableName << " errored out. RC=" << rc << "." << endl;
		return -2;
	}

	int numOfSlots = _pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE - 8);
	int newFreePtr = 0;
	int recSize = 0;
	int recPtr = 0;
	int slotPtr = 0;

	for (int i=1; i<=numOfSlots; i++)
	{
		// Copy data from SlotOffset to newFreePtr
		// Update Slot Offset to newFreePtr
		// Update newFreePtr += SlotDataSize.

		slotPtr = PF_PAGE_SIZE - 8 - 8*i;
		recPtr = _pfi->getIntFromBuffer(buffer, slotPtr);
		recSize = _pfi->getIntFromBuffer(buffer, slotPtr + 4);

		if (recPtr==-1) // Indicating that the record was deleted...
		{
			// Now if the record is deleted, you don't want to remove the slot itself.
			//Just skip. Nothing to copy or update.
			cout << "Page/Slot # " <<pageNumber << "/" << i << " - Skipped.  FreePtr is at: " << newFreePtr << endl;
		}
		else
		{
			// copying byte by byte into the buffer
			for(int k = 0; k < recSize; k++)
			{
				*((char *)buffer+newFreePtr+k) = *((char *)buffer+recPtr+k) ;
			}


		    _pfi->addIntToBuffer(buffer, slotPtr, newFreePtr); // If you have moved it then update the slot's recordOffset
			cout<< "Page/Slot # " <<pageNumber << "/" << i << " - " << recSize << " bytes at " << recPtr << " moved to " << newFreePtr << endl;
		    newFreePtr += recSize; // increment thisonly if you have moved bytes
		}
	}

	//update the newFreePtr

	// Also initialize the free space with a ' ' so that you visually show you have moved the bytes up
	for(int k = newFreePtr; k < (PF_PAGE_SIZE-8-8*numOfSlots-1); k++)
	{
		*((char *)buffer+k) = ' ' ;
	}



	_pfi->addIntToBuffer(buffer, PF_PAGE_SIZE-8, numOfSlots);
	_pfi->addIntToBuffer(buffer, PF_PAGE_SIZE-4, newFreePtr);
	// write thepage
    rc = _pfi->putPage(tRef.pfh, pageNumber, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table. RC=" << rc << endl;
		rc = -1;
	}


	return rc;
}



RM_ScanIterator::RM_ScanIterator()
{
	m_count = 0;
	m_compareValue = 0;
	m_lastVisitedRid.pageNum = 0;
	m_lastVisitedRid.slotNum = 0;
}

RM_ScanIterator::~RM_ScanIterator()
{

}

RC RM::scan(const string tableName,const string conditionAttribute,const CompOp compOp, const void *value, const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator)
{
	// variable initialization
	int result =0;
	int currentPgNum =0;
	int noOfSlots=0;
	int currSlotPtr=0;
	int currRecordPtr =0;
	int recordSize = 0;
	bool occurrenceFound;
	RC rc;
	bool validate = false;
	void * returnedData = malloc(40);
	int compareData =0;

	void * buffer = malloc(PF_PAGE_SIZE);
	m_ci = Catalog::Instance();
	rm_ScanIterator.m_tableobj = m_ci->getTableRef(tableName);
	// Validate the data with the Attribute on condition
	list<ColRef>::iterator itCompColRef; // get the iterator for ColRef

	PF_Interface *_pfi = PF_Interface::Instance();

	// store all info if the user needs more tuples. Operation performed using getnexttuple
	rm_ScanIterator.m_conditionAttribute = conditionAttribute;
	rm_ScanIterator.m_compOp = compOp;
	//------------------------------------------------------------------------------------------------------
	if(value!= NULL)
	{
		memcpy(&rm_ScanIterator.m_compareValue, (char *)value, sizeof(int));
		for(itCompColRef = rm_ScanIterator.m_tableobj.colRefs.begin(); itCompColRef!= rm_ScanIterator.m_tableobj.colRefs.end(); ++itCompColRef)
		{
			if( itCompColRef->colName == _strToUpper(rm_ScanIterator.m_conditionAttribute) && (itCompColRef->colDelInd ==0) && (currRecordPtr!=-1))
			{

				// Validate if the condition passes
				memcpy(&compareData, (char *)buffer+ currRecordPtr+itCompColRef->recordOffset, sizeof(int));

			}
		}
	}
	else
	{

		rm_ScanIterator.m_compareValue = 0;
		compareData = 0;
	}
	//------------------------------------------------------------------------------------------------------------------------------

	// If its a NO_OP validate returns true
	validate = rm_ScanIterator.validateData(rm_ScanIterator.m_compOp, rm_ScanIterator.m_compareValue, compareData);
	//1. Open the required table
	//2. Scan through each Pg to find out abt the record which hold this info and cout the first occurrence of it
	//3. After that use getNextTuple to get the other info.
	// Get the number of pages
	unsigned numbOfPgs = rm_ScanIterator.m_tableobj.pfh.GetNumberOfPages();

	vector<string>::const_iterator attrNamesptr;
	int i =0;
	for(attrNamesptr = attributeNames.begin(); attrNamesptr!=attributeNames.end(); ++attrNamesptr)
	{
		rm_ScanIterator.m_values[i] = (*attrNamesptr);
		i++;
	}
	while((currentPgNum+1)<=numbOfPgs)
	{
		rc = m_pfi->getPage(rm_ScanIterator.m_tableobj.pfh,currentPgNum,buffer);
		if(rc!=0)
		{
			cout << " Scan:page read failed"<< endl;
			result =-1;
			break;
		}
		// Read the slot directory to find info about the existing record
		noOfSlots = m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
		// read each record and check whether it passes on the condition imposed

		currSlotPtr = PF_PAGE_SIZE-8-8; // Position of the first slot pointer
		currRecordPtr = 0;

		for (int j=0; j<noOfSlots; ++j)
		{
			currSlotPtr = PF_PAGE_SIZE-8-(8*(j+1));
			currRecordPtr = m_pfi->getIntFromBuffer(buffer, currSlotPtr); // This is top position on where to read from the buffer
			recordSize = m_pfi->getIntFromBuffer(buffer, currSlotPtr+4); // Now you know how many bytes to read from the buffer

			cout << "First Occurrence of the Scanned Data "<<endl;
			list<ColRef>::iterator itColRef; // get the iterator for ColRef
			for(int k =0; k <i; k++)
			{
				for(itColRef = rm_ScanIterator.m_tableobj.colRefs.begin(); itColRef!= rm_ScanIterator.m_tableobj.colRefs.end(); ++itColRef)
				{
					if( itColRef->colName == _strToUpper(rm_ScanIterator.m_values[k]) && (itColRef->colDelInd ==0) && (currRecordPtr!=-1))
					{
						if(itColRef->colType == TypeVarChar && validate)
						{
							int size;
							memcpy(&size, (char *)buffer+ (currRecordPtr+itColRef->recordOffset+4), sizeof(int));
							int varCharOffset = 0;
							varCharOffset = _pfi->getIntFromBuffer(buffer, currRecordPtr+itColRef->recordOffset);
							char *tempData = (char*)malloc(size);
							memset(tempData, 0, size);
							memcpy(tempData, (char *)buffer+ currRecordPtr+varCharOffset, size);
							cout << itColRef->colName <<"::" << tempData << endl;

							free(tempData);
							occurrenceFound = true;
						}
						else if(itColRef->colType == TypeInt && validate)
						{
							int tempIntValue =0;
							memcpy(&tempIntValue, (char *)buffer+ currRecordPtr+itColRef->recordOffset, sizeof(int));
							cout << itColRef->colName << "::" << tempIntValue << endl;
							occurrenceFound = true;

						}
						else
						{
							float tempfloatValue =0;
							validate = rm_ScanIterator.validateData(rm_ScanIterator.m_compOp, rm_ScanIterator.m_compareValue, compareData);

							if(validate)
							{
								memcpy(&tempfloatValue, (char *)buffer+ currRecordPtr+itColRef->recordOffset, sizeof(int));
								cout << itColRef->colName <<"::" << tempfloatValue << endl;
								occurrenceFound = true;
							}
						}
					}
				}
				if(!occurrenceFound) break;

			}
			if(occurrenceFound)break;
		}
		if(occurrenceFound)
		{
			free(buffer);
			free(returnedData);
			result =0;
			break;
		}
		else
		{
			currentPgNum++;
		}
	}

	return result;
}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	RC rc;
	RM* rm = RM::Instance();
	void * buffer = malloc(PF_PAGE_SIZE);
	int result = 0;
	int noOfSlots =0;
	int currSlotPtr=0;
	int currRecordPtr =0;
	int recordSize = 0;
	int currentPgNum = m_lastVisitedRid.pageNum;
	bool found = false;
	bool validate = false;
	int compareData =0;
	int offset =0;
	PF_Interface *_pfi = PF_Interface::Instance();
	// Get the number of pages
	unsigned numbOfPgs = m_tableobj.pfh.GetNumberOfPages();
	while((currentPgNum+1)<=numbOfPgs)
	{
		rc = rm->m_pfi->getPage(m_tableobj.pfh,currentPgNum,buffer);
		if(rc!=0)
		{
			cout << " Scan for getNext Tuple:page read failed"<< endl;
			result =-1;
			break;
		}

		// Read the slot directory to find info about the existing record
		noOfSlots = rm->m_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
		// read each record and check whether it passes on the condition imposed

		currSlotPtr = PF_PAGE_SIZE-8-8; // Position of the first slot pointer
		currRecordPtr = 0;
		for (int j= (m_lastVisitedRid.slotNum+1); j<=noOfSlots; j++)
		{
/*
 * Lineno: 1396
Before: currSlotPtr = PF_PAGE_SIZE-8-(8*(j+1));
After: currSlotPtr = PF_PAGE_SIZE-8-(8*(j));
 *
 */			currSlotPtr = PF_PAGE_SIZE-8-(8*(j));
			currRecordPtr = rm->m_pfi->getIntFromBuffer(buffer, currSlotPtr); // This is top position on where to read from the buffer
			recordSize = rm->m_pfi->getIntFromBuffer(buffer, currSlotPtr+4); // Now you know how many bytes to read from the buffer

			// ----------------------------------------------------------------------------------------------------------------------------
			list<ColRef>::iterator itCompColRef; // get the iterator for ColRef
			if(m_conditionAttribute!="")
			{
				for(itCompColRef = m_tableobj.colRefs.begin(); itCompColRef!= m_tableobj.colRefs.end(); ++itCompColRef)
				{

					if( itCompColRef->colName == _strToUpper(m_conditionAttribute) && (itCompColRef->colDelInd ==0) && (currRecordPtr!=-1))
					{
						// Validate if the condition passes
						memcpy(&compareData, (char *)buffer+ currRecordPtr+itCompColRef->recordOffset, sizeof(int));
						break;
					}
				}
			}
			else
			{
				compareData = 0;
			}
			//-----------------------------------------------------------------------------------------------------------------------------
			// If its a NO_OP, validate returns true
			validate = validateData(m_compOp, m_compareValue, compareData);

			int k =0;
			list<ColRef>::iterator itColRef; // get the iterator for ColRef
			while(!(m_values[k]==""))
			{
				for(itColRef = m_tableobj.colRefs.begin(); itColRef!= m_tableobj.colRefs.end(); ++itColRef)
				{
					if( itColRef->colName == _strToUpper(m_values[k]) && (itColRef->colDelInd ==0) && (currRecordPtr!=-1))
					{
						m_lastVisitedRid.pageNum = currentPgNum;
						m_lastVisitedRid.slotNum = j;

						if(validate)
						{
							found = true;
							if (itColRef->colType == TypeInt)
							{
								memcpy((char*)data+ offset, (char *)buffer+ currRecordPtr+itColRef->recordOffset, sizeof(int));
								offset+= sizeof(int);

							} else if (itColRef->colType == TypeReal)
							{
								memcpy((char*)data+ offset, (char *)buffer+ currRecordPtr+itColRef->recordOffset, sizeof(int));
								offset+= sizeof(int);


							} else // cr->colType == TypeVarChar
							{
								// Read 4 bytes for the varchar
								// Then read that many bytes
								int size = 0;
								memcpy(&size, (char *)buffer+ (currRecordPtr+itColRef->recordOffset+4), sizeof(int));
								int varCharOffset = 0;
								varCharOffset = _pfi->getIntFromBuffer(buffer, currRecordPtr+itColRef->recordOffset);
								char *tempData = (char*)malloc(size);
								memset(tempData, 0, size);
								memcpy(tempData, (char *)buffer+ currRecordPtr+varCharOffset, size);

								memcpy((char*)data+ offset, &size, sizeof(int));
								offset+= sizeof(int);
								memcpy((char*)data+ offset, tempData, size);
								offset+= size;

								free(tempData);
							}
						}
					}
				}
				if(!validate)
				{
					break;
				}
				k++;
			}
			if(found)
			{
				rid.pageNum = currentPgNum;
				rid.slotNum = j+1;
				break;
			}
		}
		if(found)
		{
			free(buffer);
			break;
		}
		else
		{
			m_lastVisitedRid.slotNum =0;
			currentPgNum++;
		}

	}
	if(!found)
	{
		result = RM_EOF;
	}
	return result;
}



bool RM_ScanIterator::validateData (CompOp opObj, int compareVal, int retrievedData)
{
	bool validate = false;
	switch(opObj)
	{
	case EQ_OP:
		if(retrievedData == compareVal)
		{
			validate = true;
		}
		break;
	case LT_OP:
		if(retrievedData< compareVal)
		{
			validate = true;
		}
		break;
	case GT_OP:
		if(retrievedData> compareVal)
		{
			validate = true;
		}
		break;
	case LE_OP:
		if(retrievedData<= compareVal)
		{
			validate = true;
		}
		break;
	case GE_OP:
		if(retrievedData>= compareVal)
		{
			validate = true;
		}
		break;
	case NE_OP:
		if(retrievedData != compareVal)
		{
			validate = true;
		}
		break;
	case NO_OP:
	{
		validate = true;
	}
	break;
	default:
	{
		validate = false;
	}
	break;
	}

	return validate;
}

RC RM_ScanIterator::close()
{

	m_count =0;
	return -1;
}

//This converts a string to Uppercase
string _strToUpper(string str)
{
  const unsigned length = str.length();
  for(unsigned i=0; i<length ; ++i)
  {
    str[i] = std::toupper(str[i]);
  }
  return str;
}
