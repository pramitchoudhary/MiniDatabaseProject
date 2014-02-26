/*
 * Catalog.cc
 *
 *  Created on: Oct 16, 2011
 *	CS222 - Principles of Database Systems
 *		Project 1 - Record Manager Interface (RM)
 *		Coded By: Dexter (#24)
 *
 *
 *	Catalog is a singleton class that supports a number of untility functions around the catalog functionality.
 *	The CATALOG table supported by this implementation has the following structure:
 *	TableName: COLUMNS
 *		TABLENAME VARCHAR[100]
 *		COLUMNNAME VARCHAR[100]
 *		POSITION INTEGER
 *		TYPE INTEGER
 *		LENGTH INTEGER
 *		DROPPED_COLUMN INTEGER
 *
 *	This class supports utility methods to manipulate other objects in the system using a memory representation of the tables defined.
 *
 */

#include <cassert>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>

#include "catalog.h"
#include "pf_interface.h"

Catalog* Catalog::_catalog = 0;


Catalog* Catalog::Instance()
{
    if(!_catalog)
    	_catalog = new Catalog();
    return _catalog;
}

/*
 * The Catalog constructor sets up the Catalog table structure on instantiation.
 * The CATALOG table is opened here and is ready to accept the first create table.
 */
Catalog::Catalog()
{
	// Initialize the variables
	RC rc;
	PF_Interface* _pfi = PF_Interface::Instance();
	_catalogRef.tableName = "CATALOG";

	for (int i=0; i<10; i++)
	{
		_tableRefs[i].tableName = "EMPTY";
	}

	// Setup the Catalog Table's structure that we hold in memory for faster processing
	ColRef colRef;
	{
		colRef.colName="TABLENAME";
		colRef.colPosition = 1;
		colRef.colType = TypeVarChar;
		colRef.colLength = 100;
		colRef.colDelInd = 0;
		colRef.recordOffset = 4;
	}
	_catalogRef.colRefs.push_back(colRef);

	{
		colRef.colName="COLUMNNAME";
		colRef.colPosition = 2;
		colRef.colType = TypeVarChar;
		colRef.colLength = 100;
		colRef.colDelInd = 0;
		colRef.recordOffset = 12;
	}
	_catalogRef.colRefs.push_back(colRef);

	{
		colRef.colName="POSITION";
		colRef.colPosition = 3;
		colRef.colType = TypeInt;
		colRef.colLength = 4;
		colRef.colDelInd = 0;
		colRef.recordOffset = 20;
	}
	_catalogRef.colRefs.push_back(colRef);

	{
		colRef.colName="TYPE";
		colRef.colPosition = 4;
		colRef.colType = TypeInt;
		colRef.colLength = 4;
		colRef.colDelInd = 0;
		colRef.recordOffset = 24;
	}
	_catalogRef.colRefs.push_back(colRef);

	{
		colRef.colName="LENGTH";
		colRef.colPosition = 5;
		colRef.colType = TypeInt;
		colRef.colLength = 4;
		colRef.colDelInd = 0;
		colRef.recordOffset = 28;
	}
	_catalogRef.colRefs.push_back(colRef);

	{
		colRef.colName="DROPPED_COLUMN";
		colRef.colPosition = 6;
		colRef.colType = TypeInt;
		colRef.colLength = 4;
		colRef.colDelInd = 0;
		colRef.recordOffset = 32;
	}
	_catalogRef.colRefs.push_back(colRef);

	// Note that the PF_FileHandle of this object is set only after the OpenCatalog method.
	rc = _pfi->openCatalog();
	if (rc == 0)
		_catalogRef.pfh = _pfi->_cat_handle;
	else
		cout << "ERROR - Unable to create FileHandle for Catalog Table. RC=" << rc << endl;


}

/*
 * This method parses the data buffer passed to it and interprets the content in the buffer using
 * a Catalog Table Reference passed. The Table Reference is a memory representation the table.
 * It will tell us at runtime what the columns and their datatypes are so that the data can be
 * suitably broken up into their constituents.
 */
unsigned Catalog::getRecSizeFromRecordBuffer(TableRef tRef, const void *data)
{
	PF_Interface* _pfi = PF_Interface::Instance();
	list<ColRef>::iterator itColRef; // get the iterator for ColRef

	unsigned fixedSize = 4; // Account for the first 4 bytes containing the number of fields.
	unsigned variableSize = 0;

	ColRef *cr;
	// Count the number of bytes for each field
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();
		fixedSize += 4;
		// Add 4 more if the field is of type Varchar.
		if (cr->colType == TypeVarChar)
		{
			variableSize +=_pfi->getIntFromBuffer(data, fixedSize);
			fixedSize += 4;
		}

	}

	//cout << "Record Data Size = " << fixedSize << " + "<< variableSize << " = " << fixedSize+variableSize << " bytes.";

	return (fixedSize+variableSize);
}



Catalog::~Catalog()
{
}

/*
 * This function reads all the pages in the CATALOG table one after another looking for records belonging to a specific TableName.
 * The columns are all placed into a list so that it can be iterated on.
 *
 */
RC Catalog::scanCatalog(const string tableName, list<ColRef> &colRefs)
{
	// Loop through all the data records in all the pages.
	// If there is a record for the specified table name
	//    Split up the data record and identify the fields
	//    Create a ColumnRef object
	//    Add the object in to a vector

	RC rc;
	Catalog *_ci = Catalog::Instance();
	PF_Interface* _pfi = PF_Interface::Instance();
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

		// Next go through each data record in the page and determine its content
		noOfSlotsInPage =_pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
		currSlotPtr = PF_PAGE_SIZE-8-8; // Position of the first slot pointer
		currRecordPtr = 0;
		for (unsigned j=0; j<noOfSlotsInPage; j++)
		{
			currRecordPtr = _pfi->getIntFromBuffer(buffer, currSlotPtr); // This is topositionon where to read from the buffer
			recordSize = _pfi->getIntFromBuffer(buffer, currSlotPtr+4); // Now you know how many bytes toread from the buffer
			if (currRecordPtr<0) // Deleted Catalog records can exist in the table.
			{
				cout<< "  DELETED ENTRY...Page# " << i << "  Slot# " << j+1 << " has " << recordSize <<" bytes at " << currRecordPtr << " ."  << endl;
				currSlotPtr -=8; // Position on to the next Slot
				continue;
			}
			cout<< "  Page# " << i << "  Slot# " << j+1 << " has " << recordSize <<" bytes at " << currRecordPtr << " ."  << endl;

			// Do a byte-byte copy from buffer to recordData upto the size of the record
			// copying byte by byte into the buffer
		    for(unsigned k = 0; k < recordSize; k++)
		    {
		    	*((char *)recordData+k) = *((char *)buffer+currRecordPtr+k);
		    }

			currColRef = _ci->convertCatalogRecordDataToColRef(_strToUpper(tableName), recordData,recordSize);

			// If the record belongs to the table, then it is added to the vector.
			if (currColRef.colName.compare("SKIP") == 0) // meaning that the column does not belong to this table.
			{

			}
			else
			{
				// add this column to the vector
				colRefs.push_back(currColRef);
			}
			currSlotPtr -=8; // Position on to the next Slot
		}
	}

	// Sorting the collection helps as you can then get thesein thelist in the order in which data will be stored.
	colRefs.sort(_sortColRefByPosition);

	// These steps populate the record offset based on the columns how they are now stored (ie. sorted by Position)
	list<ColRef>::iterator itColRef; // get the iterator for ColRef
	unsigned recordOffset = 4; // Account for the first 4 bytescontaining the number of fields.
	ColRef *cr;
	for (itColRef = colRefs.begin(); itColRef != colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();
		cr->recordOffset = recordOffset;
		recordOffset += 4;
		if (cr->colType == TypeVarChar)
		{
			// Add 4more bytes for each varchar
			recordOffset += 4;
		}
	}


	free(buffer);
	return 0;
}

/*
 * This is a simple converter of a Catalog column entry into a ColRef object so that it can be passed around for further processing.
 */
ColRef Catalog::convertCatalogRecordDataToColRef(const string tableName, const void *recordData, unsigned recordSize)
{
	PF_Interface *_pfi = PF_Interface::Instance();

	ColRef cR; // This will be returned
	void *stringData = malloc(200);
	unsigned tempVal = 0;
	unsigned tempPtr = 0;
	string str = "";


	// We know what the Catalog data record looks like and so simply split it into its constituents and return the ColRef
	// Skip the first 4 bytes... For catalog data has 6 fields always

	tempPtr = _pfi->getIntFromBuffer(recordData, 4);
	tempVal = _pfi->getIntFromBuffer(recordData, 8);
    str = _pfi->getVarcharFromBuffer(recordData, tempPtr, tempVal);
	//cout << "Found Catalog record for table :" << str << "." << endl;

	if (str.compare(tableName) == 0)
	{
		// Found a record for this table...
		// Fething column name
		tempPtr = _pfi->getIntFromBuffer(recordData, 12);
		tempVal = _pfi->getIntFromBuffer(recordData, 16);
		str = _pfi->getVarcharFromBuffer(recordData, tempPtr, tempVal);
		cR.colName.assign(str);
		// Fething column position
		cR.colPosition = _pfi->getIntFromBuffer(recordData, 20);
		// Fething column type
		cR.colType = (AttrType)_pfi->getIntFromBuffer(recordData, 24);
		// Fething column length
		cR.colLength = _pfi->getIntFromBuffer(recordData, 28);
		// Fething column deleteInd
		cR.colDelInd = _pfi->getIntFromBuffer(recordData, 32);

		cR.recordOffset = 0; // will beassigned once we get the collection sorted.

		free(stringData);
		return cR;

	}
	else
	{
		cout << "   Column Record skipped"  << endl;
		// You will be returning anull ColRef here indicating that the record should be skipped.
		free(stringData);
		cR.colName.assign("SKIP");
		return cR;
	}
	//cout<< "Should not come here" << endl;
	free(stringData);
	return cR;
}

/*
 * This method is used to populate the Catalog table with suitable values for all the columns that are not passed by the user.
 */
RC Catalog::addCatalogTuple(void *data)
{
	RC rc;
	PF_Interface *_pfi = PF_Interface::Instance();
	Catalog *_ci = Catalog::Instance();

	unsigned pageNum = 0;
    void *buffer = malloc(PF_PAGE_SIZE);
	unsigned freePtr = 0;
	unsigned noOfSlots = 0;
	unsigned newRecSize = 0;
	unsigned newSlotPtr = 0;
	unsigned newSlotSize;


	// Note that the record size is not passed here. You should find that out from the data record.
	// To do that, you must first parse the content of the record... here is how...
	//  from the TableRef object add the length of the Fixed Length fields.
	//  from the TableRef object find size of each varchar and determine the variable-length portion in the data to beinserted.
	//  The sum of these gives you the new RecordSize
	newRecSize = _ci->getRecSizeFromRecordBuffer(_ci->_catalogRef, data);
	cout<< "NewRecordSize=" << newRecSize << endl;

	// Locate Free Page in CATALOG table assuming it is Page 0 for now.
	pageNum = _pfi->locateNextFreePage(_ci->_catalogRef.pfh, newRecSize);
	if (pageNum < 0)
	{
		cout << "ERROR - Unable to locate next free page from Catalog Table. RC=" << rc << endl;
		free(buffer);
		return -1;
	}

    rc = _pfi->getPage(_ci->_catalogRef.pfh, pageNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to read Page from Catalog Table. RC=" << rc << endl;
		free(buffer);
		return -1;
	}

	// Locate the Free Pointer
	freePtr = _pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-4);
	// Find the number of slots in the slot directory
	noOfSlots = _pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);

	//cout<< "FreePointer=" << freePtr << "; NoOfSlots=" << noOfSlots << endl;

	// Write the tuple into the page at freePtr
    _pfi->addVarcharToBuffer(buffer, freePtr, data, newRecSize);
    //	strncpy((char *)buffer+freePtr, (char *)data, newRecSize);

	// Find the position of the new Slot
	// compute the value of newSlotPtr
	newSlotPtr = PF_PAGE_SIZE - 8 // accounting for the free space pointer & no of slots
			- 8 * noOfSlots // each slot occupies 4 bytes for offset and 4 more for size
			- 8; // start position for the new slot
	//cout<< "NewSlotPointer=" << newSlotPtr << endl;

	newSlotSize = newRecSize;

	// copy current freePtr at location: newSlotPtr
	_pfi->addIntToBuffer(buffer, newSlotPtr, freePtr);
	// copy newSlotSize at location: newSlotPtr+4
	_pfi->addIntToBuffer(buffer, newSlotPtr+4, newSlotSize);
	noOfSlots += 1;
	freePtr += newRecSize;
	// Write freePtr at location: PF_PAGE_SIZE-4
	_pfi->addIntToBuffer(buffer, PF_PAGE_SIZE-4, freePtr);
	// Write noOfSlots at location: PF_PAGE_SIZE-8
	_pfi->addIntToBuffer(buffer, PF_PAGE_SIZE-8, noOfSlots);

	// Write the page back
    rc = _pfi->putPage(_ci->_catalogRef.pfh, pageNum, buffer);
	if (rc != 0)
		cout << "ERROR - Unable to write page into Catalog Table. RC=" << rc << endl;

	// free malloc
	free(buffer);
	return 0;
}

// This should mark the tuple as logically deleted
RC Catalog::deleteCatalogTuple(const void *data)
{
	return 0;
}

/*
 * This method removes the entry from the Table Reference Cache held in the memory.
 * This is typically important for us to refresh the TableRef object due to changes in the table structure.
 */
RC Catalog::releaseTableRef(const string tableName)
{
	string upperTableName = _strToUpper(tableName);
	PF_Interface *_pfi = PF_Interface::Instance();
	//Catalog *_ci = Catalog::Instance();
	TableRef tRef;
	int i = 0;
	RC rc;
	if (upperTableName.compare("CATALOG") == 0)
	{
		cout << "Catalog Reference Requested. This entry cannot be released. No action taken." << endl;
		return 0;
	}


	for (i=0; i<10; i++)
	{
		if ((_tableRefs[i].tableName.compare("EMPTY")) == 0)
		{
			continue;
		}

		// This means there is a Table Reference that exists and must be released.
		if (_tableRefs[i].tableName.compare(upperTableName)==0)
		{
			rc = _pfi->closeTable(_tableRefs[i].pfh);

			//You need to erase the contents of _tableRefs[i].colRefs
			while (!_tableRefs[i].colRefs.empty())
			{
				_tableRefs[i].colRefs.pop_front();
			}

			_tableRefs[i].tableName.assign("EMPTY");
		}
	}
	return 0;
}

/*
 * This is a data conversion routine to convert a TUPLE_Format used by the calling programs in RM to an internal Catalog format.
 */
void Catalog::convertTupleFormatToRecordFormat(TableRef tRef, const void *tupleData, void *recordData)
{
	PF_Interface *_pfi = PF_Interface::Instance();
	// This code is based on the following comment:
	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()

	// The tupleData will always be based on the current list of columns and types in the database
	// Assume that sufficient memory has been allocated for the recordData where you will be writing.

	unsigned noOfFields = 0;
	unsigned recPtr = 0; // Offset Pointer in the record
	unsigned varRecPtr= 0;
	unsigned tupPtr = 0; // Offset Pointer in the tuple

	// These steps populate the record offset based on the columns how they are now stored (ie. sorted by Position)
	list<ColRef>::iterator itColRef; // get the iterator for ColRef
	ColRef *cr;
	unsigned fixedSize = 4;
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();
		fixedSize += 4;
		if (cr->colType == TypeVarChar)
			fixedSize += 4;
	}
	varRecPtr = fixedSize;

	// We will always write as many fields as the catalog says
	_pfi->addIntToBuffer(recordData, recPtr, tRef.colRefs.size());

	int i = 0;
	int varFieldSize =0;
	float f=0;

	recPtr = 4; // start writing from the 4th byte
	tupPtr = 0;
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();

		if (cr->colDelInd == 1) // If a field is deleted then it won't be passed!
		{
			continue;
		}
		else
		{
			noOfFields++; // May be needed to handle dropped columns on the return
		}

		if (cr->colType == TypeInt)
		{
			i = _pfi->getIntFromBuffer(tupleData, tupPtr);
			_pfi->addIntToBuffer(recordData, recPtr, i);
			recPtr += 4;
			tupPtr += 4;
			i = 0;
		} else if (cr->colType == TypeReal)
		{
			f = _pfi->getFloatFromBuffer(tupleData, tupPtr);
			_pfi->addFloatToBuffer(recordData, recPtr, f);
			recPtr += 4;
			tupPtr += 4;
			f = 0;

		} else // cr->colType == TypeVarChar
		{
			// Read 4 bytes for the varchar
			// Then read that many bytes
			varFieldSize = _pfi->getIntFromBuffer(tupleData, tupPtr); // this is the size of the string data I read
			tupPtr += 4;
			_pfi->addIntToBuffer(recordData, recPtr, varRecPtr); // Write the record offset for varchar
			recPtr += 4;
			_pfi->addIntToBuffer(recordData, recPtr, varFieldSize); // Write the #of bytes for varchar
			recPtr += 4;

			string s = _pfi->getVarcharFromBuffer(tupleData, tupPtr, varFieldSize);

			//_pfi->addVarcharToBuffer(recordData, varRecPtr, s.c_str(), i); // write the characters
			// copying byte by byte into the buffer
		    for(int k = 0; k < varFieldSize; k++)
		    {
		    	*((char *)recordData+varRecPtr+k) = *((char *)tupleData+tupPtr+k);
		    }


			tupPtr += varFieldSize;
			varRecPtr += varFieldSize;
		}
	}

}

/*
 * This is a data conversion routine to convert an internal Catalog format to a TUPLE_Format used by the calling programs in RM.
 */
void Catalog::convertRecordFormatToTupleFormat(TableRef tRef, const void *recordData, void *tupleData)
{
	/*
	 * Logic:
	 * Find thenumber of columns the record has
	 * Compare with the number of cols in catalog
	 * If catCols >= recCols
	 *    Some columns have been deleted
	 *    Copy only the columns that are NOT deleted into tupleformat
	 * (recCols > catCols --- condition isimpossible)
	 *
	 */



	PF_Interface *_pfi = PF_Interface::Instance();
	// This code is based on the following comment:
	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()

	// The tupleData will always be based on the current list of columns and types in the database
	// Assume that sufficient memory has been allocated for the recordData where you will be writing.

	unsigned recFields = 0;
	unsigned catFields = 0;
	unsigned recPtr = 0; // Offset Pointer in the record
	unsigned tupPtr = 0; // Offset Pointer in the tuple

	// These steps populate the record offset based on the columns how they are now stored (ie. sorted by Position)
	list<ColRef>::iterator itColRef; // get the iterator for ColRef
	ColRef *cr;
	unsigned fixedSize = 4;
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();
		fixedSize += 4;
		catFields++;
		if (cr->colType == TypeVarChar)
			fixedSize += 4;
	}

	// We will always write as many fields as the catalog says
	recFields = _pfi->getIntFromBuffer(recordData, recPtr);

	int i=0;
	int varFieldSize =0;
	int varFieldOffset = 0;
	float f=0;

	recPtr = 4; // start writing from the 4th byte
	tupPtr = 0;
	for (itColRef = tRef.colRefs.begin(); itColRef != tRef.colRefs.end(); ++itColRef)
	{
		cr = itColRef.operator ->();

		if (cr->colDelInd == 1) // If a field is deleted then it won't be passed!
		{
			continue;
		}

		if (cr->colType == TypeInt)
		{
			i = _pfi->getIntFromBuffer(recordData, recPtr);
			_pfi->addIntToBuffer(tupleData, tupPtr, i);
			recPtr += 4;
			tupPtr += 4;
			i = 0;
		} else if (cr->colType == TypeReal)
		{
			f = _pfi->getFloatFromBuffer(recordData, recPtr);
			_pfi->addFloatToBuffer(tupleData, tupPtr, f);
			recPtr += 4;
			tupPtr += 4;
			f = 0;

		} else // cr->colType == TypeVarChar
		{
			// Read 4 bytes for the varchar
			// Then read that many bytes
			varFieldOffset = _pfi->getIntFromBuffer(recordData, recPtr); // this is the offset of the string data I write
			recPtr += 4;

			varFieldSize = _pfi->getIntFromBuffer(recordData, recPtr); // this is the size of the string data I write
			recPtr += 4;
			_pfi->addIntToBuffer(tupleData, tupPtr, varFieldSize); // Write the #of bytes for varchar
			tupPtr += 4;

			// copying byte by byte into the buffer
		    for(int k = 0; k < varFieldSize; k++)
		    {
		    	*((char *)tupleData+tupPtr+k) = *((char *)recordData+varFieldOffset+k);
		    }
		    varFieldOffset = 0;

			tupPtr += varFieldSize;
		}
	}

}

/*
 * This is a method that provides a memory reference to the table structure defined earlier.
 * The TableRef object is used to recognize the constituents of the data records or to format them accordingly.
 */
TableRef Catalog::getTableRef(const string tableName)
{
	string upperTableName = _strToUpper(tableName);
	PF_Interface *_pfi = PF_Interface::Instance();
	Catalog *_ci = Catalog::Instance();
	TableRef tRef;
	int i = 0;
	int iCacheEntry = -1;

	if (upperTableName.compare("CATALOG") == 0)
	{
		cout << "Catalog Reference Requested. No entry used from cache." << endl;
		return _ci->_catalogRef;
	}

	for (i=0; i<10; i++)
	{
		if ((_tableRefs[i].tableName.compare("EMPTY")) == 0)
		{
			iCacheEntry = i; // You can use this empty location to place the TableRef if this is the first time.
			continue;
		}

		// This means there is a Table Reference that can be used.
		if (_tableRefs[i].tableName.compare(upperTableName)==0)
		{
			//cout << "Table Entry has been cached already. Returning cache entry for..." << _tableRefs[i].tableName << endl;
			return _tableRefs[i];
		}
	}

	// A new TableRef object needs to be prepared...
	(tRef.tableName).assign(upperTableName);
	_ci->scanCatalog(upperTableName, tRef.colRefs);
	_pfi->openTable(upperTableName,tRef.pfh);

	// update an empty cache entry
	if (iCacheEntry != -1)
	{
		_tableRefs[iCacheEntry] = tRef;
	}

	return tRef;
}


// This function allows us to sort the collection of ColRefs by Position
bool _sortColRefByPosition(ColRef cA, ColRef cB)
{
	if (cA.colPosition < cB.colPosition)
	{
		return true;
	}
	else
	{
		return false;
	}
}

