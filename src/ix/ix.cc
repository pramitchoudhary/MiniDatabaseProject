/*
 * ix.cc
 *
 *  Created on: Nov 19, 2011
 *	CS222 - Principles of Database Systems
 *		Project 2 - Index Manager Interface (IX)
 *		Coded By: Dexter (#24)
 *
 *	This is the main implimentation class for the IX layer. It contains all the APIs identified for the IX later.
 *	It is designed to use bulk of functionality provided inside the IXPF class for the Paged File operations.
 */

#include <fstream>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <cctype>       // std::toupper
#include <math.h>

#include "ix.h"
#include "ixpf.h"

IX_Manager* IX_Manager::_ix_manager = 0;


IX_IndexHandle::IX_IndexHandle()
{
	ix_pfHandleForIndexPage = NULL;
}


IX_IndexHandle::~IX_IndexHandle()
{

}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)  // Delete index entry
{
	RC rc;
	//PF_Interface *pfi = PF_Interface::Instance();
	IXPF *ixpf = IXPF::Instance();
	IndexRef iRef = ixpf->getIndexRef(this->ix_indexName);
	IX_KeyData inputKD;


	// Initialize Child Entry for the recursive part of the algorithm
	IX_KeyData newChildEntry;
	inputKD.rid.pageNum=rid.pageNum;
	inputKD.rid.slotNum=rid.slotNum;
	newChildEntry.rid.pageNum=0;
	newChildEntry.rid.slotNum=0;
	AttrType attType = iRef.colRefs.front().colType;


	if (attType == TypeInt)
	{
		newChildEntry.val.iValue=0;
		memcpy(inputKD.val.uData, (char *)key, 4);
		cout << " Input:  Key Value = " << inputKD.val.iValue << " RID=[" << rid.pageNum << "|" << rid.slotNum << "]" << endl;
	}
	else if (attType == TypeReal)
	{
		newChildEntry.val.fValue=0.0;
		memcpy(inputKD.val.uData, (char *)key, 4);
		cout << " Input:  Key Value = " << inputKD.val.fValue << endl;
	}
	else // Type can be Varchar but we have not implemented it.
	{
		cout << "ERROR : - Index Attribute Type of VarChar is not supported by this implementation." << endl;
		return -1;
	}

	//RID keyRid;
	RID rootRid = ixpf->getRootRID(iRef.indexName);

	IX_Node iN; // temp node.
	ixpf->getRoot(iRef.indexName, iN);
	RID collapseRid;
	collapseRid.pageNum=0; collapseRid.slotNum=0;

	rc = ixpf->deleteKey(iRef.indexName, iN.noOfKeys, rootRid, inputKD, collapseRid);

	//cout << "**** Key inserted************************" << endl;

	if (rc < 0)
		IX_PrintError(rc-54); // All Error codes for insert entry start at -55!
	return rc;
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)  // Insert new index entry
{
	RC rc =0;
	//PF_Interface *pfi = PF_Interface::Instance();
	IXPF *ixpf = IXPF::Instance();
	IndexRef iRef = ixpf->getIndexRef(this->ix_indexName);
	IX_KeyData inputKD;


	// Initialize Child Entry for the recursive part of the algorithm
	IX_KeyData newChildEntry;
	inputKD.rid.pageNum=rid.pageNum;
	inputKD.rid.slotNum=rid.slotNum;
	newChildEntry.rid.pageNum=0;
	newChildEntry.rid.slotNum=0;
	AttrType attType = iRef.colRefs.front().colType;


	if (attType == TypeInt)
	{
		newChildEntry.val.iValue=0;
		memcpy(inputKD.val.uData, (char *)key, 4);
		cout << " Input:  Key Value = " << inputKD.val.iValue << " RID=[" << rid.pageNum << "|" << rid.slotNum << "]" << endl;
	}
	else if (attType == TypeReal)
	{
		newChildEntry.val.fValue=0.0;
		memcpy(inputKD.val.uData, (char *)key, 4);
		cout << " Input:  Key Value = " << inputKD.val.fValue << endl;
	}
	else // Type can be Varchar but we have not implemented it.
	{
		cout << "ERROR : - Index Attribute Type of VarChar is not supported by this implementation." << endl;
		IX_PrintError(IX_RC_IE_001);
		return IX_RC_IE_001;
	}

	RID rootRid = ixpf->getRootRID(iRef.indexName);
	rc = ixpf->insertKey(iRef.indexName, rootRid, inputKD, newChildEntry);

	//cout << "**** Key inserted************************" << endl;

	if (rc < 0)
		IX_PrintError(rc-40); // All Error codes for insert entry start at -41!
	return rc;

}

IX_Manager *IX_Manager::Instance()
{
    if(!_ix_manager)
    	_ix_manager = new IX_Manager();

    return _ix_manager;
}

IX_Manager::IX_Manager()
{
}

IX_Manager::~IX_Manager()
{
}


RC IX_Manager::CreateIndex(const string tableName, const string attributeName)
{

	// You need to create the column entries in the Catalog table
	// Convert the information into a data record and call insertCatalogTuple()
	RC rc;
	PF_Interface *pfi = PF_Interface::Instance();
	IXPF *ixpf = IXPF::Instance();
	string upperIndexName = _strToUpper(tableName + "_" + attributeName);
	Catalog *cat = Catalog::Instance(); // This opens the catalog also
	RM *rm = RM::Instance(); // This allows the use of the RM methods

	vector<Attribute> attrs;

	if (pfi->isPagedFileCreated(upperIndexName))
	{
		cout << "ERROR - Index exists on the disk already. Cannot create again." << endl;
		IX_PrintError(IX_RC_CI_001);
		return IX_RC_CI_001;
	}


	rc = rm->getAttributes(tableName, attrs);
	if (rc < 0)
	{
		cout << "ERROR - Table [" << tableName << "] could not be located in the Catalog" << endl;
		IX_PrintError(IX_RC_CI_002);
		return IX_RC_CI_002;
	}

	// Now loop through the attributes and get the datatype
	void *data = malloc(400); // Give enough room for TableName, ColumnName and the other data items

	vector<Attribute>::const_iterator itAttrs; // get the iterator for Attributes
	const Attribute *attr;

	unsigned i = 0;
	unsigned fldPtr = 0; // FieldPointer
	unsigned currRecordEnd = 36; // CurrentRecordEnd is the end of the fixed portion of the record.
	string upperColumnName;
	string upperAttributeName = _strToUpper(attributeName);
	bool colFound = false;

	for (itAttrs = attrs.begin(); itAttrs != attrs.end(); ++itAttrs)
	{
		i++;
		attr = itAttrs.operator ->();
		upperColumnName = _strToUpper(attr->name);

		cout << attr->name << " | " << attr->type << " | " << attr->length << endl;
		if (upperColumnName.compare(upperAttributeName) == 0)
		{
			cout << "Found Attribute: " << attr->name << " | " << attr->type << " | " << attr->length << endl;
			colFound = true;
			break;
		}
		else
		{
			continue;
		}

	}

	if (!colFound)
	{
		free(data);
		cout << "ERROR - Column [" << attributeName << "] could not be located in the Catalog for the table" << endl;
		IX_PrintError(IX_RC_CI_003);
		return IX_RC_CI_003;
	}

	if (attr->type == TypeVarChar)
	{
		free(data);
		cout << "ERROR - Column [" << attributeName << "] is of Type VarChar. Index cannot be built on this column." << endl;
		IX_PrintError(IX_RC_CI_004);
		return IX_RC_CI_004;
	}


	// At this point attr is pointing to the column name we need.
	pfi->addIntToBuffer(data, fldPtr, 6);//0-3 - For the Field Count
	fldPtr+=4;

	pfi->addIntToBuffer(data, fldPtr, currRecordEnd);//4-7 - For the column TABLENAME
	fldPtr+=4;
	pfi->addIntToBuffer(data, fldPtr, upperIndexName.length());//8-11 - Size of the Varchar TABLENAME
	fldPtr+=4;
	pfi->addVarcharToBuffer((char *)data, currRecordEnd, (void *)upperIndexName.c_str(), upperIndexName.length());// In the Variable Length portion
	currRecordEnd += upperIndexName.length(); // Next spot where you can add Varchar data

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


	ixpf->releaseIndexRef(upperIndexName); // Really may not be necessary

	ixpf->getIndexRef(upperIndexName); // This will create an empty table


	free(data);
	return 0;
}

RC IX_Manager::DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName)
{
	RC rc;
	string upperIndexName = _strToUpper(tableName + "_" + attributeName);
	IXPF *ixpf = IXPF::Instance();
	Catalog *_ci = Catalog::Instance();
	PF_Interface* _pfi = PF_Interface::Instance();

	ixpf->releaseIndexRef(upperIndexName);
	ixpf->removeIndex(upperIndexName);

	// Now you need to remove the entry in the Catalog for the index.
	void *buffer = malloc(PF_PAGE_SIZE);

	// First find the number of pages to loop through
	unsigned noOfPages = _ci->_catalogRef.pfh.GetNumberOfPages();
	unsigned noOfSlotsInPage = 0;
	unsigned recordSize = 0;
	unsigned currSlotPtr = 0;
	int currRecordPtr = 0;
	ColRef currColRef;

	// Technically the largest recordsize can be PF_PAGE_SIZE-16. Make sure you have room for this.
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
			IX_PrintError(IX_RC_DI_001);
			return IX_RC_DI_001;
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
			currColRef = _ci->convertCatalogRecordDataToColRef(upperIndexName, recordData,recordSize);
			if (currColRef.colName.compare("SKIP") == 0) // meaning that the column does not belong to this table.
			{
				cout<< "  Catalog entry does not belong to this table.Skipping." << endl;
				// This catalog record does not belong to the table. Skip it.
			}
			else
			{
				cout<< "  DELETING Catalog entry for :" << upperIndexName << " | " << currColRef.colName << endl;
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
			// It should never come here and so clean up and exit gracefully
			cout << "ERROR - Unable to write Page " << i << " from Catalog Table. RC=" << rc << endl;
			free(buffer);
			IX_PrintError(IX_RC_DI_002);
			return IX_RC_DI_002;
		}

	}


	free(buffer);
	return 0;

}

RC IX_Manager::OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle)
{
	RC rc = 0;
	string upperIndexName = _strToUpper(tableName + "_" + attributeName);
	IXPF *ixpf = IXPF::Instance();
	IndexRef iRef = ixpf->getIndexRef(upperIndexName); // This will open the Index.

	if(iRef.colRefs.size()==0)
	{
		cout << "ERROR - No such index - Aborting." << upperIndexName << endl;
		IX_PrintError(IX_RC_OI_001);
		return IX_RC_OI_001;
	}

	if(rc == 0)
	{
		indexHandle.ix_pfHandleForIndexPage = &(iRef.pfh);
		indexHandle.ix_indexName = upperIndexName;
	}

	return rc;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)  // close index
{
	RC rc = -1;
	IXPF *ixpf = IXPF::Instance();
	IndexRef iRef = ixpf->getIndexRef(indexHandle.ix_indexName);

	rc = ixpf->releaseIndexRef(indexHandle.ix_indexName);
	// We leave it to releaseIndexRef to close the file.

	return rc;

}



IX_IndexScan::IX_IndexScan()
{
	if(m_scanRecord.size() > 0)
		m_scanRecord.clear();
}

IX_IndexScan::~IX_IndexScan()
{

}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle,
	      CompOp      compOp,
	      void        *value)
{
	RC rc=-1;
	RID rid;
	float dntAllowValue =0;
	float emptyValue =0;

	if(m_scanRecord.size() > 0)
		m_scanRecord.clear();

	bool validate = false;
	IXPF *ixpf = IXPF::Instance();

	IX_KeyValue inputKD;

	vector<IX_KeyData>::const_iterator iter;

	if(!indexHandle.ix_indexName.empty())
	{
		IndexRef iRef = ixpf->getIndexRef(indexHandle.ix_indexName);

		AttrType attType = iRef.colRefs.front().colType;
		if(value!= NULL)
		{
			if (attType == TypeInt)
			{
				memcpy(inputKD.uData, (char *)value, 4);
				m_type = TypeInt;
			}
			else if (attType == TypeReal)
			{
				memcpy(inputKD.uData, (char *)value, 4);
				m_type = TypeReal;

			}
			else // Type can be Varchar but we have not implemented it.
			{
				cout << "ERROR : - Index Attribute Type of VarChar is not supported by this implementation." << endl;
				return -1;
			}
		}
		else
		{
			inputKD.iValue = 0;
			inputKD.fValue = 0;

		}

		// rid returned is a leaf
		rid = ixpf->find(indexHandle.ix_indexName, inputKD);
		while(rid.pageNum > 0 && rid.slotNum > 0)
		{
			bool bLeafEntry;
			IX_Node iN;
			IX_Leaf iL;
			rc = ixpf->getIndexEntry(indexHandle.ix_indexName, rid, bLeafEntry, iN, iL);
			iter = iL.keyDataList.begin();
			while((iter !=iL.keyDataList.end()))
			{
				if(m_type == TypeInt || (value == NULL))
				{
					dntAllowValue = iter->val.iValue;
					emptyValue = IX_NOVALUE;
				}
				else
				{
					dntAllowValue = iter->val.fValue;
					emptyValue = IX_HIFLOAT;
				}

				if(dntAllowValue != emptyValue )
				{
					validate = validateData(compOp, inputKD, iter->val);
					if(validate)
					{
						m_scanRecord.push_back(iter->rid);

					}
				}
				iter++;
			}
			// Now the other Leaf nodes satisfying the condition
			if(compOp == LT_OP || compOp == LE_OP )
				rid = iL.prevPtr;
			else
				rid = iL.nextPtr;
		}
		cout << "Scan Completed......"<< endl;
		m_itr = m_scanRecord.begin();
		rc = 0;
	}

	return rc;
}



bool IX_IndexScan::validateData (CompOp opObj, IX_KeyValue compareVal, IX_KeyValue retrievedData)
{
	bool validate = false;
	switch(opObj)
	{
	case EQ_OP:
		if(m_type == TypeInt)
		{
			if(retrievedData.iValue == compareVal.iValue)
			{
				validate = true;
			}
		}
		else
		{
			if(fabs(compareVal.fValue- retrievedData.fValue) <= IX_TINY)
				validate = true;
		}
		break;
	case LT_OP:
		if(m_type == TypeInt)
		{
			if(retrievedData.iValue < compareVal.iValue)
			{
				validate = true;
			}
		}
		else
		{
			if(retrievedData.fValue < compareVal.fValue )
				validate = true;
		}
		break;
	case GT_OP:
		if(m_type == TypeInt)
		{
			if(retrievedData.iValue > compareVal.iValue)
			{
				validate = true;
			}
		}
		else
		{
			if(retrievedData.fValue > compareVal.fValue)
			{
				validate = true;
			}
		}
		break;
	case LE_OP:
		if(m_type == TypeInt)
		{
			if(retrievedData.iValue <= compareVal.iValue)
			{
				validate = true;
			}
		}
		else
		{
			if(retrievedData.fValue <= compareVal.fValue)
			{
				validate = true;
			}
		}
		break;
	case GE_OP:
		if(m_type == TypeInt)
		{
			if(retrievedData.iValue >= compareVal.iValue)
			{
				validate = true;
			}
		}
		else
		{
			if(retrievedData.fValue >= compareVal.fValue)
			{
				validate = true;
			}
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


RC IX_IndexScan::GetNextEntry(RID &rid)
{

	RC rc =-1;
	if(m_itr!= m_scanRecord.end())
	{
		rid.pageNum = m_itr->pageNum;
		rid.slotNum = m_itr->slotNum;
		rc = 0;
	}
	m_itr++;
	if(rc < 0)
	{
		cout << " No more record to access"<< endl;
	}
/*	if (rc<0)
	{
		cerr << "In Get Next entry... ";
		IX_PrintError(rc);
	}*/
	return rc;
}


RC IX_IndexScan::CloseScan()
{

	m_scanRecord.clear();
	return 0;
}


void IX_PrintError (RC rc)
{
	// These return codes are defined in bree.h

	// PrintError should print only if the return code is less than 0.
	if (rc>=0)
		return;

	switch (rc)
	{
		// RC for Create Index
		case  IX_RC_CI_001:    // -11
		{
			cerr << "Index exists on the disk already. Cannot create again. RC = " << rc << endl;
			break;
		}
		case  IX_RC_CI_002:    // -12
		{
			cerr << "Table could not be located in the Catalog. RC = " << rc << endl;
			break;
		}
		case  IX_RC_CI_003:    // -13
		{
			cerr << "Column/Attribute Name could not be located in the Catalog for the table. RC = " << rc << endl;
			break;
		}
		case  IX_RC_CI_004:    // -14
		{
			cerr << "Column/Attribute is of Type VarChar. Index cannot be built on this column. RC = " << rc << endl;
			break;
		}
		// RC for Destroy Index
		case  IX_RC_DI_001:    // -21
		{
			cerr << "Unable to read a page from Catalog Table. RC = " << rc << endl;
			break;
		}
		case  IX_RC_DI_002:    // -22
		{
			cerr << "Unable to read a page from Catalog Table. RC = " << rc << endl;
			break;
		}
		// RC for Open Index
		case  IX_RC_OI_001:    // -31
		{
			cerr << "Unable to find the Index requested to Open. RC = " << rc << endl;
			break;
		}
		// RC for Insert Entry
		case  IX_RC_IE_001:    // -41
		{
			cerr << "Index Attribute Type of VarChar is not supported by this implementation. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_002:    // -42
		{
			cerr << "Index Leaf Node was full. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_003:    // -43
		{
			cerr << "Key match has failed. Perhaps a Rebuild is required. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_004:    // -44
		{
			cerr << "Update Node Data has failed. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_005:    // -45
		{
			cerr << "Index Leaf Entry not found. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_006:    // -46
		{
			cerr << "Index Leaf Entry Update has failed. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_007:    // -47
		{
			cerr << "Cannot Insert a new Leaf after Split. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_008:    // -48
		{
			cerr << "Cannot Update existing Leaf after Split. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_009:    // -49
		{
			cerr << "Cannot Update existing Leaf for the Prev Pointer after Split. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_010:    // -50
		{
			cerr << "Duplicate Key Insert is not supported. RC = " << rc << endl;
			break;
		}
		case  IX_RC_IE_011:    // -51
		{
			cerr << "Index non-leaf Node Entry was not found. RC = " << rc << endl;
			break;
		}
		// RC for DeleteEntry
		case  IX_RC_DE_001:    // -55
		{
			cerr << "Index Tree is Empty / Null. RC = " << rc << endl;
			break;
		}
		case  IX_RC_DE_002:    // -56
		{
			cerr << "Cannot locate non-leaf Index Node. RC = " << rc << endl;
			break;
		}
		case  IX_RC_DE_003:    // -57
		{
			cerr << "Key Match has failed. Perhaps a Rebuild is required. RC = " << rc << endl;
			break;
		}
		case  IX_RC_DE_004:    // -58
		{
			cerr << "Cannot locate Leaf Index Node. RC = " << rc << endl;
			break;
		}
		case  IX_RC_DE_005:    // -59
		{
			cerr << "Error during removal of keyData from Leaf Node. RC = " << rc << endl;
			break;
		}
		// RC for Index Scan
		// RC for Open Scan
		// RC for Close Scan
		case  IX_RC_SCAN_01:    // -61
		case  IX_RC_SCAN_02:    // -62
		case  IX_RC_SCAN_03:    // -63
		case  IX_RC_SCAN_04:    // -64
		case  IX_RC_SCAN_05:    // -65
		// RC for Get Next Entry
		case  IX_RC_GNE_01:    // -71
		case  IX_RC_GNE_02:    // -72
		case  IX_RC_GNE_03:    // -73
		case  IX_RC_GNE_04:    // -74
		case  IX_RC_GNE_05:    // -75
		case -1:
		{
			cerr << "Unrecoverable Error Encountered. Please contact HelpDesk. RC = " << rc << endl;
			break;
		}
	}
	/*
	if(rc!=0)
	{
		if(rc == -1)
		{
			cerr <<"Unexpected error occurred. Please bear with us."<< endl;
		}
		else if((rc ==-2) || (rc==-3))
		{
			cerr <<"Error related to page access. Please be Patient"<< endl;
		}
		else if(rc ==-5)
			cerr <<"Error related to Catalog file access. Please be Patient"<< endl;

	}
	*/
}
