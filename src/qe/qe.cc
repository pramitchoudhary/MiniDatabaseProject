
#include "qe.h"

#include <iostream>
#include <sstream>
#include <math.h>
#include <sys/stat.h>
#include <time.h>




Filter::Filter(Iterator *input,                         // Iterator of input R
       const Condition &condition               // Selection condition
)
{
	m_input = input;
	// Maintain a copy of the condition used i the constructor
	m_condition.bRhsIsAttr = condition.bRhsIsAttr;
	m_condition.lhsAttr = condition.lhsAttr;
	m_condition.op = condition.op;
	m_condition.rhsAttr = condition.rhsAttr;

	if (m_condition.bRhsIsAttr)
	{
		cout << "ERROR - Filter Condition cannot take a condition comparing two Attribute objects." << endl;
	}


	if (m_condition.rhsValue.type == TypeVarChar)
	{
		int len = *(int *)((char *)m_condition.rhsValue.data);
		m_condition.rhsValue.data = malloc(sizeof(int) + len);
		memcpy(m_condition.rhsValue.data, (char *)condition.rhsValue.data, sizeof(int)+len);
		m_condition.rhsValue.type = condition.rhsValue.type;
	}
	else if (m_condition.rhsValue.type == TypeReal) //(m_condition.rhsValue.type == TypeReal)
	{
		m_condition.rhsValue.type = condition.rhsValue.type;
		m_condition.rhsValue.data = malloc(sizeof(float));
		memcpy(m_condition.rhsValue.data, (float *)condition.rhsValue.data, 4); // Note that sizeof(int) and float is assumed to be 4.
	}
	else // (m_condition.rhsValue.type == TypeInt)
	{
		m_condition.rhsValue.type = condition.rhsValue.type;
		m_condition.rhsValue.data = malloc(sizeof(int));
		memcpy(m_condition.rhsValue.data, (int *)condition.rhsValue.data, 4); // Note that sizeof(int) and float is assumed to be 4.
	}

	m_filterAttrNumber = -1;

	// Call the getAttributes method on the Iterator to get the attributes inside input
	input->getAttributes(m_inputAttrs);

	for (unsigned i=0; i<m_inputAttrs.size(); i++)
	{
		if (m_inputAttrs.at(i).name.compare(m_condition.lhsAttr) == 0)
		{
			m_filterAttrNumber = i;
			break;
		}
	}
	if (m_filterAttrNumber == -1)
	{
		cout << "ERROR - Filter condition did not match any attribute in the Attribute List" << endl;
	}
}


Filter::~Filter()
{
    free(m_condition.rhsValue.data);
}

RC Filter::getNextTuple(void *data)
{
    cout << "Inside Filter... getNextTuple :" << endl;
	if (m_filterAttrNumber == -1)
	{
		cout << "ERROR - Filter condition did not match any attribute in the Attribute List" << endl;
		return QE_EOF;
	}

	while(m_input->getNextTuple(data) != RM_EOF)
    {
    	vector<Value> attrValues;
    	// Break up data into constituent items
    	_parseDataBuffer(data, m_inputAttrs, attrValues);

    	// Check if the condition matches the attribute value at the corresponding location in the record
    	if (_isFilterCondTrue(m_condition, attrValues.at(m_filterAttrNumber) ))
    	{
    		cout << "Record found in Filter... " << endl;
    		return 0;
    	}
    }
    cout << "Leaving Filter... getNextTuple :" << endl;
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	// Filter does not alter the attribute list.
	attrs = this->m_inputAttrs;
}


// Project
Project::Project(Iterator *input, const vector<string> &attrNames)
{
	m_input = input;
	m_input->getAttributes(m_originalSetAttrs);
	m_inputAttrName.clear();
	m_inputAttrName = attrNames;
	vector<Attribute> temp;
	getAttributes(temp);
}



Project::~Project()
{
	m_inputAttrName.clear();
	m_originalSetAttrs.clear();
}



RC Project::getNextTuple(void *data)
{

	// open up the data and remove the unwanted attributes.
	void * returnedData = malloc(PF_PAGE_SIZE);
	//initialize
	memset(returnedData, 0, PF_PAGE_SIZE);
	int offset = 0;
	int offsetReturn = 0;
	int count =0;
	int noOfAttributes = m_inputAttrName.size();
	if(m_input->getNextTuple(returnedData)!= QE_EOF)
	{
		for(int k =0; k<noOfAttributes; k++)
		{
			offsetReturn = 0;
			for(unsigned i =0; i< m_originalSetAttrs.size(); i++)
			{
				if(m_originalSetAttrs[i].name == m_inputAttrName[k])
				{
					if(m_originalSetAttrs[i].type == TypeVarChar)
					{
						int sizeVarChar =0; // size of the varchar
						memcpy(&sizeVarChar , (char*)returnedData + offsetReturn , sizeof(int));
						memcpy((char *)data + offset, &sizeVarChar , sizeof(int)); // copied to return Data
						offset += sizeof(int);
						offsetReturn+=sizeof(int);
						memcpy((char *)data + offset, (char*)returnedData + offsetReturn, sizeVarChar);
						offset += sizeVarChar;
						count+=1;
						break;
					}
					else if(m_originalSetAttrs[i].type == TypeInt || m_originalSetAttrs[i].type == TypeReal)
					{
						memcpy((char*)data+ offset, (char *)returnedData+ offsetReturn, sizeof(int));
						offset+= sizeof(int);
						offsetReturn += sizeof(int);
						count+=1;
						break;
					}
				}
				else
				{
					if(m_originalSetAttrs[i].type == TypeVarChar)
					{
						int sizeVarChar =0; // size of the varchar
						memcpy(&sizeVarChar , (char*)returnedData + offsetReturn , sizeof(int));

						offsetReturn +=sizeof(int) + sizeVarChar;
					}
					else if(m_originalSetAttrs[i].type == TypeInt || m_originalSetAttrs[i].type == TypeReal)
					{
						offsetReturn+=sizeof(int);
					}
				}

			}
		}
		if(count == noOfAttributes)
		{
			free(returnedData);
			return 0;
		}
	}
	return QE_EOF;
}


void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	vector<Attribute>::const_iterator itr;

	for(unsigned k =0; k<m_inputAttrName.size(); k++)
	{
		for(itr = m_originalSetAttrs.begin(); itr!=m_originalSetAttrs.end(); itr++)
		{
			if(itr->name == m_inputAttrName[k])
			{
				attrs.push_back(*itr);
			}
		}
	}

}



// We implement a tuple-based NL Join here.
NLJoin::NLJoin(Iterator *leftIn,                             // Iterator of input R
       TableScan *rightIn,                           // TableScan Iterator of input S
       const Condition &condition,                   // Join condition
       const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
)
{
	m_leftIn = leftIn;
	m_rightIn = rightIn;
	m_numPages = numPages;
	// Maintain a copy of the condition used in the constructor

	m_condition = condition; // <-- Check if this works

	m_condition.bRhsIsAttr = condition.bRhsIsAttr;
	m_condition.lhsAttr = condition.lhsAttr;
	m_condition.op = condition.op;
	m_condition.rhsAttr = condition.rhsAttr;


	m_lAttrIndex = -1;
	leftIn->getAttributes(m_lAttrs);
	for (unsigned i=0; i<m_lAttrs.size(); i++)
	{
		if (m_lAttrs.at(i).name.compare(m_condition.lhsAttr) == 0)
		{
			m_lAttrIndex = i;
		}
	}
	if (m_lAttrIndex == -1)
	{
		cout << "ERROR - Condition LHS Attribute did not match any attribute in the LHS Attribute List" << endl;
	}

	m_rAttrIndex = -1;
	rightIn->getAttributes(m_rAttrs);
	for (unsigned i=0; i<m_rAttrs.size(); i++)
	{
		if (m_rAttrs.at(i).name.compare(m_condition.rhsAttr) == 0)
		{
			m_rAttrIndex = i;
		}
	}
	if (m_rAttrIndex == -1)
	{
		cout << "ERROR - Condition RHS Attribute did not match any attribute in the RHS Attribute List" << endl;
	}



}

NLJoin::~NLJoin()
{
	m_lAttrs.clear();
	m_rAttrs.clear();
}

RC NLJoin::getNextTuple(void *data)
{
    cout << "Inside NLJoin... getNextTuple :" << endl;
	if (m_lAttrIndex == -1)
	{
		cout << "ERROR - Condition LHS Attribute did not match any attribute in the LHS Attribute List" << endl;
		return QE_EOF;
	}
	if (m_rAttrIndex == -1)
	{
		cout << "ERROR - Condition RHS Attribute did not match any attribute in the RHS Attribute List" << endl;
		return QE_EOF;
	}


    /*
     * In NL Join, we iterate on the left Relation as the outer.
     * Get a tuple.
     * Iterate on the right Relation as the inner.
     * Check if the Join condition is satisfied.
     * If so, return the combined set of columns in the *data buffer
     * If not, Loop through the inner relation.
     * If the inner relation runs out (ie. EOF) then reset the Inner and start from the beginning.
     *    At the same time, do a getNextTupe on the outer.
     * If the Outer relation runs out then return EOF.
     */
    void *dataLeft = malloc(QE_MAX_DATA);
    void *dataRight = malloc(QE_MAX_DATA);

	vector<Value> attrLValues;
	vector<Value> attrRValues;
	vector<Value>::iterator itAV;
    while(m_leftIn->getNextTuple(dataLeft) != QE_EOF)
    {
        while(m_rightIn->getNextTuple(dataRight) != RM_EOF)
        {
        	attrLValues.clear();
        	attrRValues.clear();

			// Break up dataLeft into constituent items
			_parseDataBuffer(dataLeft, m_lAttrs, attrLValues);
			_parseDataBuffer(dataRight, m_rAttrs, attrRValues);

			// Check if the condition matches the attribute value at the corresponding location in the record
			bool bRecordMatch = _bCompareValues(attrLValues.at(m_lAttrIndex), m_condition.op, attrRValues.at(m_rAttrIndex));
			if (bRecordMatch)
			{
				cout << "Record found in NLJoin... " << endl;
				//Combine attrLValues and attrRValues
				//Convert combined vector to dataBuffer
				//Populate *data
				itAV = attrLValues.end();
				attrLValues.insert(itAV, attrRValues.begin(), attrRValues.end());
				_formatDataBuffer(attrLValues, data);
				free(dataLeft);
				free(dataRight);
				return 0;
			}
			else
			{
				//cout << "Skipping to next record in Inner NL" << endl;
			}
        }
        // Reset the inner relation
        m_rightIn->setIterator();
    }
	free(dataLeft);
	free(dataRight);
    cout << "Leaving NLJoin... getNextTuple :" << endl;
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	vector<Attribute>::iterator it;
	it = attrs.begin();
	attrs.insert(it, m_lAttrs.begin(), m_lAttrs.end());
	it = attrs.end();
	attrs.insert(it, m_rAttrs.begin(), m_rAttrs.end());
}

INLJoin::INLJoin(Iterator *leftIn,                               // Iterator of input R
        IndexScan *rightIn,                             // IndexScan Iterator of input S
        const Condition &condition,                     // Join condition
        const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
)
{
	m_leftIn = leftIn;
	m_rightIn = rightIn;
	m_numPages = numPages;
	// Maintain a copy of the condition used in the constructor

	m_condition = condition; // <-- Check if this works

	m_condition.bRhsIsAttr = condition.bRhsIsAttr;
	m_condition.lhsAttr = condition.lhsAttr;
	m_condition.op = condition.op;
	m_condition.rhsAttr = condition.rhsAttr;


	m_lAttrIndex = -1;
	leftIn->getAttributes(m_lAttrs);
	for (unsigned i=0; i<m_lAttrs.size(); i++)
	{
		if (m_lAttrs.at(i).name.compare(m_condition.lhsAttr) == 0)
		{
			m_lAttrIndex = i;
		}
	}
	if (m_lAttrIndex == -1)
	{
		cout << "ERROR - Condition LHS Attribute did not match any attribute in the LHS Attribute List" << endl;
	}

	m_rAttrIndex = -1;
	rightIn->getAttributes(m_rAttrs);
	for (unsigned i=0; i<m_rAttrs.size(); i++)
	{
		if (m_rAttrs.at(i).name.compare(m_condition.rhsAttr) == 0)
		{
			m_rAttrIndex = i;
		}
	}
	if (m_rAttrIndex == -1)
	{
		cout << "ERROR - Condition RHS Attribute did not match any attribute in the RHS Attribute List" << endl;
	}



}

INLJoin::~INLJoin()
{
	m_lAttrs.clear();
	m_rAttrs.clear();
}

RC INLJoin::getNextTuple(void *data)
{
    cout << "Inside INLJoin... getNextTuple :" << endl;
	if (m_lAttrIndex == -1)
	{
		cout << "ERROR - Condition LHS Attribute did not match any attribute in the LHS Attribute List" << endl;
		return QE_EOF;
	}
	if (m_rAttrIndex == -1)
	{
		cout << "ERROR - Condition RHS Attribute did not match any attribute in the RHS Attribute List" << endl;
		return QE_EOF;
	}


    /*
     * In INL Join, we iterate on the left Relation as the outer.
     * Get a tuple.
     * Iterate on the right Relation as the inner.
     * Check if the Join condition is satisfied.
     * If so, return the combined set of columns in the *data buffer
     * If not, Loop through the inner relation.
     * If the inner relation runs out (ie. EOF) then reset the Inner and start from the beginning.
     *    At the same time, do a getNextTupe on the outer.
     * If the Outer relation runs out then return EOF.
     */
    void *dataLeft = malloc(QE_MAX_DATA);
    void *dataRight = malloc(QE_MAX_DATA);

	vector<Value> attrLValues;
	vector<Value> attrRValues;
	vector<Value>::iterator itAV;
    while(m_leftIn->getNextTuple(dataLeft) != QE_EOF)
    {
    	m_rightIn->setIterator(NO_OP, NULL);
        while(m_rightIn->getNextTuple(dataRight) != RM_EOF)
        {
        	attrLValues.clear();
        	attrRValues.clear();

			// Break up dataLeft into constituent items
			_parseDataBuffer(dataLeft, m_lAttrs, attrLValues);
			_parseDataBuffer(dataRight, m_rAttrs, attrRValues);

			// Check if the condition matches the attribute value at the corresponding location in the record
			bool bRecordMatch = _bCompareValues(attrLValues.at(m_lAttrIndex), m_condition.op, attrRValues.at(m_rAttrIndex));
			if (bRecordMatch)
			{
				cout << "Record found in INLJoin... " << endl;
				//Combine attrLValues and attrRValues
				//Convert combined vector to dataBuffer
				//Populate *data
				itAV = attrLValues.end();
				attrLValues.insert(itAV, attrRValues.begin(), attrRValues.end());
				_formatDataBuffer(attrLValues, data);
				free(dataLeft);
				free(dataRight);
				return 0;
			}
			else
			{
				//cout << "Skipping to next record in Inner INL" << endl;
			}
        }
        // Reset the inner relation - We are doing this before the while anyway.
        // m_rightIn->setIterator(NO_OP, NULL);
    }
	free(dataLeft);
	free(dataRight);
    cout << "Leaving INLJoin... getNextTuple :" << endl;
	return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	vector<Attribute>::iterator it;
	it = attrs.begin();
	attrs.insert(it, m_lAttrs.begin(), m_lAttrs.end());
	it = attrs.end();
	attrs.insert(it, m_rAttrs.begin(), m_rAttrs.end());
}

HashJoin::HashJoin(Iterator *leftIn,                                // Iterator of input R
         Iterator *rightIn,                               // Iterator of input S
         const Condition &condition,                      // Join condition
         const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
)
{
	/*
	 * - Determine the Left and Right attributes in the condition
	 * - Check if they are of the same type.
	 * - Determine the hash function to use
	 * - Create as many page files as provided in the numPages... (or determine how to translate to partitions)
	 * 		- each paged file corresponds to a bucket.
	 * - Read Each tuple from LeftIn (outer relation R)
	 * 		- Apply Hash function on join attribute, determine the hash bucket
	 * 		- Write data into the corresponding bucket file
	 * - Read Each tuple from RightIn (inner relation S)
	 * 		- Apply Hash function on join attribute, determine the hash bucket
	 * 		- Write data into the corresponding bucket file
	 * - Create a TEMP_HJ_RESULT PagedFile
	 * - For Each partition i, do...
	 * 		- Read all records from the Si partition
	 * 		- Make hashtable, hashing on the join attribute using a different hash function
	 * 		For each record in the Ri partition do
	 * 			- Hash the join attribute
	 * 			- Locate the bucket in the hash table
	 * 			- Join the records and insert into the TEMP_HJ_RESULT paged file (if the record matches)
	 * 		end for
	 * 	 end for
	 *
	 *
	 *
	 */

	m_leftIn = leftIn;
	m_rightIn = rightIn;
	m_numPages = numPages;
	// Maintain a copy of the condition used in the constructor

	m_condition.bRhsIsAttr = condition.bRhsIsAttr;
	m_condition.lhsAttr = condition.lhsAttr;
	m_condition.op = condition.op;
	m_condition.rhsAttr = condition.rhsAttr;


	m_lAttrIndex = -1;
	leftIn->getAttributes(m_lAttrs);
	for (unsigned i=0; i<m_lAttrs.size(); i++)
	{
		if (m_lAttrs.at(i).name.compare(m_condition.lhsAttr) == 0)
		{
			m_lAttrIndex = i;
		}
	}
	if (m_lAttrIndex == -1)
	{
		cout << "ERROR - Condition LHS Attribute did not match any attribute in the LHS Attribute List" << endl;
	}

	m_rAttrIndex = -1;
	rightIn->getAttributes(m_rAttrs);
	for (unsigned i=0; i<m_rAttrs.size(); i++)
	{
		if (m_rAttrs.at(i).name.compare(m_condition.rhsAttr) == 0)
		{
			m_rAttrIndex = i;
		}
	}
	if (m_rAttrIndex == -1)
	{
		cout << "ERROR - Condition RHS Attribute did not match any attribute in the RHS Attribute List" << endl;
	}

	// TODO: Based on numPages, compute the numParts - number of partitions.
	m_numPartitions = numPages; // Used to be: 5;
	m_numKeyBuckets = m_numPartitions * 10; // Used to be: 50;
	unsigned currPart = 0;

	for (unsigned i=0; i<m_numPartitions; i++)
	{
        ostringstream ostr;
        ostr << "LEFT_IN_P" << i;
		PF_FileHandle pf;
		_openTempTable(ostr.str(),pf);
		m_lIn_PartName.push_back(ostr.str());
		m_leftIn_PF.push_back(pf);
	}

	for (unsigned i=0; i<m_numPartitions; i++)
	{
        ostringstream ostr;
        ostr << "RIGHT_IN_P" << i;
		PF_FileHandle pf;
		_openTempTable(ostr.str(),pf);
		m_rIn_PartName.push_back(ostr.str());
		m_rightIn_PF.push_back(pf);
	}


	// Allocate enough room for a record.
	void *dataLeft = malloc(QE_MAX_DATA);
    while(leftIn->getNextTuple(dataLeft) != QE_EOF)
    {
    	vector<Value> attrValues;
    	// Break up data into constituent items
    	unsigned recLen;
    	recLen = _parseDataBuffer(dataLeft, m_lAttrs, attrValues);

    	// Determine the partition to send to
    	currPart = _generatePartitionKey(attrValues.at(m_lAttrIndex), m_numPartitions);

        ostringstream ostr;
        ostr << "LEFT_IN_P" << currPart;
    	RID rid;

    	// Write to corresponding partition
    	_insertTupleIntoTempTableBucket(ostr.str(), m_leftIn_PF.at(currPart), dataLeft, recLen, rid);

    	cout << "Record added to Temp Table: " << ostr.str() << " Length=" << recLen << " bytes at RID: " << rid.pageNum << "|" << rid.slotNum << endl;
    }
    free(dataLeft);

	void *dataRight = malloc(QE_MAX_DATA);
    while(rightIn->getNextTuple(dataRight) != QE_EOF)
    {
    	vector<Value> attrValues;
    	// Break up data into constituent items
    	unsigned recLen;
    	recLen = _parseDataBuffer(dataRight, m_rAttrs, attrValues);

    	// Determine the partition to send to
    	currPart = _generatePartitionKey(attrValues.at(m_rAttrIndex), m_numPartitions);

    	ostringstream ostr;
    	ostr << "RIGHT_IN_P" << currPart;
    	RID rid;

    	// Write to corresponding partition
    	_insertTupleIntoTempTableBucket(ostr.str(), m_rightIn_PF.at(currPart), dataRight, recLen, rid);

    	cout << "Record added to Temp Table: " << ostr.str() << " Length=" << recLen << " bytes at RID: " << rid.pageNum << "|" << rid.slotNum << endl;
    }
    free(dataRight);

    // Create a HashJoinResult File
    srand ( time(NULL) );
    unsigned rn = rand();
    ostringstream ostr;
    ostr << "TEMP_HJ_RESULT_" << rn;
    m_hjResultFilename = ostr.str();
	_openTempTable(m_hjResultFilename, m_hjResultPF);

	for (unsigned i=0; i<m_numPartitions; i++)
	{
		multimap<unsigned,RecordInfo> partitionMap;
		multimap<unsigned,RecordInfo>::iterator mapIt;
		_createMapFromPartition(m_rIn_PartName.at(i), m_rightIn_PF.at(i), m_rAttrs, m_rAttrIndex, m_numKeyBuckets, partitionMap);

		cout << "Hash Table has " << partitionMap.size() << " entries." << endl;


	    dataLeft = malloc(QE_MAX_DATA);
//	    dataRight = malloc(QE_MAX_DATA);

		vector<Value> attrLValues;
		vector<Value> attrRValues;
		vector<Value>::iterator itAV;

		// This is so that we get all records in sequence. Needs to be reset for each partition.
		m_lastRid.pageNum = 0;
		m_lastRid.slotNum = 0;
		while(_getNextTupleFromTempTable(m_lIn_PartName.at(i), m_leftIn_PF.at(i), dataLeft, m_lastRid) != QE_EOF)
		{
			// Match the record from the hash table

			attrLValues.clear();
			// Break up dataLeft into constituent items
			_parseDataBuffer(dataLeft, m_lAttrs, attrLValues);
	    	// Determine the key value to hash the record to
	    	unsigned recKey = _generatePartitionKey(attrLValues.at(m_lAttrIndex), m_numKeyBuckets);

			mapIt = partitionMap.find(recKey);
			if (mapIt!=partitionMap.end())
			{
				// Key found. Now check for the values as there can be more than one hit due to key collision
			    do
			    {
					dataRight = mapIt->second.recData;
					attrRValues.clear();
					_parseDataBuffer(dataRight, m_rAttrs, attrRValues);
			    	// Check
					// Check if the condition matches the attribute value at the corresponding location in the record
					bool bRecordMatch = _bCompareValues(attrLValues.at(m_lAttrIndex), m_condition.op, attrRValues.at(m_rAttrIndex));
					if (bRecordMatch)
					{
						//cout << "Record found in HashJoin... " << endl;
						//Combine attrLValues and attrRValues
						//Convert combined vector to dataBuffer
						//Populate *data
						itAV = attrLValues.end();
						attrLValues.insert(itAV, attrRValues.begin(), attrRValues.end());
				    	// Join
					    void *data = malloc(QE_MAX_DATA);
						int newRecSize = _formatDataBuffer(attrLValues, data);
						// DO NOT FREE dataRight free(dataRight);
				    	//Write
						RID rid;
						RC rc = _insertTupleIntoTempTableBucket(m_hjResultFilename, m_hjResultPF, data, newRecSize, rid);
						if (rc!=0)
						{
							cout << "ERROR - Insert Record into Hash table Results file has failed." << endl;

						}

					}
					else
					{
						//cout << "Skipping to next record on Hash table" << endl;
					}
			    	mapIt++;
			    } while (mapIt != partitionMap.upper_bound(recKey));

			}
		}
		free(dataLeft);
		partitionMap.clear();
	}





	for (unsigned i=0; i<m_numPartitions; i++)
	{
		_closeTempTable(m_leftIn_PF.at(i));
		_removeTempTable(m_lIn_PartName.at(i));
		_closeTempTable(m_rightIn_PF.at(i));
		_removeTempTable(m_rIn_PartName.at(i));
	}

	m_lIn_PartName.clear();
	m_leftIn_PF.clear();
	m_rIn_PartName.clear();
	m_rightIn_PF.clear();


	// At this point only the Hash Join Result Table must be on the disk.

	// Set the Last Rid for getNextTuple to return from the beginning
	m_lastRid.pageNum = 0;
	m_lastRid.slotNum = 0;



}

HashJoin::~HashJoin()
{
	/*
	 *	-	Remove the Paged Files from disk
	 *	-	Delete all vectors
	 *	-	Free memory allocs
	 */

	m_lAttrs.clear();
	m_rAttrs.clear();
	_closeTempTable(m_hjResultPF);
	_removeTempTable(m_hjResultFilename);

}

RC HashJoin::getNextTuple(void *data)
{
	/*
	 * -	While GetNextTupleFromTEMP_HS_JOIN Paged File != EOF
	 * 			Return the next row
	 * 		End While
	 * 		Return QE_EOF
	 */

	while(_getNextTupleFromTempTable(m_hjResultFilename, m_hjResultPF, data, m_lastRid) != QE_EOF)
	{
		cout << "Record found in HashJoin... " << endl;
		return 0;
	}
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void HashJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	vector<Attribute>::iterator it;
	it = attrs.begin();
	attrs.insert(it, m_lAttrs.begin(), m_lAttrs.end());
	it = attrs.end();
	attrs.insert(it, m_rAttrs.begin(), m_rAttrs.end());
}

unsigned _generatePartitionKey(Value attrVal, unsigned uParts)
{
	PF_Interface *_pfi = PF_Interface::Instance();

	if (attrVal.type == TypeInt)
	{
		int iVal = 0;
		iVal = 	_pfi->getIntFromBuffer(attrVal.data, 0);
		return (abs(iVal) % uParts);
	}
	else if (attrVal.type == TypeReal)
	{
		float fVal = 0;
		fVal = 	_pfi->getFloatFromBuffer(attrVal.data, 0);
		unsigned temp = (unsigned)ceil(fabs(fVal));
		return (temp % uParts);
	}
	else
	{
		// This is Jenkins one-at-a-time Hash function
		// http://en.wikipedia.org/wiki/Jenkins_hash_function
		unsigned iLen = 0;
		iLen = 	_pfi->getIntFromBuffer(attrVal.data, 0);
		char *key = (char *)attrVal.data+4;
		unsigned hash=0;
		unsigned i;
		for(i=0;(i < iLen); ++i)
		{
			hash += key[i];
			hash += (hash << 10);
			hash ^= (hash >> 6);
		}
		hash += (hash << 3);
		hash ^= (hash >> 11);
		hash += (hash << 15);
		return (hash % uParts);
	}

	return 0;


}

unsigned _formatDataBuffer(vector<Value> &attrValues, void *data)
{
	PF_Interface *_pfi = PF_Interface::Instance();
	unsigned offset = 0;
	for (unsigned i=0; i<attrValues.size(); i++)
	{
		if (attrValues.at(i).type == TypeInt)
		{
			memcpy((char *)data+offset, (char *)attrValues.at(i).data, sizeof(int));
			offset += sizeof(int);
		}
		else if (attrValues.at(i).type == TypeReal)
		{
			memcpy((char *)data+offset, (char *)attrValues.at(i).data, sizeof(float));
			offset += sizeof(float);
		}
		else // (attrValues.at(i).type == TypeVarChar)
		{
			int vLen = _pfi->getIntFromBuffer(attrValues.at(i).data, 0);
			memcpy((char *)data+offset, (char *)attrValues.at(i).data, sizeof(int) + vLen);
			offset += sizeof(int) + vLen;
		}
	}
	return offset;
}



unsigned _parseDataBuffer(void *data, vector<Attribute> &attrs, vector<Value> &attrValues)
{
	unsigned offset = 0;
	for (unsigned i=0; i<attrs.size(); i++)
	{
		Value tempVal;

		tempVal.type = attrs.at(i).type;
		if (attrs.at(i).type == TypeInt)
		{
	        tempVal.data = malloc(sizeof(int));
	        memcpy(tempVal.data, (char *)data + offset, sizeof(int));
	        offset += sizeof(int);
		}
		else if (attrs.at(i).type == TypeReal)
		{
	        tempVal.data = malloc(sizeof(float));
	        memcpy(tempVal.data, (char *)data + offset, sizeof(float));
	        offset += sizeof(float);
		}
		else // type == TypeVarChar
		{
	        int length = *(int *)((char *)data + offset);
	        tempVal.data = malloc(sizeof(int) + length);
	        memcpy(tempVal.data, (char *)data + offset, sizeof(int) + length);
	        offset += sizeof(int) + length;
		}

		attrValues.push_back(tempVal);

	}
	return offset;
}


// This checks if the attribute value matches the condition
bool _isFilterCondTrue(Condition condition, Value attrValue)
{

	if (condition.bRhsIsAttr)
	{
		cout << "ERROR - Filter Condition cannot handle Attribute on RHS. " << endl;
		return false;
	}

	bool b = _bCompareValues(attrValue, condition.op, condition.rhsValue);

	return b;
}


bool _bCompareValues(Value lValue, CompOp op, Value rValue)
{
	PF_Interface *_pfi = PF_Interface::Instance();

	if (lValue.type == rValue.type)
	{
		if (lValue.type == TypeInt)
		{
			int iL = 0;
			int iR = 0;
			iL = _pfi->getIntFromBuffer(lValue.data, 0);
			iR = _pfi->getIntFromBuffer(rValue.data, 0);
			switch (op)
			{
				case EQ_OP:
				{
					if (iL == iR)
						return true;
					else
						return false;
				}
				case LT_OP:
				{
					if (iL < iR)
						return true;
					else
						return false;
				}
				case GT_OP:
				{
					if (iL > iR)
						return true;
					else
						return false;
				}
				case LE_OP:
				{
					if (iL <= iR)
						return true;
					else
						return false;
				}
				case GE_OP:
				{
					if (iL >= iR)
						return true;
					else
						return false;
				}
				case NE_OP:
				{
					if (iL != iR)
						return true;
					else
						return false;
				}
				default:
				{
					return false;
				}
			}
		}
		else if (lValue.type == TypeReal)
		{
			float fL = 0;
			float fR = 0;
			fL = _pfi->getFloatFromBuffer(lValue.data, 0);
			fR = _pfi->getFloatFromBuffer(rValue.data, 0);
			//cout << "Comparing " << fL << " with " << fR << " fabs Value = " << fabs(fL-fR) << " ." << endl;
			switch (op)
			{
				case EQ_OP:
				{
					if (fabs(fL-fR) <= IX_TINY)
						return true;
					else
						return false;
				}
				case LT_OP:
				{
					if (fL < fR)
						return true;
					else
						return false;
				}
				case GT_OP:
				{
					if (fL > fR)
						return true;
					else
						return false;
				}
				case LE_OP:
				{
					if (fL <= fR)
						return true;
					else
						return false;
				}
				case GE_OP:
				{
					if (fL >= fR)
						return true;
					else
						return false;
				}
				case NE_OP:
				{
					if (fabs(fL-fR) > IX_TINY)
						return true;
					else
						return false;
				}
				default:
				{
					return false;
				}
			}
		}
		else // type == TypeVarChar
		{
			int vLen = 0;
			string sL = "";
			string sR = "";
			vLen = _pfi->getIntFromBuffer(lValue.data, 0);
			sL = _pfi->getVarcharFromBuffer(lValue.data, sizeof(int), vLen);
			vLen = _pfi->getIntFromBuffer(rValue.data, 0);
			sR = _pfi->getVarcharFromBuffer(rValue.data, sizeof(int), vLen);
			switch (op)
			{
				case EQ_OP:
				{
					if (sL.compare(sR) == 0)
						return true;
					else
						return false;
				}
				case LT_OP:
				{
					if (sL.compare(sR) < 0)
						return true;
					else
						return false;
				}
				case GT_OP:
				{
					if (sL.compare(sR) > 0)
						return true;
					else
						return false;
				}
				case LE_OP:
				{
					if (sL.compare(sR) <= 0)
						return true;
					else
						return false;
				}
				case GE_OP:
				{
					if (sL.compare(sR) >= 0)
						return true;
					else
						return false;
				}
				case NE_OP:
				{
					if (sL.compare(sR) != 0)
						return true;
					else
						return false;
				}
				default:
				{
					return false;
				}
			}
		}
	}
	else
	{
		cout << "ERROR - Compare Values will only be possible with values of the same Type. " << endl;
		return false;
	}

}

RC _insertTupleIntoTempTableBucket(const string tableName, PF_FileHandle pfh, const void *data, const int newRecSize, RID &rid)
{
	PF_Interface *m_pfi = PF_Interface::Instance();

	string upperTableName = _strToUpper(tableName);
	// Variable Initialization
	int freePtr=0;
	int newSlotPtr=0;
	int noOfSlots=0;
	int newSlotSize=0;
	int pgNum=0;
	int result=0;
	RC rc;
	void *buffer = malloc(PF_PAGE_SIZE);

	// Locate the next free page
	pgNum = m_pfi->locateNextFreePage( pfh, newRecSize);
	if (pgNum < 0)
	{
		cout << "ERROR - Unable to locate next free page from Table." << endl;
		result =-1;
	}

	// Read the Page and update the buffer
    rc = m_pfi->getPage(pfh, pgNum, buffer);
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
    memcpy((char *)buffer + freePtr, data, newRecSize);

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

    rc = m_pfi->putPage(pfh, pgNum, buffer);
	if (rc != 0)
	{
		cout << "ERROR - Unable to write Page to Table. RC=" << rc << endl;
		result = -1;
	}

	free(buffer);
	return result;
}


RC _readTupleFromTempTableBucket(const string tableName, PF_FileHandle pfh, const RID &rid, void *data)
{
	string upperTableName = _strToUpper(tableName);
	RC rc;
	PF_Interface *_pfi = PF_Interface::Instance();
	unsigned uTemp = 0;


	if (pfh.GetNumberOfPages()<rid.pageNum)
	{
		if(rid.pageNum!=IX_NOVALUE)
			cout << "ERROR - RID contains a Page that does not exist. Aborting. Page#:" << rid.pageNum << "." << endl;
		return -4;
	}


	// You need to allocate for the page being read and clean it up at the end.
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = _pfi->getPage(pfh, rid.pageNum, buffer);
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

	memcpy((char *)data, (char *)buffer+tuplePtr, tupleSize);

	free(buffer);
	return 0;
}


RC _openTempTable(const string tableName, PF_FileHandle &fh)
{
    PF_Manager *pfM = PF_Manager::Instance();

	RC rc = 0;
    PF_Interface *pfi = PF_Interface::Instance();
    string upperTableName = _strToUpper(tableName);

    struct stat stFileInfo;
    if (stat(upperTableName.c_str(), &stFileInfo) == 0)
    {
        cout << upperTableName << "File located on disk. Reusing." << endl;
    }
    else
    {
        // No catalog file found. Create one.
        rc = pfM->CreateFile(upperTableName.c_str());
        if(rc!=0)
        {
            cout << "File " << upperTableName << " could not be created. Cannot proceed without a catalog." << endl;
            return rc;
        }

    }

    // At this point a file called upperTableName should exist on the disk.
    rc = pfM->OpenFile(upperTableName.c_str(), fh);
    if(rc!=0)
    {
        cout << "File " << upperTableName << " could not be opened." << endl;
        return rc;
    }

    // Get the number of pages
    unsigned count = fh.GetNumberOfPages();
    if (count>0)
    {
    	// Good so that the Pagedfile has already been setup on the disk and you should go ahead with it.
    }
    else
    {
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
        rc = fh.AppendPage(data);
        free(data);
    }

    // Hopefully all should have worked right and a success should be returned.
	return rc;

}


//  This call may be required only as a cleanup. We can release a TableRef object if one exists.
RC _closeTempTable(PF_FileHandle pf)
{
    PF_Manager *pfM = PF_Manager::Instance();
	RC rc = 0;
    rc = pfM->CloseFile(pf);
    if (rc!=0)
    {
        cout << "File could not be closed properly." << endl;
        return rc;
    }

	return rc;
}


RC _removeTempTable(const string tableName)
{
    PF_Manager *pfM = PF_Manager::Instance();
	RC rc;

	string upperTableName = _strToUpper(tableName);

    rc = pfM->DestroyFile(upperTableName.c_str());
    struct stat stFileInfo;
    if (stat(upperTableName.c_str(), &stFileInfo) == 0)
    {
        cout << "Failed to destroy file! RetCode=" << rc << "." << endl;
        return -1;
    }
    else
    {
        cout << "File " << upperTableName << " has been destroyed." << endl;
        return rc;
    }
    return rc;
}

void _createMapFromPartition(string tableName, PF_FileHandle pfh,
		vector<Attribute> attrs, int attIndex, unsigned numBuckets,
		multimap<unsigned,RecordInfo> &partitionMap)
{

	int numPages = pfh.GetNumberOfPages();
	PF_Interface *_pfi = PF_Interface::Instance();
	RC rc=0;


	// You need to allocate for the page being read and clean it up at the end.
	void *buffer = malloc(PF_PAGE_SIZE);
	// Loop through each page
	for (int i=0; i<numPages; i++)
	{
		rc = _pfi->getPage(pfh, i, buffer);
		if (rc !=0)
		{
			cout << "ERROR - Read Page#" << i << " from PagedFile " << tableName << " errored out. RC=" << rc << "." << endl;
		}

		int noOfTuples = _pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);


		// Loop through each tuple in the page
		for (int j=0; j<noOfTuples; j++)
		{
			RID rid;
			rid.pageNum = i;
			rid.slotNum = j+1;

			unsigned uSlotPtr = PF_PAGE_SIZE - 8 - 8*rid.slotNum;
			unsigned recKey;
			RecordInfo recInfo;
			recInfo.recLen = _pfi->getIntFromBuffer(buffer, uSlotPtr+4);
			recInfo.recData = malloc(recInfo.recLen);
			rc = _readTupleFromTempTableBucket(tableName, pfh, rid, recInfo.recData);
			if (rc!=0)
			{
				cout << "ERROR - Page#" << rid.pageNum << " Slot#" << rid.slotNum << " from PagedFile " << tableName << " does not exist." << endl;
				free(recInfo.recData);
			}
			// Generate a key for the record based on join attribute
	    	vector<Value> attrValues;
	    	// Break up data into constituent items
	    	_parseDataBuffer(recInfo.recData, attrs, attrValues);

	    	// Determine the key value to hash the record to
	    	recKey = _generatePartitionKey(attrValues.at(attIndex), numBuckets);

			// Add recInfo to map
	    	partitionMap.insert(pair<unsigned,RecordInfo>(recKey,recInfo));
			//cout << "Record added at Hash Key=" << recKey << endl;
		}
	}
	free(buffer);
}


RC _getNextTupleFromTempTable(string tableName, PF_FileHandle pfh, void *data, RID &lastRid)
{
	// lastRid has the last Rid that was returned. This will position on the next Tuple to display.
	unsigned numPages = pfh.GetNumberOfPages();
	if (lastRid.pageNum >= numPages)
	{
		cout << "ERROR - Last record position: Page#" << lastRid.pageNum << " from PagedFile " << tableName << " seems incorrect. Aborting." << endl;
	}
	PF_Interface *_pfi = PF_Interface::Instance();
	RC rc=0;
	// You need to allocate for the page being read and clean it up at the end.
	void *buffer = malloc(PF_PAGE_SIZE);
	// Loop through each page in the partition, starting from the lastRid's PageNum
	for (unsigned i=lastRid.pageNum; i<numPages; i++)
	{
		rc = _pfi->getPage(pfh, i, buffer);
		if (rc !=0)
		{
			cout << "ERROR - Read Page#" << i << " from PagedFile " << tableName << " errored out. RC=" << rc << "." << endl;
		}

		unsigned noOfTuples = _pfi->getIntFromBuffer(buffer, PF_PAGE_SIZE-8);
		if (lastRid.slotNum > noOfTuples)
		{
			cout << "ERROR - Last record position: Page#" << lastRid.pageNum << " Slot# " << lastRid.slotNum << " from PagedFile " << tableName << " seems incorrect. Aborting." << endl;
		}



		// Loop through each tuple in the page...
		for (unsigned j=lastRid.slotNum; j<noOfTuples; j++)
		{
			//RID rid;
			lastRid.pageNum = i;
			lastRid.slotNum = j+1;

			rc = _readTupleFromTempTableBucket(tableName, pfh, lastRid, data);
			if (rc!=0)
			{
				cout << "ERROR - Page#" << lastRid.pageNum << " Slot#" << lastRid.slotNum << " from PagedFile " << tableName << " does not exist." << endl;
			}
			// Need to return just one record at a time.
			return 0;

		}
		lastRid.slotNum = 0; // Reset this as you are about to switch to the next page.
	}
	lastRid.pageNum = 0; // Reset this as you are done scrolling through the entire table.
	free(buffer);
	return QE_EOF;
}
