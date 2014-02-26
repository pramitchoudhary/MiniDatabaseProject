/*
 * catalog.h
 *
 *  Created on: Oct 16, 2011
 *	CS222 - Principles of Database Systems
 *		Project 1 - Record Manager Interface (RM)
 *		Coded By: Dexter (#24)
 */

#ifndef CATALOG_H_
#define CATALOG_H_

#include <string>
#include <stdio.h>
#include <list>
#include "../pf/pf.h"


using namespace std;

/******************   THESE DEFINITIONS ARE MOVED FROM RM.H ***********************/
// Return code
typedef int RC;


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};


// Comparison Operator
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

/********************  END OFMOVE FROM RM.H   ******************************************/




// This is the memory representation of a Column entry in the database.
typedef struct ColRef {
	string colName; // attribute name
	unsigned colPosition; // attribute position
	AttrType colType; // attribute type
	AttrLength colLength; // attribute length
	unsigned colDelInd; // attribute deleted
	unsigned recordOffset; // byte position on a record directory... to speed up the access of your fields in the record.
} ColRef;

// This is the memory representation of a Table Definition in the database. It references the ColRef object
typedef struct TableRef {
	string tableName; // table name
	PF_FileHandle pfh; // PF FileHandle of the PagedFile corresponding to the table
	list<ColRef> colRefs; // Array of column References... allows you to directly access your columns by column position.
} TableRef;

class Catalog;

class Catalog
{
	public:

		static Catalog* Instance(); // Access to the _catalog instance

		/*
		 *    This function scans the Catalog PagedFile and returns all rows corresponding to the tableName passed
		 The rows are further split into its constituent attributes and ColRef objects are populated.
		 A vector of these ColRef objects is returned (hopefully sorted by colPosition.)
		 If the table is not created yet, then there will be no rows and a non-zero RC value is returned.
		 *
		 */
		RC scanCatalog(const string tableName, list<ColRef> &colRefs);

		// This function is used internally to add an entry into the Catalog's PagedFile.
		RC addCatalogTuple(void *data);

		// This function is used to mark a Calatog entry as 'deleted' so that the data attribute is simulated as dropped.
		RC deleteCatalogTuple(const void *data);


		/*
		 *    This function searches for the TableRef object in the Catalog cache
		 *    and returns an object that contains the complete representation of the table in the catalog.
		 *    If the TableRef has not been cached, it will call the scanTableRef object and find an empty slot to cache the TableRef object.
		 *    It will then pass teh TableRef object back to the caller.
		 *    This will encapsulate the management of the Catalog Table References.
		 *
		 */
		TableRef getTableRef(const string tableName);

		// Release the TableRef object in the cache
		// Typically required if the cache is full or...
		// If you want to refresh the object in the memory due to some change in the table structure.
		RC releaseTableRef(const string tableName);


		// Computes the record size based on the TableReference and includes variable length data.
		unsigned getRecSizeFromRecordBuffer(TableRef tRef, const void *data);

		// Format converters
		ColRef convertCatalogRecordDataToColRef(const string tableName, const void *recordData, unsigned recordSize);
		void convertTupleFormatToRecordFormat(TableRef tref, const void *tupleData, void *recordData);
		void convertRecordFormatToTupleFormat(TableRef tRef, const void *recordData, void *tupleData);


		TableRef _tableRefs[10]; // This is the cached TableReference objects
		TableRef _catalogRef; // This is the single reference for the CATALOG table used for all Catalog record management

	protected:
		Catalog(); // Constructor
		~Catalog(); // Destructor

	private:
		static Catalog *_catalog;
};

// This function allows us to sort the collection of ColRefs by Position
bool _sortColRefByPosition(ColRef cA, ColRef cB);

// To convert strings to upper case
string _strToUpper(string str);

#endif /* CATALOG_H_ */

