/*
 * pf_interface.h
 *
 *
 *  Created on: Oct 16, 2011
 *	CS222 - Principles of Database Systems
 *		Project 1 - Record Manager Interface (RM)
 *		Coded By: Dexter (#24)
 */

#ifndef PF_INTERFACE_H_
#define PF_INTERFACE_H_

#include <stdio.h>
#include <string>
#include <cstring>
#include "../pf/pf.h"

using namespace std;


class PF_Interface;

/*
 * 	This will be a singleton class and designed to hold utility functions for the RM methods to use.
 * 	The RM methods are oblivious of the implementation of the PF layer.
 * 	Some PagedFile lifecycle operations at the PF layer may be required in more than one method in the RM layer.
 * 	As a result, it makes sense to combine these functions in a separate class and reuse them.
 *
 */
class PF_Interface
{
	public:

		PF_FileHandle _cat_handle;
		static PF_Interface* Instance();                                      // Access to the _pf_intf instance


		static PF_Manager *_pf_mgr;

		/*
		 *	This call will call the setupTableRef() to create a TableRef object if one does not exist
		 *	This call will also need to open the PagedFile called tableName... calling the PF_Manager.
		 *	Table name and PF filename will be identical.
		 *	The value of the PF_FileHandle is populated in the TableRef object
		 *	All RM layer calls operating on a table should typically be preceded
		 *	by this call to ensure the PagedFiles are open for use
		 *
		 */
		RC openTable(const string tableName, PF_FileHandle &fh);

		// This opens a PagedFile called CATALOG on the disk. This table will hold the Catalog information.
		// If the catalog file does not exist, it will be created and opened for use.
		RC openCatalog();


		//  This call may be required only as a cleanup.
		RC closeTable(PF_FileHandle pf);

		//  This call may be required only as a cleanup. It will cleanly close the CATALOG disk file.
		RC closeCatalog();

		// This call encapsulates the calls to the PF Layer to destroy the PagedFile at the end of a
		// deleteTable fucntion called by RM.
		RC removeTable(const string tableName);

		// This gets a specified page from the opened PagedFile
		RC getPage(PF_FileHandle pf, unsigned pg, void *buffer);

		// This puts a specified page into the opened PagedFile
		RC putPage(PF_FileHandle pf, unsigned pg, const void *buffer);

		/* This methods looks for the first available page in a PagedFile that can accomodate the record
		 * It loops through all the pages one by one until it can find room on the page.
		 * The room on the page should account for the space required for the Slot in the directory
		 * as well as the record itself.
		 *
		 * If no existing pages can accomodate the record with the size specified, then a new page is
		 * added to the PagedFile and returned.
		 */
		int locateNextFreePage(PF_FileHandle pf, unsigned newRecSize);

		/*
		 * These are a series of functions that copy in or out of a char * buffer into different formats:
		 * The formats coded are:
		 * - Integer (4 bytes)
		 * - Float (4 bytes)
		 * - Varchar (variable number of bytes)
		 */
		void addFloatToBuffer(const void *data, unsigned position, float fVal);
		float getFloatFromBuffer(const void *data, unsigned position);
		void addIntToBuffer(const void *data, unsigned position, int iVal);
		int getIntFromBuffer(const void *data, unsigned position);
		void addUnsignedToBuffer(const void *data, unsigned position, unsigned uVal);
		unsigned getUnsignedFromBuffer(const void *data, unsigned position);
		void addVarcharToBuffer(const void *data, unsigned position, void *value, unsigned valSize);
		string getVarcharFromBuffer(const void *data, unsigned position, unsigned valSize);
		bool isPagedFileCreated(const string tableName);

	protected:
		PF_Interface();                                                       // Constructor
		~PF_Interface   ();                                                   // Destructor

	private:
		static PF_Interface *_pf_intf;

};


//This converts a string to Uppercase
string _strToUpper(string str);

#endif /* PF_INTERFACE_H_ */
