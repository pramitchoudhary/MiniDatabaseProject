/*
 * pf_interface.cc
 *
 *  Created on: Oct 16, 2011
 *	CS222 - Principles of Database Systems
 *		Project 1 - Record Manager Interface (RM)
 *		Coded By: Dexter (#24)
 *
 *	The PF Interface is a singleton class with utility functions that encapsulate the PF layer details
 *	from the RM or upper layers. It supports a number of low-level functions like string conversions,
 *	PagedFile lifecycle operations etc.
 */

//#include "rm.h"
#include "pf_interface.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>


using namespace std;

PF_Interface* PF_Interface::_pf_intf = PF_Interface::Instance();
PF_Manager* PF_Interface::_pf_mgr = PF_Manager::Instance();

// We use this approach to convert between unsigned characters and int/real/Varchar
union FloatHex{
   unsigned char   uData[4];   // Four bytes to hold a float
   float         fValue;      // The float
} ufh;
union UnsignedHex{
   unsigned char   uData[4];   // Four bytes to hold a HEX unsigned
   unsigned         uValue;      // The unsigned value
} uuh;
union IntHex{
   unsigned char   uData[4];   // Four bytes to hold a HEX int
   int         iValue;      // The int value
} uih;

/*
 * These are a series of functions that copy in or out of a char * buffer into different formats:
 * The formats coded are:
 * - Integer (4 bytes)
 * - Float (4 bytes)
 * - Varchar (variable number of bytes)
 */

// This handles FloatToHex conversion
void PF_Interface::addFloatToBuffer(const void *data, unsigned position, float fVal)
{
    FloatHex u_fh;
    u_fh.fValue = fVal;
    for(unsigned i = 0; i < 4; i++)
    {
    	*((char *)data+position+i) = u_fh.uData[i];
    }
}
// This handles HexToFloat conversion
float PF_Interface::getFloatFromBuffer(const void *data, unsigned position)
{
    FloatHex u_fh;
    u_fh.fValue = 0;
    for(unsigned i = 0; i < 4; i++)
    {
    	u_fh.uData[i] = *((char *)data+position+i);
    }
    return u_fh.fValue;
}
// This handles IntToHex conversion
void PF_Interface::addIntToBuffer(const void *data, unsigned position, int iVal)
{
    IntHex u_ih;
    u_ih.iValue = iVal;
    for(unsigned i = 0; i < 4; i++)
    {
    	*((char *)data+position+i) = u_ih.uData[i];
    }
}
// This handles HexToInt conversion
int PF_Interface::getIntFromBuffer(const void *data, unsigned position)
{
    IntHex u_ih;
    u_ih.iValue = 0;
    for(unsigned i = 0; i < 4; i++)
    {
    	u_ih.uData[i] = *((char *)data+position+i);
    }
    return u_ih.iValue;
}
// This handles UnsignedToHex conversion
void PF_Interface::addUnsignedToBuffer(const void *data, unsigned position, unsigned uVal)
{
    UnsignedHex u_uh;
    u_uh.uValue = uVal;
    for(unsigned i = 0; i < 4; i++)
    {
    	*((char *)data+position+i) = u_uh.uData[i];
    }
}
// This handles HexToUnsigned conversion
unsigned PF_Interface::getUnsignedFromBuffer(const void *data, unsigned position)
{
    UnsignedHex u_uh;
    u_uh.uValue = 0;
    for(unsigned i = 0; i < 4; i++)
    {
    	u_uh.uData[i] = *((char *)data+position+i);
    }
    return u_uh.uValue;
}
// This handles the task to adding a string into a buffer
void PF_Interface::addVarcharToBuffer(const void *data, unsigned position, void *value, unsigned valSize)
{
	// copying byte by byte into the buffer
    for(unsigned i = 0; i < valSize; i++)
    {
    	*((char *)data+position+i) = *((char *)value+i);
    }

}
// This handles the task to retrieving a string from a buffer
string PF_Interface::getVarcharFromBuffer(const void *data, unsigned position, unsigned valSize)
{
	string s = "";
    s.append((char *)data+position,valSize);
    return s;

}


PF_Interface* PF_Interface::Instance()
{
    if(!_pf_intf)
    	_pf_intf = new PF_Interface();

    return _pf_intf;
}


PF_Interface::PF_Interface()
{
	_pf_mgr = PF_Manager::Instance();
}

PF_Interface::~PF_Interface()
{
}

/*
 * This is a function that encapsulates some low-level calls to the PF layer.
 * It provides the RM layer an ability to operate on an abstraction called Table which in the PF layer is a Paged File.
 * However, the Creation and Opening of the PagedFile on the filesystem will need ot be done before any operation
 * This method takes care of sequencing such file lifecycle operations.
 *
 * This method creates a PagedFile on the disk if one does not exist.
 * If a file exists, it will use it.
 * OpenTable is valid only after a table has been created. Therefore, this call checks if the table exists in the Catalog.
 * As a side-effect of this call, this method sets up a TableRef object for the table in the cache of TableRef objects held in memory.
 *
 */
RC PF_Interface::openTable(const string tableName, PF_FileHandle &fh)
{
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
        rc = _pf_mgr->CreateFile(upperTableName.c_str());
        if(rc!=0)
        {
            cout << "File " << upperTableName << " could not be created. Cannot proceed without a catalog." << endl;
            return rc;
        }

    }

    // At this point a file called upperTableName should exist on the disk.
    rc = _pf_mgr->OpenFile(upperTableName.c_str(), fh);
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

/*
 * This is a specialization of the openCatalog and is restricted to opening the CATALOG file
 * that is persisted on the disk to capture table structures created.
 * This opens a PagedFile called CATALOG on the disk. This table will hold the Catalog information.
 * If the catalog file does not exist, it will be created and opened for use.
 */
RC PF_Interface::openCatalog()
{
	RC rc = 0;
    string fileName = "CATALOG";
    PF_Interface *pfi = PF_Interface::Instance();
    // Open the file "Catalog"

    struct stat stFileInfo;
    if (stat(fileName.c_str(), &stFileInfo) == 0)
    {
        cout << fileName << " CATALOG file located. Reusing." << endl;
    }
    else
    {
        // No catalog file found. Create one.
        rc = _pf_mgr->CreateFile(fileName.c_str());
        if(rc!=0)
        {
            cout << "File " << fileName << " could not be created. Cannot proceed without a catalog." << endl;
            return rc;
        }

    }

    // At this point a file called CATALOG should exist on the disk.
    rc = _pf_mgr->OpenFile(fileName.c_str(), _cat_handle);
    if(rc!=0)
    {
        cout << "File " << fileName << " could not be opened." << endl;
        return rc;
    }

    // Get the number of pages
    unsigned count = _cat_handle.GetNumberOfPages();
    if (count>0)
    {
    	// Good so that catalog file has already been setup on the disk and you should go ahead with it.
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
        rc = _cat_handle.AppendPage(data);
        free(data);
    }

    // Hopefully all should have worked right and a success should be returned.
	return rc;
}


//  This call may be required only as a cleanup. We can release a TableRef object if one exists.
RC PF_Interface::closeTable(PF_FileHandle pf)
{
	RC rc = 0;
    rc = _pf_mgr->CloseFile(pf);
    if (rc!=0)
    {
        cout << "File could not be closed properly." << endl;
        return rc;
    }

	return rc;
}

//  This call may be required only as a cleanup. It will cleanly close the CATALOG disk file.
RC PF_Interface::closeCatalog()
{
	RC rc = 0;
    rc = _pf_mgr->CloseFile(_cat_handle);
    if (rc!=0)
    {
        cout << "CATALOG file could not be closed." << endl;
        return rc;
    }

	return rc;
}
/*
 * This call encapsulates the calls to the PF Layer to destroy the PagedFile at the end
 * of a deleteTable function called by RM.
 *
 */
RC PF_Interface::removeTable(const string tableName)
{
	RC rc;

	string upperTableName = _strToUpper(tableName);

    rc = _pf_mgr->DestroyFile(upperTableName.c_str());
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

// This gets a specified page from the opened PagedFile
// It encapsulates the PF Layer's functionality
RC PF_Interface::getPage(PF_FileHandle pf, unsigned pg, void *buffer)
{
	RC rc;
    rc = pf.ReadPage(pg, buffer);
    return rc;

}

// This puts a specified page into the opened PagedFile
// It encapsulates the PF Layer's functionality
RC PF_Interface::putPage(PF_FileHandle pf, unsigned pg, const void *buffer)
{
	RC rc;
    rc = pf.WritePage(pg, buffer);
    return rc;
}

/* This methods looks for the first available page in a PagedFile that can accomodate the record
 * It loops through all the pages one by one until it can find room on the page.
 * The room on the page should account for the space required for the Slot in the directory
 * as well as the record itself.
 *
 * If no existing pages can accomodate the record with the size specified, then a new page is
 * added to the PagedFile and returned.
 */
int PF_Interface::locateNextFreePage(PF_FileHandle pf, unsigned newRecSize)
{
	RC rc;
	unsigned freePtr = 0;
	unsigned noOfSlots = 0;
    PF_Interface *pfi = PF_Interface::Instance();
    int freeSpaceOnPage = 0;

    unsigned iMaxPages= pf.GetNumberOfPages();
    void *buffer = malloc(PF_PAGE_SIZE);

    for (unsigned i=0;i<iMaxPages;i++)
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

/*
 * This checks if a PagedFile already exists on the disk.
 * If so then the CREATE TABLE on that table should not proceed
 */
bool PF_Interface::isPagedFileCreated(const string tableName)
{
    string upperTableName = _strToUpper(tableName);
    struct stat stFileInfo;
    if (stat(upperTableName.c_str(), &stFileInfo) == 0)
    {
        cout << upperTableName << " exists on disk. TABLE HAS ALREADY BEEN CREATED." << endl;
    	return true;
    }
    else
    {
    	return false;
    }
}

