#include <iostream>
#include <stdio.h>
#include <string>
#include <sys/stat.h>
#include <cassert>
#include "pf.h"

using namespace std;

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{

	FILE *filePointer;
	//Check if the file already exists in the System.
	//Variable description.
	struct stat stFileInfo;
	int result =-1;

	if(stat(fileName, &stFileInfo) != 0)
	{
		filePointer = fopen(fileName,"wb+");
		if(filePointer!= NULL)
		{
			cout << " CreateFile executed, File created successfully"<<endl;
			fclose(filePointer);
			result = 0;
		}
	}
	else
	{
		cout<<" Given file name already exists"<< endl;
	}


	return result;
}


RC PF_Manager::DestroyFile(const char *fileName)
{

	// variables
	struct stat stFileInfo;
	int result =-1;

	if(stat(fileName, &stFileInfo) == 0) // Make sure that the concerned file exists.
	{
		// Delete the file specified
		result = remove(fileName);
	}
	else
	{

		perror ("File could not be destroyed because of the following error");
	}

	return result;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	// Variables
	struct stat stFileInfo;
	int result= -1;
	// Check if the file exists
	if(stat(fileName, &stFileInfo) == 0)
	{
		if(fileHandle.m_fileHandleForPage == NULL)
		{
			fileHandle.m_fileHandleForPage = fopen(fileName,"rb+");
			if(fileHandle.m_fileHandleForPage)
			{
				cout << " File opened successfully"<< endl;
				result = 0;
			}
		}
		else
		{
			perror("The following error occurred");
		}
	}
	else
	{
		cout<< " Requested file doesn't exists"<< endl<< endl;
	}
	return result;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	// Variables.
	int result = 0;
	// Close the file.
	if(fileHandle.m_fileHandleForPage) //only if the pointers exists, release it
	{
		result = fclose(fileHandle.m_fileHandleForPage);

	}
	return result;
}


PF_FileHandle::PF_FileHandle():m_fileHandleForPage(NULL)
{

}
 

PF_FileHandle::~PF_FileHandle()
{

}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
    // Variables
	int startoffset, result, returnValue=-1;
	// Check if the page requested to be read is actually present
	if((pageNum+1) <= GetNumberOfPages())
	{
		startoffset = pageNum * PF_PAGE_SIZE;
		fflush(m_fileHandleForPage);
		fseek (m_fileHandleForPage, startoffset, SEEK_SET);
		result = fread(data, 1, PF_PAGE_SIZE, m_fileHandleForPage);
		if(result == PF_PAGE_SIZE)
		{
			//cout << "Page read successfully" << endl;
			returnValue = 0;
		}
		else
		{
			cout << " Requested page could not be read"<< endl;
		}
    }
	else
	{
		cout<< " The requested page doesn't exists"<< endl;
	}

	return returnValue;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	// Variables
	int result, startPoint, returnValue =-1;

	// Check if the page requested to be Updated is actually present
	if((pageNum+1) <= GetNumberOfPages())
	{
		startPoint = (pageNum)*PF_PAGE_SIZE;

		if(m_fileHandleForPage!=NULL)
		{
			rewind(m_fileHandleForPage);
			fseek (m_fileHandleForPage, startPoint, SEEK_SET);
			result = fwrite(data,1,PF_PAGE_SIZE,m_fileHandleForPage);
			if(result == PF_PAGE_SIZE)
			{
				returnValue = 0;
				//cout << " Page"<<pageNum<<" update is successful"<< endl;
			}
			else
			{
				perror("Update failed because of the following error");
			}

		}
	}
	else
	{
		cout << " Requested page could not be updated, as it does not exists"<< endl;
	}
	return returnValue;
}


RC PF_FileHandle::AppendPage(const void *data)
{
	// Variables
	unsigned int numOfPgs;
	int result, returnValue = -1;

	// Get the number of pages
	numOfPgs = GetNumberOfPages();
	int offset = numOfPgs*PF_PAGE_SIZE;
	if(numOfPgs == 0 && m_fileHandleForPage)
	{
		result = fwrite(data, 1, PF_PAGE_SIZE, m_fileHandleForPage);
	}
	else
	{
		if(m_fileHandleForPage)
		{
			fflush(m_fileHandleForPage);
			fseek (m_fileHandleForPage, offset, SEEK_SET);
			result = fwrite(data, 1, PF_PAGE_SIZE, m_fileHandleForPage);
		}
	}

	if(PF_PAGE_SIZE == result)
	{
		returnValue = 0;
		//cout << " Page successfully created"<< endl;
	}

	return returnValue;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	// return the count of the number of pages currently in the File.
	unsigned int filesize, numberofPages;
	if(m_fileHandleForPage)
	{
		fseek(m_fileHandleForPage, 0, SEEK_END);
		filesize = ftell(m_fileHandleForPage);
		numberofPages = filesize/PF_PAGE_SIZE;
	}
	return numberofPages;
}
