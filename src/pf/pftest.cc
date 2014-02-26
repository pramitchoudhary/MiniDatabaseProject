#include <iostream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"

using namespace std;

const int success = 0;



// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else
    {
    	cout << " File marked for deletion does't exists"<< endl;
    	return false;
    }
}

int PFTest_1(PF_Manager *pf)
{
	// Functions Tested:
    // 1. CreateFile
    cout << "****In PF Test Case 1****" << endl;

    RC rc;
    string fileName = "test";
    int result = -1;

    // Create a file named "test"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
        result = 0;
    }
    else
    {
        cout << "Failed to create file!" << endl;
    }

    // 2. Create "test" again, should fail
    rc = pf->CreateFile(fileName.c_str());
    assert(rc != success);

    if(rc!= success)
    {
    	cout<< " Test1 passed"<< endl;
    }
    return result;
}

int PFTest_2(PF_Manager *pf)
{
	  // Functions Tested:
	  // 1. OpenFile
      // 2. AppendPage
      // 3. GetNumberOfPages
      // 4. WritePage
      // 5. ReadPage
	  // 6. Check the integrity
      // 7. CloseFile
      // 8. DestroyFile
      cout << "****In PF Test Case 2****" << endl;

      RC rc;
      int result = -1;
      string fileName = "test";

      // 1. Open the file "test"
      PF_FileHandle fileHandle;
      rc = pf->OpenFile(fileName.c_str(), fileHandle);
      assert(rc == success);

      // 2. Append a new Page
      void *data = malloc(PF_PAGE_SIZE);
      for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
      {
    	  *((char *)data+i) = i % 94 + 32;
      }

      rc = fileHandle.AppendPage(data);
      assert(rc == success);

      // 3. Get the number of pages
      unsigned count = fileHandle.GetNumberOfPages();
      assert(count == (unsigned)1);

      // 4. Update the first page
      // Write ASCII characters from 32 to 41 (inclusive)
      data = malloc(PF_PAGE_SIZE);
      for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
      {
          *((char *)data+i) = i % 10 + 32;
      }
      rc = fileHandle.WritePage(0, data);
      assert(rc == success);

     // 5. Read the page
      void *buffer = malloc(PF_PAGE_SIZE);
      rc = fileHandle.ReadPage(0, buffer);
      assert(rc == success);

     // 6. Check the integrity
     rc = memcmp(data, buffer, PF_PAGE_SIZE);
     assert(rc == success);

     // 7. Close the file "test"
      rc = pf->CloseFile(fileHandle);
      assert(rc == success);

      free(data);
      free(buffer);

      // 8. DestroyFile
      rc = pf->DestroyFile(fileName.c_str());
      assert(rc == success);

     if(!FileExists(fileName.c_str()))
     {
         cout << "File " << fileName << " has been destroyed." << endl;
         cout << "Test Case 2 Passed!" << endl << endl;
         result= 0;
     }
     else
     {
         cout << "Failed to destroy file!, Test 2 failed" << endl;
     }
      return result;
}

int PFTest_3(PF_Manager *pf)
{
	// Extension of PFTest_2
    cout << "****In PF Test Case 3****" << endl;
    // Variables
    RC rc;
    int result =-1;
    string fileName = "test1";

    // Create a file named "test"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file "test"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

	// 1. Append 2 pages
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
  	  *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.AppendPage(data);
    assert(rc == success);

    data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = 'c';
    }
    rc = fileHandle.AppendPage(data);
    assert(rc == success);

    //2. Update the first page
    data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = 'P';
    }
    rc = fileHandle.WritePage(0, data);
    assert(rc == success);

    // 3. Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)2);

    //4. Read a page currently not present. Currently, only 2 pages are there.
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(7, buffer);
    assert(rc != success);

    //5. Update a page currently not present. Currently, only 2 pages are there.
    rc = fileHandle.WritePage(7, data);
    assert(rc != success);

    // Routine Calls-------------------------
    //Close the file "test"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    //DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    if(!FileExists(fileName.c_str()))
    {
    	cout << "File " << fileName << " has been destroyed." << endl;
    	cout << "Test Case 3 Passed!" << endl << endl;
    	result = 0;
    }
    else
    {
    	cout << "Failed to destroy file!" << endl;
    }

    return result;
}

/*int main()
{
    PF_Manager *pf = PF_Manager::Instance();
    remove("test");
    remove("test1");
   
    PFTest_1(pf); 
    PFTest_2(pf);
    PFTest_3(pf);

    return 0;
}*/
