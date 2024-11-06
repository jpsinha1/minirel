#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

// ------------------------------------------------------------------------------------------------
// Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to 
// disk. Returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O 
// layer returned an error when a dirty page was being written to disk and OK otherwise.  This 
// private method will get called by the readPage() and allocPage() methods described below.

// Make sure that if the buffer frame allocated has a valid page in it, that you remove the 
// appropriate entry from the hash table.
// ------------------------------------------------------------------------------------------------
const Status BufMgr::allocBuf(int & frame) 
{
    int iterations = 0;

    while (iterations < (2 * numBufs)) 
    {
        advanceClock();
        BufDesc* currentFrame = &bufTable[clockHand];  // get the current frame each iteration
        iterations++;

        // Check if the current frame is available for allocation
        if (currentFrame->pinCnt == 0 && !currentFrame->refbit) {
            // If the frame is valid and dirty, write it back to disk
            if (currentFrame->valid) {
                if (currentFrame->dirty) {
                    Status status = currentFrame->file->writePage(currentFrame->pageNo, &bufPool[clockHand]);
                    if (status != OK) return status;
                }

                // Remove the page from the hash table
                Status status = hashTable->remove(currentFrame->file, currentFrame->pageNo);
                if (status != OK) return status;
            }

            // Clear the frame and set it as the selected frame
            currentFrame->Clear();
            frame = clockHand;
            return OK;
        }

        // Reset the refbit for the next pass
        if (currentFrame->refbit) {
            currentFrame->refbit = false;
        }
    }

    return BUFFEREXCEEDED;
}


	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = -1;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if(status != OK) {
        allocBuf(frameNo); // might break
        status = file->readPage(PageNo, &bufPool[frameNo]);

        if(status != OK) 
            return status;

        hashTable->insert(file, PageNo, frameNo);
        bufTable[frameNo].Set(file, PageNo); 

    } else {
        bufTable[frameNo].refbit = 1;
        bufTable[frameNo].pinCnt++;
    }
    page = &bufPool[frameNo];
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo = -1;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if(status != OK) 
        return status;
    else if(bufTable[frameNo].pinCnt == 0) 
        return PAGENOTPINNED;
    bufTable[frameNo].pinCnt--;
    if (dirty == true)
        bufTable[frameNo].dirty = true;
    return OK;
    
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {
    // init. variables for frame index and status tracking
    int frameNo = -1; 
    Status status = NOTUSED1;

    // allocate a new page within the specified file
    status = file->allocatePage(pageNo);
    if (status == UNIXERR) {
        return UNIXERR;
    }

    // allocate a free frame in the buffer pool
    status = allocBuf(frameNo);
    if (status != OK) {
        return status;
    }

    // insert page into hash table for easy lookup
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK) {
        return HASHTBLERROR;
    }

    // set up buffer descriptor for the newly allocated frame
    bufTable[frameNo].Set(file, pageNo);

    // return a pointer to the allocated buffer pool frame
    page = &bufPool[frameNo];
    return OK;                   
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
