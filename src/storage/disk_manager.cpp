#include <stdexcept>
#include <sys/stat.h>
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

#include<iostream>
using namespace std;

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  //ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  //ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}
void DiskManager::ReadBitMapPage(page_id_t extent_id, char *page_data) {
  //int extent_id=logical_page_id/BIT_MAP_SIZE;
  //Page_data will record the data read from the disk
  page_id_t BitMap_Index=extent_id*(BITMAP_SIZE+1)+1;
  //Read the Bit map from the disk
  ReadPhysicalPage(BitMap_Index,page_data);
}


// allocatePage function changed point: line 67, 68, 81, 82, 88, 102, 106
// here only contains the changed version of allocatepage function and deallocatepage function
// 2022-5-4
// updator: haoming Yu
page_id_t DiskManager::AllocatePage() {
  //0.Update Disk_File_Meta_page
  //1.We need to linear Search the Extent,and find the Extent which is not full
  //2.After we get the extent,The We need to read bitmap in that Extent from the disk
  //3.Using the bitmap to NextFreePage_id
  //4.Get the Logical_Page_id(Extent_id*BIT_MAP_SIZE+NextFreePage_id)
  //5.Write back to the Physical Disk--------WritePhysicalPage(MapPageId(Logical_Page_id),Page_Data)
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);
  uint32_t NextPage=0;
  //Situation 0: if this is the first Allocation
  if(meta_page->GetExtentNums()==0)
  {
    // std::cout<<"num_extents_--------" <<meta_page->num_extents_<<endl;
    // std::cout<<"num_allocated_pages_--------" <<meta_page->num_allocated_pages_<<endl;
    // std::cout<<"extent_used_page[0]--------" <<meta_page->extent_used_page_[0]<<endl;
    //First Part-Update Disk_File_Meta_Data
    //Allocate num_extents
    meta_page->num_extents_++;
    //Update num_allocated_page
    meta_page->num_allocated_pages_++;
    //Update extent_used_page
    meta_page->extent_used_page_[0]=1;

    //Second Part-Update the BitMap
    //Read BitMap from the corresponding  extent
    char Page_Data[PAGE_SIZE];//Page_data will record the data read from the disk
    // ReadBitMapPage(0,Page_Data);
    ReadPhysicalPage(1,Page_Data);
    
    // for(int i=0;i<10;i++)
    // {
    //   ReadPhysicalPage((i)*(BITMAP_SIZE+1)+1,Page_Data);
    //   BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);
    //   std::cout<<"PageAllocated "<<(i)*(BITMAP_SIZE+1)+1<<"------------- "<<Bitmap_page->page_allocated_<<std::endl;
    //   std::cout<<"MaxSupportSize------------- "<<Bitmap_page->GetMaxSupportedSize()<<std::endl; 
    // }
    
    //Allocate Page
    BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);
    bool state=Bitmap_page->AllocatePage(NextPage);
    if(state)
    {
      //In the Disk Storage Layout, page 1 is the First BitMapPage
      //Write Back Page 1 to the Disk
      //std::cout<<"Allocate Success"<<std::endl;
      char *Page_Data = reinterpret_cast<char *>(Bitmap_page);
      WritePhysicalPage(1,Page_Data);
    }
    else
    {
      // std::cout<<"PageAllocated------------- "<<Bitmap_page->page_allocated_<<std::endl;
      // std::cout<<"MaxSupportSize------------- "<<Bitmap_page->GetMaxSupportedSize()<<std::endl;
      std::cerr<<"Error----AllocatePage Failed1"<<std::endl;
    }
  }
  //Situation 1: General Case.
  else {
    //First Part: -> Update disk_file_meta_page
    //1.Update num_allocated_pages first
    meta_page->num_allocated_pages_++;
    //2.Find the Last extent which is not full
    int flag=0;
    uint32_t i;
    for (i=0; i < meta_page->num_extents_; i++)
    {
      if (meta_page->extent_used_page_[i] < BITMAP_SIZE)
      {
        flag=1;
        break;
      }
    }
    //3.Check the overflow of the current extents
    if(flag==0)
    {
      //it means the current extents are all full.
      //Used New extent
      i = meta_page->num_extents_++;
      meta_page->extent_used_page_[i]++;
    }
    else
    {
      //it means the current extents are not full.
      // original code: meta_page->extent_used_page_[meta_page->num_extents_ - 1]++;
      meta_page->extent_used_page_[i]++;
    }

    // i is the corresponding extent_id of this allocate

    //Second Part- Read BitMap
    char Page_Data[PAGE_SIZE];
    ReadBitMapPage(i,Page_Data);
    //Thrid Part- Allocate Page
    BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);
    bool state = Bitmap_page->AllocatePage(NextPage);
    
    if(state==true)
    {
      page_id_t BitMap_page_id=i*(BITMAP_SIZE+1)+1;  
      //Write Back BitMap Page  to the Disk
      char *Page_Data = reinterpret_cast<char *>(Bitmap_page);
      WritePhysicalPage(BitMap_page_id,Page_Data);
      NextPage += i*BITMAP_SIZE; // the extent_id * (number of data pages) + page number in the extent => logical page id.
    }
    else
    {
      std::cerr<<"Error----AllocatePage Failed2"<<std::endl;
    }
  }
  
  return NextPage;
}

// deallocatePage function changed point is line 143 -> reason lies in 144th line.
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  
  //0.Update Disk_File_Meta_page
  //1.Convert the logical_page_id to Physical_page_id
  //2.if we DeAllocatePage, We may need to update num_extents_
  //3.Then We need to read bitmap in that Extent from the disk
  //4.Using the bitmap to Deallocate the logical_page_id%BITMAX_Size
  //5.Write back to the Physical Disk--------WritePhysicalPage(MapPageId(Logical_Page_id),Page_Data)
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);

  page_id_t Physical_Page_Id = this->MapPageId(logical_page_id);
  //Situation 0: if the extents is Empty, We could not DeAllocate.
  if(meta_page->GetExtentNums()==0)
  {
    // std::cerr<<"Error- Can not DeAllocate the Extent"<<std::endl;
    return ; // stop the execution of the rest of the function.
  }

  //Situation 1: if the extents only has one page, so if we delete the extents, we need to update diskFileData
  
  //page_id_t physical_page_id=MapPageId(logical_page_id);
  int extent_id=logical_page_id/BITMAP_SIZE;
  //Situation 1.1: if the extents only has one page, and that is not free.
  if(meta_page->extent_used_page_[extent_id]==1&&IsPageFree(logical_page_id)==false)
  {
    //Update the Disk_File_Meta_Data
    meta_page->num_allocated_pages_--;
    //original code: meta_page->num_extents_--;
    // the reason for changing -> comment this code away. 
    // Because this extent(with 0 data pages) might not be the tail, 
    // simply decrease the extents num might cause problem while allocating a new one.
    meta_page->extent_used_page_[extent_id]=0;
    //Read the Corresponding BitMap From the Disk,DeAllocate the BitMap
    char Page_Data[PAGE_SIZE];
    ReadBitMapPage(extent_id,Page_Data);
    BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);

    char Init_Page_Data[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++) 
    {
      Init_Page_Data[i] = '\0';
    }

    //DeAllocate the bitmap
    bool state=Bitmap_page->DeAllocatePage(logical_page_id%BITMAP_SIZE);
    if(state==true)
    {
      //std::cout<<"Situation 1.1-----disk_manager::DeAllocate()------Success"<<std::endl;
      //Write Back to the Disk
      page_id_t BitMap_page_id=extent_id*(BITMAP_SIZE+1)+1;  
      //Write Back BitMap Page  to the Disk
      char *Page_Data = reinterpret_cast<char *>(Bitmap_page);
      WritePhysicalPage(BitMap_page_id,Page_Data);
      WritePhysicalPage(Physical_Page_Id, Init_Page_Data);

    }
    else
    {
      std::cerr<<"Situation 1.1-----disk_manager::DeAllocate()------Failed"<<std::endl;
      WritePhysicalPage(Physical_Page_Id, Init_Page_Data);
      return;
    }
  }
  //Situation 1.2: if the extents only has one page, and that page is free.
  else if(meta_page->extent_used_page_[extent_id]==1&&IsPageFree(logical_page_id)==true)
  {
    std::cerr<<"Situation 1.2-----Can not Deallocate,Page "<<logical_page_id<<" is Free"<<std::endl;
    return;
  }
  //Situation 2: General Case.
  else{
    // uint32_t num_allocated_pages_{0};
    // uint32_t num_extents_{0};   // each extent consists with a bit map and BIT_MAP_SIZE pages
    // uint32_t extent_used_page_[0];

    //First Part: -> Update disk_file_meta_page
    //1.Update num_allocated_pages first
    meta_page->extent_used_page_[extent_id]--;
    meta_page->num_allocated_pages_--;
    //Second Part: -> Read BitMap
    char Page_Data[PAGE_SIZE];
    ReadBitMapPage(extent_id,Page_Data);
    //Thrid Part: -> Allocate Page
    BitmapPage<PAGE_SIZE> *Bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);
    bool state = Bitmap_page->DeAllocatePage(logical_page_id%BITMAP_SIZE);

    char Init_Page_Data[PAGE_SIZE];
    for (int i = 0; i < PAGE_SIZE; i++) {
      Init_Page_Data[i] = '\0';
    }
    
    if(state==true)
    {
      page_id_t BitMap_page_id=(extent_id)*(BITMAP_SIZE+1)+1;  
      //std::cout<<"Situation 2.1-----disk_manager::DeAllocate()------Success"<<std::endl;
      //Write Back BitMap Page  to the Disk
      char *Page_Data = reinterpret_cast<char *>(Bitmap_page);
      WritePhysicalPage(BitMap_page_id,Page_Data);
      WritePhysicalPage(Physical_Page_Id, Init_Page_Data);
    }
    else
    {
      std::cerr<<"Situation 2.1-----disk_manager::DeAllocate-----Failed"<<std::endl;
      WritePhysicalPage(Physical_Page_Id, Init_Page_Data);
      return;
    }
  }
}
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  //1.First Find the Corresponding Extent_id.
  //2.Second Read the bitMap.
  //3.Using the bitMap to determine Whether the Logical_page_id is Free or not.

  char Page_Data[PAGE_SIZE];//Page_data will record the data read from the disk
  int extent_id=logical_page_id/BITMAP_SIZE;
  ReadBitMapPage(extent_id,Page_Data);
  //Convert the Logical_page_id to the Every Extent Pageid
  page_id_t Bitmap_page_id=logical_page_id%BITMAP_SIZE;
  BitmapPage<PAGE_SIZE> * bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(Page_Data);
  //Get the state of the page
  bool state=bitmap_page->IsPageFree(Bitmap_page_id);
  return state;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  //This Functions will map the logical_page_id to physical_page_id
  page_id_t Delta=2;
  //First We need to Know the extent numbers of the logical_page_id
  int extent_id=logical_page_id/BITMAP_SIZE;
  page_id_t physical_page_id=extent_id+Delta+logical_page_id;
  return physical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}