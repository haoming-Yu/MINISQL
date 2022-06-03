#include "page/bitmap_page.h"
#include<iostream>
using namespace std;
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  bool state=false;
  
  //Bitmap is not FUll
  if(page_allocated_<GetMaxSupportedSize())
  {
    //Update page_allocatted
    this->page_allocated_++;
    //page_offset is next_free_page
    //if the next_free_page is not deleted before
    
    while(IsPageFree(this->next_free_page_)==false&&this->next_free_page_<GetMaxSupportedSize())
    {
      this->next_free_page_++;
      //flag=1;
    }
    page_offset=this->next_free_page_;
    //Get the Byte_index and bit_index of the Allocate Page
    uint32_t byte_index=this->next_free_page_/8;
    uint32_t bit_index=this->next_free_page_%8;
    //Update BitMap 
    unsigned char tmp=0x01;
    bytes[byte_index]=(bytes[byte_index]|(tmp<<(7-bit_index)));
    //Update next_free_page
    while(IsPageFree(this->next_free_page_)==false&&this->next_free_page_<GetMaxSupportedSize()){this->next_free_page_++;}
    state=true;
    //std::cout<<"next_free_page-----------"<<next_free_page_<<endl;

  }
  else{
  //BitMap is Full
      std::cout<<"Can not Allocate Page Anymore"<<std::endl;
      state=false;
  }
  return state;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  //Get the Byte_index and bit_index
  uint32_t byte_index=page_offset/8;
  uint32_t bit_index=page_offset%8;
  bool state=false;
  if(this->page_allocated_==0||IsPageFree(page_offset)==true)
  {
    
   std::cout<<"Allocate First"<<std::endl;
    state= false;
  }
  else 
  {
      unsigned char tmp=0x01;
      tmp=~(tmp<<(7-bit_index));
      bytes[byte_index]=bytes[byte_index]&tmp;
      this->page_allocated_--;
      if(page_offset<this->next_free_page_)this->next_free_page_=page_offset;
      state=true;
  }
  return state;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index=page_offset/8;
  uint32_t bit_index=page_offset%8;
  unsigned char tmp=0x01;
  unsigned char PageState=(bytes[byte_index]&(tmp<<(7-bit_index)));
  if(PageState==0) return true;
  else return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;