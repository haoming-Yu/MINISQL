#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetNextPageId(INVALID_PAGE_ID);
  this->SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return this->next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { this->next_page_id_=next_page_id; }

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const { 
	
	int index = -1;
	for (int i = 0; i < this->GetSize(); i++) {
		if (comparator(this->array_[i].first, key) >= 0) 
		{
            index = i;
            break;
		}
	}
        return index;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key=this->array_[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}



INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::ValueIndex(const  ValueType &value) const {
  int index = -1;
  for (int i = 0; i < this->GetSize(); i++) {
    if (array_[i].second == value) {
      index = i;
      break;
    }
  }
  return index;
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int index = -1;
  if (this->GetSize() + 1 <= this->GetMaxSize()) {
    for (int i = 0; i < this->GetSize(); i++) {
      // Find the Position Which the Key will insert into.
      if (comparator(key, this->array_[i].first) < 0) {
        index = i;
        break;
      }
    }
    // NewKey &Value should insert into end of the NewNode
    if (index == -1) {
      index = this->GetSize();
      if (this->GetSize() < this->GetMaxSize()) this->array_[index] = {key, value};

      this->IncreaseSize(1);
  /*    for (int i = 0; i < this->GetSize(); i++) {
        std::cout << this->array_[i].first  << endl;
      }*/
      return this->GetSize();
    }
    // NewKey& Value should insert into middle of the NewNode
    else {
      // Copy From the End to First
      for (int i = this->GetSize(); i > index; i--) {
        this->array_[i] = this->array_[i - 1];
      }
      // Insert the NewKey&Value into Right Position
      if (this->GetSize() < this->GetMaxSize()) this->array_[index] = {key, value};
      this->IncreaseSize(1);
      return this->GetSize();
    }
  } else {
    return this->GetSize()+1;
  }

}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,BufferPoolManager* bufferpoolManager) { 

    int DeleteSize = (this->GetMinSize());
    recipient->CopyNFrom(&this->array_[(this->GetSize()-this->GetMinSize())], DeleteSize);  // Recipient Increase Size Here
    if (this->GetNextPageId() != INVALID_PAGE_ID) {
      recipient->SetNextPageId(this->GetNextPageId());
    }
    this->SetNextPageId(recipient->GetPageId());
   
    //Decrease Size
    this->IncreaseSize(-recipient->GetSize());
   /* for (int i = 0; i < recipient->GetSize(); i++) {
      std::cout << recipient->array_[i].first << endl;
    }*/

}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) { 
    
   
  // Copy Entries into This Page
  for (int i = 0; i < size; i++) {
       
      this->array_[i].first = items[i].first;
      this->array_[i].second = items[i].second;
    
  }
  // Increment the Size
  this->IncreaseSize(size);

}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator)const  {
  
   bool state = false;
  //Find the key in the correspoding array_
  for (int i = 0; i < this->GetSize(); i++) {
    if (comparator(key, this->array_[i].first) == 0) {
    //then store its corresponding value in input "value" and return true
      value=this->array_[i].second  ;
      state = true;
      break;
    }
  }
  return state;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) { 
    bool state = false;
  //cout << "before-----------------------------------" << endl;
  //for (int i = 0; i < this->GetSize(); i++) {
  //    cout << i << " : " << this->array_[i].first << endl;
  //}
    for (int i = 0; i < this->GetSize(); i++) {
      if (comparator(key, this->array_[i].first) == 0) {
        state = true;
        int index = i;
        for (int i = index + 1; i < this->GetSize(); i++) {
          this->array_[i - 1].first = this->array_[i].first;
          this->array_[i - 1].second = this->array_[i].second;
        }
        this->IncreaseSize(-1);
        break;
      }
    }
  /*  cout << "After-----------------------------------" << endl;
    for (int i = 0; i < this->GetSize(); i++) {
      cout << i << " : " << this->array_[i].first << endl;
    }*/
    if (state == false) {
      return this->GetSize();
    }
    return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient ) { 
    //Copy the Instance into the recipient Page
    for (int i = 0; i < this->GetSize(); i++) {
      recipient->CopyLastFrom(this->array_[i]);  // Add Size Here
    }
    
    this->IncreaseSize(-this->GetSize());                     // Decrease Size of this Page
    //Set the Next Page id.
    recipient->SetNextPageId(this->GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) { 
    recipient->CopyLastFrom(this->array_[0]);//Add Size Here
    //Remove the First Key
    for (int i = 0; i < this->GetSize()-1; i++) {
      this->array_[i] = this->array_[i + 1];  
    }
    this->IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) { 

    this->array_[this->GetSize()] = item; 
    this->IncreaseSize(1);
   /* for (int i = 0; i < this->GetSize(); i++) {
      cout << "fuck"<<i << this->array_[i].first << endl;
    }*/

}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {

  recipient->CopyFirstFrom(this->array_[this->GetSize() - 1]);
  this->IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  
  for (int i = this->GetSize(); i >0; i--) {
    this->array_[i] = this->array_[i-1];
  }
  this->array_[0] = item;
  this->IncreaseSize(1);

}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;