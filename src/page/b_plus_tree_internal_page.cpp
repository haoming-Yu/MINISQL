#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);

}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code  
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { 
    
  /*  if (index == 0) {
    std::cerr << "index is 0 Can not SetKeyAt 0" << endl;
    }*/
    array_[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const { 
    int index = -1;
    for (int  i = 0; i < this->GetSize(); i++) {
      if (array_[i].second == value) {
        index = i;
        break;
      }
    }
    return index;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  ValueType val = array_[index].second;
  return val;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  ValueType val;
  //std::cout << "Page " << this->GetPageId() << "--";
  //for (int i = 0; i < this->GetSize(); i++) {
  //  std::cout << this->array_[i].second << " ";
  //}
  //cout << endl;
  if (comparator(key, array_[1].first) < 0) {
   
    return array_[0].second;
  }
  int flag = 1;
  for (int i = 1; i < this->GetSize(); i++) {
    //If you Find the InputKey is Less than in one of the InternalNode
    if (comparator(key, this->KeyAt(i)) < 0) {
      val = this->ValueAt(i-1);
      flag = 0;
      break;
    }
  }
  if (flag == 1) {
   // it  means larger than all the node
    val = this->ValueAt(this->GetSize() - 1);
  }
  return val;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * 用给一直分裂到root
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) 
{
    
    //old_value means Old root-page id
  this->array_[0].second = old_value;
    SetKeyAt(1, new_key);
  this->array_[1].second = new_value;
  //Increase Size by two
  this->IncreaseSize(2);

}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  //Get Index Of the old_value
  int index = ValueIndex(old_value);
  //If the index do not overflow 
  if (index + 1 < this->GetMaxSize()) {
    for (int i = this->GetSize(); i > index+1; i--) {
      if (i - 1 > 0) {
        this->array_[i] = this->array_[i - 1];
      } else {
        this->array_[i].second = this->array_[i - 1].second;      
      }
    }
    this->SetKeyAt(index + 1, new_key);
    array_[index + 1].second = new_value;
    this->IncreaseSize(1);
  } else {
    throw "B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter-OverFlow" ;
    return this->GetSize() + 1;
  }
  
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  
  // Copy into the recipient Page
  recipient->CopyNFrom(&this->array_[this->GetSize()-this->GetMinSize()],this->GetMinSize(),buffer_pool_manager);
  // Remove From the Current Page
  this->IncreaseSize(-recipient->GetSize());
  
  //buffer_pool_manager->UnpinPage(this->GetPageId(),true);
 

} 

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  //Copy Entries into This Page
  for (int i = 0; i < size; i++) {
    if (i == 0) {
    //Skip the first Element
      this->array_[i].second = items[0].second;
    } else {
      this->array_[i].first = items[i].first;
      this->array_[i].second = items[i].second;
    }
  }
  //Increment the Size
  this->IncreaseSize(size);
  //Adopt the parent Id 
  for (int i = 0; i < size; i++) {
    auto *page = buffer_pool_manager->FetchPage(items[i].second);
    if (page != nullptr) {
      auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
      // Change the Parent id to me
      node->SetParentPageId(this->GetPageId());
      // UnpinPgae
      buffer_pool_manager->UnpinPage(items[i].second, true);
    }
  }
  
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) { 

    //Delete the first Child
    if (index == 0&&this->GetSize()>=2) {
        //Remove from the first Value
      for (int i = 0; i < this->GetSize(); i++) {
          if (i == 0) {
          this->array_[i].second = this->array_[i + 1].second;
          } else {
            this->array_[i] = this->array_[i + 1];
          }
      }
      this->IncreaseSize(-1);
    }

    if (this->GetSize()==0) {
    std::cerr << "Can not Remove" << endl;
    } else {
      //Move Forward the array_
       if (this->GetSize() == 2) {
          // it means it only has one child and one pair key& value
          // it delete the last element, so we need to delete the last pair,
          // it does not need to remove 

          this->IncreaseSize(-1);
          return;
      
      } else {
        for (int i = index + 1; i < this->GetSize(); i++) {
          this->array_[i - 1].first = this->array_[i].first;
          this->array_[i - 1].second = this->array_[i].second;
        }
        for (int i = 0; i < this->GetSize(); i++) {
          /*if(i!=0)cout << "fuck2---key " << i << " " << this->array_[i].first << endl;
          else cout << "fuck2---value " << i << " " << this->array_[i].second << endl;*/
        }
      }
      
    }
    this->IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  //Delete the Last key & value pair
  this->Remove(1);
  //Return the value for the First Child.
  ValueType val = this->array_[0].second;
  return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  MappingType NewPair = {middle_key, this->array_[0].second};
  for (int i = 0; i < this->GetSize(); i++) {
    if (i != 0)
      recipient->CopyLastFrom(this->array_[i], buffer_pool_manager);
    else
      recipient->CopyLastFrom(NewPair, buffer_pool_manager);
  }
  //Remove the Key from this Page
  this->IncreaseSize(-this->GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType NewPair = {middle_key, this->array_[0].second};
  recipient->CopyLastFrom(NewPair,buffer_pool_manager);//Add Size Here
  //Remove the First Key
  // If we need to Update the Middle_key in the Parent node
  // We just Get from the Index ==0
  for (int i = 0; i < this->GetSize()-1; i++) {
    this->array_[i] = this->array_[i + 1];
  }
  this->IncreaseSize(-1);
  
  
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = this->GetSize();
  
  //Append the Entry at the end
  this->array_[size] = pair;
  //IncreaseSize
  this->IncreaseSize(1);

  //Adopt the parent page_id
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  if (page != nullptr) {
    auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  
    node->SetParentPageId(this->GetPageId());
    /* do something */
    buffer_pool_manager->UnpinPage(pair.second, true);
  }

}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {

  MappingType NewPair = {middle_key, this->array_[this->GetSize()-1].second};
  recipient->CopyFirstFrom(NewPair, buffer_pool_manager);//AddSize Here
  //If we need to update the Parent's middle_key just get from the index==GetSize
  this->IncreaseSize(-1);

}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for (int i = this->GetSize(); i >0; i--) {
    if (i-1 == 0)
      this->array_[i].second = this->array_[i-1].second;
    else
      this->array_[i] = this->array_[i-1];
  }
  this->array_[0].second = pair.second;
  this->array_[1].first = pair.first;
  
  // IncreaseSize
  this->IncreaseSize(1);

  // Adopt the parent page_id
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  if (page != nullptr) {
    auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

    node->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(pair.second, true);
  }
  
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ResetParent(const page_id_t &old_node, const page_id_t &new_node,BufferPoolManager* buffer_pool_manager_) {
  // Read the old_page and new_page
  BPlusTreePage *old_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(old_node));
  BPlusTreePage *new_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_node));
  old_page->SetParentPageId(this->GetPageId());
  new_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager_->UnpinPage(old_node, true);
  buffer_pool_manager_->UnpinPage(new_node, true);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;