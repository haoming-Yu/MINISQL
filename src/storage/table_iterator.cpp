#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const { return this->rowId_ == itr.rowId_; }

bool TableIterator::operator!=(const TableIterator &itr) const { return !(this->rowId_ == itr.rowId_); }

const Row &TableIterator::operator*() {
  void *mem = heap_->Allocate(sizeof(Row));
  Row *row = new (mem) Row(this->rowId_);
  row->DeserializeFrom(this->Position, this->schema);
  return *row;
}

Row *TableIterator::operator->() {
  void *mem = heap_->Allocate(sizeof(Row));
  Row *row = new (mem) Row(this->rowId_);
  row->DeserializeFrom(this->Position, this->schema);
  return row;
}

TableIterator &TableIterator::operator++() {
  RowId next_rowId;
  if (this->Page_pointer->GetNextTupleRid(this->rowId_, &next_rowId)) {
    this->rowId_.Set(this->rowId_.GetPageId(), next_rowId.GetSlotNum());
    this->Position = this->Page_pointer->GetData() + this->Page_pointer->position_calculate(this->rowId_.GetSlotNum());
  } else {
    if (this->Page_pointer->GetNextPageId() == INVALID_PAGE_ID) {
      this->rowId_.Set(next_rowId.GetPageId(), next_rowId.GetSlotNum());
      return *this;
    } else {
      auto Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this->Page_pointer->GetNextPageId()));
      RowId first_rowId;
      Page->GetFirstTupleRid(&first_rowId);
      this->rowId_ = first_rowId;
      this->Position = Page->GetData() + Page->position_calculate(this->rowId_.GetSlotNum());
      this->Page_pointer = Page;
      buffer_pool_manager_->UnpinPage(Page->GetPageId(), false);
    }
  }

  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator tmp(*this);
  RowId next_rowId;
  if (this->Page_pointer->GetNextTupleRid(this->rowId_, &next_rowId)) {
    this->rowId_.Set(this->rowId_.GetPageId(), next_rowId.GetSlotNum());
    this->Position = this->Page_pointer->GetData() + this->Page_pointer->position_calculate(this->rowId_.GetSlotNum());
  } else {
    if (this->Page_pointer->GetNextPageId() == INVALID_PAGE_ID) {
      this->rowId_.Set(next_rowId.GetPageId(), next_rowId.GetSlotNum());
      return TableIterator(*this);
    } else {
      auto Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(this->Page_pointer->GetNextPageId()));
      RowId first_rowId;
      Page->GetFirstTupleRid(&first_rowId);
      this->rowId_ = first_rowId;
      this->Position = Page->GetData() + Page->position_calculate(this->rowId_.GetSlotNum());
      this->Page_pointer = Page;
      buffer_pool_manager_->UnpinPage(Page->GetPageId(), false);
    }
  }

  return TableIterator(tmp);
}
