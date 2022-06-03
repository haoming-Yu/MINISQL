#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "buffer/buffer_pool_manager.h"
#include "common/rowid.h"
#include "page/table_page.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(RowId rowId_, char *Position, BufferPoolManager *buffer_pool_manager_, Schema *schema,
                         TablePage *table_page)
      : heap_(new SimpleMemHeap) {
    this->rowId_ = rowId_;
    this->Position = Position;
    this->buffer_pool_manager_ = buffer_pool_manager_;
    this->Page_pointer = table_page;
    this->schema = schema;
  }

  explicit TableIterator(const TableIterator &other) : heap_(new SimpleMemHeap) {
    this->rowId_ = other.rowId_;
    this->Position = other.Position;
    this->buffer_pool_manager_ = other.buffer_pool_manager_;
    this->Page_pointer = other.Page_pointer;
    this->schema = other.schema;
  }

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();  // ++i

  TableIterator operator++(int);  // i++

 private:
  // Current rowId_
  RowId rowId_{0};

  // Current Position of the Page
  char *Position;

  // Current table page, need to access the table page of the caller.
  TablePage *Page_pointer;

  BufferPoolManager *buffer_pool_manager_;
  Schema *schema;
  MemHeap *heap_ = {nullptr};
};

#endif  // MINISQL_TABLE_ITERATOR_H
