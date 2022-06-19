# BUFFER POOL MANAGER

## 1. LRU替换策略

Buffer Pool Replacer负责跟踪Buffer Pool中数据页的使用情况，并在Buffer Pool没有空闲页时决定替换哪一个数据页。在本节中，我们需要实现一个基于LRU替换算法的`LRUReplacer`，`LRUReplacer`类在`src/include/buffer/lru_replacer.h`中被定义，其扩展了抽象类`Replacer`（在`src/include/buffer/replacer.h`中被定义，Replacer class 作为虚拟的父类存在提供了可拓展空间，对于后续增加新的替换策略减少IO具有重要的意义；如果需要增加新的替换方法，只需要在replacer类下进行继承即可，比如后续实现join拓展的时候可以增加MRU或者混合替换策略从而降低非必要的IO开销）。`LRUReplacer`的大小默认与Buffer Pool的大小相同。

因此，在这个模块中，需要重点实现以下函数，与之相关的代码位于`src/buffer/lru_replacer.cpp`中。

- ___LRU___模块__接口设计思路 :__ LRU模块通过提供给上层的BUFFER POOL MANAGER对页的pin和unpin管理不同模块协同访问页的方法，上层模块仅需要提供给LRU具体的内存中的frame_id，即可实现对LRU中的least recently used的页的查找，也可以对LRU中的页进行pin和unpin操作。

- ___LRU___模块__具体实现思路 :__ 主要通过一个双向链表和一个哈希表和双向链表迭代器的符合映射来实现LRU。其中双向链表中靠近表头的代表刚刚使用过，靠近表尾的代表最少使用的块。使用哈希表到迭代器的映射可以在O(1)的时间实现对表中的任意一个块元素的快速访问，同时使用双向链表记录使用次序的方法也可以保证O(1)的时间实现表中任意块的序更新，不需要进行所有块的更新，只需要将被访问的块提到表头即可。具体的类实现代码如下：(lru_replacer.h)

  ```cpp
  /**
   * LRUReplacer implements the Least Recently Used replacement policy.
   */
  class LRUReplacer : public Replacer {
   public:
    /**
     * Create a new LRUReplacer.
     * @param num_pages the maximum number of pages the LRUReplacer will be required to store
     */
    explicit LRUReplacer(size_t num_pages);
  
    /**
     * Destroys the LRUReplacer.
     */
    ~LRUReplacer() override;
  
    bool Victim(frame_id_t *frame_id) override;
  
    void Pin(frame_id_t frame_id) override;
  
    void Unpin(frame_id_t frame_id) override;
  
    size_t Size() override;
  
   private:
    // add your own private member variables here
    std::mutex mutx_;                // lock for threads
    std::list<frame_id_t> LRU_list;  // doubly-linked list for storage of frame_id_t -> implementation of least-recently
                                     // used (LRU algorithm)
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> LRU_hash;
    // hash table, for frame_id_t storage (first find the corresponding key, frame_id_t(hash version) ---map--->
    // linked_list's iterator) which can be accessed by using frame_id_t as a index, and the content is the iterator of
    // std::list -> which can be used to directly modify the linked-list's element and structure.
    size_t max_size;
  };
  ```

- 

- 

- `LRUReplacer::Victim(*frame_id)`：替换（即删除）与所有被跟踪的页相比最近最少被访问的页，将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数`frame_id`中输出并返回`true`，如果当前没有可以替换的元素则返回`false`；
  __实现思路说明 :__ 仅需将末尾的元素(即least recently used)的元素从LRU双向列表中剔除，并将其返回给上层模块，已经找到可以替换的一页。

  ```cpp
  // find the least-recently used element and clear it out.
  bool LRUReplacer::Victim(frame_id_t *frame_id) {
    // C++17 std::scoped_lock(in <mutex> header) can avoid the deadlock automatically.
    // The ctors can lock automatically on thread level.
    // The dtors will unlock automatically. Therefore keep the thread safe.
    std::scoped_lock lock{mutx_};
    if (LRU_list.empty()) {
      return false;
    }  // if the current buffer frame do not have elements which can be replaced, return false, the victim replacement
       // fails.
    *frame_id = LRU_list.back();  // get the least-recently used element -> lies at the tail of the doubly-linked list
    LRU_hash.erase(*frame_id);    // delete the mapping of the frame_id in the hash table.
    LRU_list.pop_back();          // remove the least-recently used element as a victim from the LRU list.
    return true;
  }
  ```

- `LRUReplacer::Pin(frame_id)`：将数据页固定使之不能被`Replacer`替换，即从`lru_list_`中移除该数据页对应的页帧。`Pin`函数应当在一个数据页被Buffer Pool Manager固定时被调用；
  __实现思路说明 :__ 此处注意，双向链表中存储的一定是可以被替换的页，不可以被替换的页需要从中移除。移除的具体方法，首先时通过哈希表找到对应的双向链表的迭代器，并将哈希表和迭代器中的页信息全部移除即可。

  ```cpp
  // get the specified frame_id out of the buffer frame. -> so that it can not be seen by the LRU algorithm, thus locked
  // into the buffer.
  void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock lock{mutx_};
    // if can not find the element in the hash table. Nothing to remove from the list. Just return, stop the execution.
    if (LRU_hash.count(frame_id) == 0) {
      return;
    }
    auto iter = LRU_hash[frame_id];
    LRU_list.erase(iter);  // remove the specified element(random access) in the doubly-linked list using a iterator of
                           // the linked-list
    LRU_hash.erase(frame_id);  // remove the corresponding element in the hash table.
  }
  ```

- `LRUReplacer::Unpin(frame_id)`：将数据页解除固定，放入`lru_list_`中，使之可以在必要时被`Replacer`替换掉。`Unpin`函数应当在一个数据页的引用计数变为`0`时被Buffer Pool Manager调用，使页帧对应的数据页能够在必要时被替换；
  __实现思路说明 :__ 将对应数据页解除固定，放入LRU替换策略中，需要注意的是此处添加的时候直接要添加到双向链表的头部，因为是刚刚使用过，默认为最常使用的页，序最大。需要先添加到双向链表中生成迭代器之后，再将对应的frame_id到迭代器的映射添加到哈希表中即可。

  ```cpp
  // put the frame_id into the buffer frame. Or get the pinned element into the LRU algorithm again.
  void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock lock{mutx_};
    // case 1: already have this frame_id in the buffer frame.
    // If the hash_table already has this frame_id, return to stop execution directly.
    // avoid the repeated addition of the element (the frame_id of the disk should be unique in the buffer frame.)
    if (LRU_hash.count(frame_id) != 0) {
      return;
    }
    // case 2: already reached the maximum size of buffer frame.
    // Have reached the maximum size of the buffer frame, can not unpin any more, should do the victim replacement first.
    if (LRU_list.size() == max_size) {
      return;
    }
    // case 3: normal case, add the frame_id to the head of the linked list -> most recently used.
    LRU_list.push_front(frame_id);
    LRU_hash.emplace(frame_id, LRU_list.begin());  // refresh the hash table
  }
  ```

- `LRUReplacer::Size()`：此方法返回当前`LRUReplacer`中能够被替换的数据页的数量。

  ```cpp
  size_t LRUReplacer::Size() { return LRU_list.size(); }  
  // return the current size of the buffer frame.
  ```

- LRUReplacer测试结果
  ![image-20220619164746091](picture/buffer_pool_mgr_lru_replacer.png)

## 2. 缓冲池管理

在实现Buffer Pool的替换算法`LRUReplacer`后，我们需要实现整个`BufferPoolManager`，与之相关的代码位于`src/include/buffer/buffer_pool_manager.h`和`src/buffer/buffer_pool_manager.cpp`中。Buffer Pool Manager负责从Disk Manager中获取数据页并将它们存储在内存中，并在必要时（dirty为1时才进行写回，如果为0则不进行写回，减少不必要的IO）将脏页面转储到磁盘中（如需要为新的页面腾出空间）。

数据库系统中，所有内存页面都由`Page`对象（`src/include/page/page.h`）表示，每个`Page`对象都包含了一段连续的内存空间`data_`和与该页相关的信息（如是否是脏页，页的引用计数等等）。注意，`Page`对象并不作用于唯一的数据页，它只是一个用于存放从磁盘中读取的数据页的容器。这也就意味着同一个`Page`对象在系统的整个生命周期内，可能会对应很多不同的物理页。`Page`对象的唯一标识符`page_id_`用于跟踪它所包含的物理页，如果`Page`对象不包含物理页，那么`page_id_`必须被设置为`INVALID_PAGE_ID`。每个`Page`对象还维护了一个计数器`pin_count_`，它用于记录固定(Pin)该页面的线程数。Buffer Pool Manager将不允许释放已经被固定的`Page`。每个`Page`对象还将记录它是否脏页，在复用`Page`对象之前必须将脏的内容转储到磁盘中。

在`BufferPoolManager`的实现中，你需要用到此前已经实现的`LRUReplacer`或是其它的`Replacer`，它将被用于跟踪`Page`对象何时被访问，以便`BufferPoolManager`决定在Buffer Pool中没有空闲页可以用于分配时替换哪个数据页。

- __上层接口调用规则 :__ 需要注意的是上层调用的时候不可以Fetch一个不存在的page，需要对第一次的page进行new操作才可以后续fetch，除此之外，需要在new或者fetch使用完成之后对页进行unpin操作。

因此，在这个模块中，需要实现以下函数：

- `BufferPoolManager::FetchPage(page_id)`：根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取；

- ```cpp
  
  ```

- `BufferPoolManager::NewPage(&page_id)`：分配一个新的数据页，并将逻辑页号于`page_id`中返回；

  ```cpp
  
  ```

- `BufferPoolManager::UnpinPage(page_id, is_dirty)`：取消固定一个数据页；

  ```cpp
  
  ```

- `BufferPoolManager::FlushPage(page_id)`：将数据页转储到磁盘中；

  ```cpp
  
  ```

- `BufferPoolManager::DeletePage(page_id)`：释放一个数据页；

  ```cpp
  
  ```

- `BufferPoolManager::FlushAllPages()`：将所有的页面都转储到磁盘中；

  ```cpp
  
  ```

- 测试结果
  ![image-20220619165127083](picture/buffer_pool_mgr.png)

对于`FetchPage`操作，如果空闲页列表（`free_list_`）中没有可用的页面并且没有可以被替换的数据页，则应返回 `nullptr`。`FlushPage`操作应该将页面内容转储到磁盘中，无论其是否被固定。

## 3. 模块相关代码

- `src/include/buffer/lru_replacer.h`
- `src/buffer/lru_replacer.cpp`
- `src/include/buffer/buffer_pool_manager.h`
- `src/buffer/buffer_pool_manager.cpp`
- `test/buffer/buffer_pool_manager_test.cpp`
- `test/buffer/lru_replacer_test.cpp`