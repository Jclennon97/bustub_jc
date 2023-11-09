#include <unistd.h>
#include <algorithm>
#include <cstring>
#include <optional>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return GetRootPageId() == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (IsEmpty()) {
    return false;
  }
  // 找到叶子节点
  LeafPage *leaf_page = FindLeafPage(key);
  // 找到值
  ValueType v;
  bool is_exist = leaf_page->ValueAtKey(key, v, comparator_);
  if (!is_exist) {
    return false;
  }
  result->push_back(v);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> LeafPage * {
  BasicPageGuard node_guard = bpm_->FetchPageBasic(GetRootPageId());
  auto node = node_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    int index = internal_node->GetKeyIndex(key, comparator_);
    page_id_t next_page_id = internal_node->ValueAt(index);
    BasicPageGuard next_node_guard = bpm_->FetchPageBasic(next_page_id);
    node = next_node_guard.As<BPlusTreePage>();
  }
  auto ret = reinterpret_cast<LeafPage *>(node);
  return ret;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::move(header_page_guard);
  // 往下走找到叶节点
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  ctx.write_set_.push_front(std::move(root_page_guard));
  auto cur_page = ctx.write_set_.back().As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(cur_page);
    int index = internal_node->GetKeyIndex(key, comparator_);
    page_id_t next_page_id = internal_node->ValueAt(index);
    WritePageGuard next_node_guard = bpm_->FetchPageWrite(next_page_id);
    ctx.write_set_.push_back(std::move(next_node_guard));
    cur_page = ctx.write_set_.back().As<BPlusTreePage>();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(cur_page);
  WritePageGuard leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  if (!leaf_page->Insert(key, value, comparator_)) {
    return false;
  }
  // 如果叶节点满了，则需要分裂
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = bpm_->NewPage(&new_page_id);
    if (new_page_id == INVALID_PAGE_ID) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "cannot allocate new page");
    }
    new_page->WLatch();
    auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf_page->Init(leaf_max_size_);
    new_leaf_page->SetPageId(new_page_id);
    // 转移一半到新节点，旧节点数量为min_size;
    auto risen_key = leaf_page->Split(new_leaf_page);
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_page_id);

    InsertIntoParent(leaf_page, risen_key, new_leaf_page, &ctx);
    new_page->WUnlatch();
    bpm_->UnpinPage(new_page_id, true);
  }
  leaf_page_guard.GetDataMut();
  leaf_page_guard.Drop();
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.back().Drop();
    ctx.write_set_.pop_back();
  }
  ctx.header_page_->Drop();
  ctx.header_page_ = std::nullopt;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(const BPlusTreePage *old_page, const KeyType &risenkey,
                                      const BPlusTreePage *sbling_page, Context *ctx) {
  if (ctx->IsRootPage(old_page->GetPageId())) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_page = bpm_->NewPage(&new_page_id);
    if (new_page_id == INVALID_PAGE_ID) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "cannot allocate new page");
    }
    new_page->WLatch();
    auto new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_page->Init(internal_max_size_);
    new_root_page->SetPageId(new_page_id);
    new_root_page->CreatNewRoot(risenkey, old_page->GetPageId(), sbling_page->GetPageId());
    auto header_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_page_id;
    ctx->root_page_id_ = new_page_id;
    new_page->WUnlatch();
    bpm_->UnpinPage(new_page_id, true);
    return;
  }
  WritePageGuard parent_page_guard = std::move(ctx->write_set_.back());
  auto parent_page = parent_page_guard.AsMut<InternalPage>();
  ctx->write_set_.pop_back();

  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    parent_page->Insert(risenkey, sbling_page->GetPageId(), comparator_);
    parent_page_guard.Drop();
    return;
  }

  auto *mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_page->GetSize() + 1)];
  auto copy_page = reinterpret_cast<InternalPage *>(mem);
  std::memcpy(mem, parent_page_guard.GetData(),
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * parent_page->GetSize());
  copy_page->Insert(risenkey, sbling_page->GetPageId(), comparator_);

  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_split_page = bpm_->NewPage(&new_page_id);
  if (new_page_id == INVALID_PAGE_ID) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "cannot allocate new page");
  }
  new_split_page->WLatch();
  auto new_internal_page = reinterpret_cast<InternalPage *>(new_split_page->GetData());
  new_internal_page->Init(internal_max_size_);
  new_internal_page->SetPageId(new_page_id);
  auto risen_key = copy_page->Split(new_internal_page);
  (void)std::memcpy(parent_page_guard.GetDataMut(), mem,
                    INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copy_page->GetMinSize());
  InsertIntoParent(parent_page, risen_key, new_internal_page, ctx);
  new_split_page->WUnlatch();
  bpm_->UnpinPage(new_page_id, true);
  parent_page_guard.Drop();
  delete[] mem;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) -> bool {
  WritePageGuard header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = bpm_->NewPage(&new_page_id);
  if (new_page_id == INVALID_PAGE_ID) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "cannot allocate new page");
  }
  new_page->WLatch();
  auto root_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  root_page->Init(leaf_max_size_);
  root_page->SetPageId(new_page_id);
  if (!root_page->Insert(key, value, comparator_)) {
    return false;
  }
  header_page->root_page_id_ = new_page_id;
  new_page->WUnlatch();
  bpm_->UnpinPage(new_page_id, true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    throw std::exception();
  }
  BasicPageGuard head_page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto head_page = head_page_guard.As<BPlusTreeHeaderPage>();
  auto page = bpm_->FetchPageBasic(head_page->root_page_id_).As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    auto first_page_id = internal_page->ValueAt(0);
    BasicPageGuard page_guard = bpm_->FetchPageBasic(first_page_id);
    page = page_guard.As<BPlusTreePage>();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page);
  return INDEXITERATOR_TYPE(leaf_page, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  BasicPageGuard head_page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto head_page = head_page_guard.As<BPlusTreeHeaderPage>();
  auto page = bpm_->FetchPageBasic(head_page->root_page_id_).As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    int index = internal_page->GetKeyIndex(key, comparator_);
    auto page_id = internal_page->ValueAt(index);
    BasicPageGuard page_guard = bpm_->FetchPageBasic(page_id);
    page = page_guard.As<BPlusTreePage>();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page);
  auto key_index = leaf_page->KetIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf_page, key_index, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  BasicPageGuard head_page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto head_page = head_page_guard.As<BPlusTreeHeaderPage>();
  auto page = bpm_->FetchPageBasic(head_page->root_page_id_).As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    auto first_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    BasicPageGuard page_guard = bpm_->FetchPageBasic(first_page_id);
    page = page_guard.As<BPlusTreePage>();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page);
  return INDEXITERATOR_TYPE(leaf_page, leaf_page->GetSize(), bpm_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
