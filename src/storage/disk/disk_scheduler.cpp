//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  /*throw NotImplementedException(
      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
      "throw exception line in `disk_scheduler.cpp`.");*/

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional(std::move(r))); }

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto request_opt = request_queue_.Get();
    if (!request_opt.has_value()) {
      break;
    }
    DiskRequest req = std::move(request_opt.value());

    if (req.is_write_) {
      disk_manager_->WritePage(req.page_id_, req.data_);
    } else {
      disk_manager_->ReadPage(req.page_id_, req.data_);
    }
    req.callback_.set_value(true);
  }
}

void DiskScheduler::WritePageToDisk(Page *page) {
  DiskRequest write_request;
  write_request.is_write_ = true;
  write_request.page_id_ = page->GetPageId();
  write_request.data_ = page->GetData();
  Schedule(std::move(write_request));
}
void DiskScheduler::ReadPage(bustub::Page *page) {
  DiskRequest read_request;
  read_request.is_write_ = false;
  read_request.data_ = page->GetData();
  read_request.page_id_ = page->GetPageId();
  read_request.callback_.set_value(true);

  Schedule(std::move(read_request));
}

}  // namespace bustub
