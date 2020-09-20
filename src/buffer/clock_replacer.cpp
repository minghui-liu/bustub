//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include "common/logger.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    // clock hand
    num_pages_ = num_pages;
    clock_hand_ = 0;
    frames_ = new Frame[num_pages];
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    frame_id_t start = clock_hand_;
    bool second_round = false;
    do {
        if (frames_[clock_hand_].in_replacer_) {
            // at least one frame is in replacer, do a second
            // round in case all frames have ref_ == true
            second_round = true;
            if (frames_[clock_hand_].ref_ == false) {
                *frame_id = clock_hand_;
                frames_[clock_hand_].in_replacer_ = false;
                return true;
            } else {
                frames_[clock_hand_].ref_ = false;   
            }
        }
        clock_hand_ = (clock_hand_+1) % num_pages_;
    } while (clock_hand_ != start);
    if (second_round) {
        do {
            if (frames_[clock_hand_].in_replacer_ && frames_[clock_hand_].ref_ == false) {
                *frame_id = clock_hand_;
                frames_[clock_hand_].in_replacer_ = false;
                return true;
            }
            clock_hand_ = (clock_hand_+1) % num_pages_;
        } while (clock_hand_ != start);
    }
    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    frames_[frame_id].in_replacer_ = false;
    // frames_[frame_id].ref_ = true;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    frames_[frame_id].in_replacer_ = true;
    frames_[frame_id].ref_ = true;
}

size_t ClockReplacer::Size() {
    size_t size = 0;
    for (size_t i = 0; i < num_pages_; i++) {
        if (frames_[i].in_replacer_ == true) {
            size++;
        }
    }
    return size;
}

void ClockReplacer::DebugPrint() {
    for (size_t i = 0; i < num_pages_; i++) {
        LOG_DEBUG("%lu  in_replacer: %d  ref: %d", i, frames_[i].in_replacer_, frames_[i].ref_);
    }
}

}  // namespace bustub
