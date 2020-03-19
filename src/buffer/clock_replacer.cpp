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

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    // clock hand
    clock_hand = 0;
    for (size_t i = 0; i < num_pages; i++) {
        ref.push_back(false);
        in_replacer.push_back(false);
    }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    frame_id_t start = clock_hand;
    bool second_round = false;
    clock_hand = (clock_hand+1) % ref.size();
    while (clock_hand != start) {
        if (in_replacer.at(clock_hand)) {
            if (ref.at(clock_hand) == false) {
                *frame_id = clock_hand;
                in_replacer.at(clock_hand) = false;
                return true;
            } else {
                ref.at(clock_hand) = false;   
            }
            second_round = true;  
        }
        // move clock_hand
        clock_hand = (clock_hand+1) % ref.size();
    }
    if (second_round) {
        clock_hand = (clock_hand+1) % ref.size();
        while (clock_hand != start) {
            if (in_replacer.at(clock_hand) && ref.at(clock_hand) == false) {
                *frame_id = clock_hand;
                in_replacer.at(clock_hand) = false;
                return true;
            }
            // move clock_hand
            clock_hand = (clock_hand+1) % ref.size();
        }
    }
    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    in_replacer.at(frame_id) = false;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    in_replacer.at(frame_id) = true;
    ref.at(frame_id) = true;
}

size_t ClockReplacer::Size() { 
    size_t size = 0;
    for (auto t=in_replacer.begin(); t!=in_replacer.end(); t++) {
        if (*t == true) {
            size++;
        }
    }
    return size;
}

}  // namespace bustub
