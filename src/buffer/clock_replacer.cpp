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
        frames.push_back({false, false});
    }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    frame_id_t start = clock_hand;
    bool second_round = false;
    clock_hand = (clock_hand+1) % frames.size();
    while (clock_hand != start) {
        if (frames.at(clock_hand).in_replacer) {
            if (frames.at(clock_hand).ref == false) {
                *frame_id = clock_hand;
                frames.at(clock_hand).in_replacer = false;
                return true;
            } else {
                frames.at(clock_hand).ref = false;   
            }
            // at least one frame is in replacer, do a second
            // round in case all frames have ref == true
            second_round = true;  
        }
        // move clock_hand
        clock_hand = (clock_hand+1) % frames.size();
    }
    if (second_round) {
        clock_hand = (clock_hand+1) % frames.size();
        while (clock_hand != start) {
            if (frames.at(clock_hand).in_replacer && frames.at(clock_hand).ref == false) {
                *frame_id = clock_hand;
                frames.at(clock_hand).in_replacer = false;
                return true;
            }
            // move clock_hand
            clock_hand = (clock_hand+1) % frames.size();
        }
    }
    return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    frames.at(frame_id).in_replacer = false;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    frames.at(frame_id).in_replacer = true;
    frames.at(frame_id).ref = true;
}

size_t ClockReplacer::Size() { 
    size_t size = 0;
    for (auto t=frames.begin(); t!=frames.end(); t++) {
        if ((*t).in_replacer == true) {
            size++;
        }
    }
    return size;
}

}  // namespace bustub
