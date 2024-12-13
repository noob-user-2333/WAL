//
// Created by user on 24-11-26.
//

#ifndef UTILITY_H
#define UTILITY_H
#include <vector>
#include <string>
#include <cstring>
#include <memory>
#include <cstdio>
#include <cassert>

namespace iedb {
    using int8 = char;
    using uint8 = unsigned char;
    using int16 = short;
    using uint16 = unsigned short;
    using int32 = int;
    using uint32 = unsigned int;
    using int64 = long;
    using uint64 = unsigned long;

    static constexpr uint32 page_size = 0x1000;


    static constexpr int status_error= -1;
    static constexpr int status_ok = 0;
    static constexpr int status_invalid_argument = 1;
    static constexpr int status_no_space = 2;
    static constexpr int status_invalid_fd = 3;
    static constexpr int status_not_access  = 4;
    static constexpr int status_io_error = 5;
    static constexpr int status_argument_overflow = 6;
    static constexpr int status_invalid_wal_page = 7;

    static constexpr char wal_magic_string[16] = "iedb wal file";
    static constexpr char wal_index_magic_string[16] = "iedb wal index";
    static constexpr int wal_index_hash_size = 32 * 1024;
    static constexpr int wal_index_item_per_hash = wal_index_hash_size / sizeof(uint32);
    static constexpr uint64 current_format_version = 0x0001000;

};

#endif //UTILITY_H
