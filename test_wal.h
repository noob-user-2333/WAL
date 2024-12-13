//
// Created by user on 24-12-13.
//

#ifndef TEST_WAL_H
#define TEST_WAL_H
#include "wal.h"
#include "test.h"
namespace iedb {
    void test_wal() {
        static constexpr char filename[] = "data/test.iedb";
        static constexpr char test_filename[] = "/home/user/random.bin";
        static uint64 buff[1024];
        auto test_data = static_cast<uint8*>(get_test_data());
        //首先测试文件创建
        auto wal = wal::create(filename);
        wal->begin_transaction();
        for (int i = 0 ; i < 1024;i++) {
            wal->write(i,test_data + i * page_size);
        }
        wal->commit_transaction();
        wal->commit_changes();

        //验证
        wal->begin_transaction();
        for (int i = 0 ; i < 1024;i++) {
            wal->read(i,buff);
            auto res = memcmp(buff,test_data + i * page_size,page_size);
            if (res) {
                fprintf(stderr,"数据页第%d页出现错误\n",i);
            }
        }
        wal->commit_transaction();
    }
}

#endif //TEST_WAL_H
