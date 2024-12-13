//
// Created by user on 24-12-13.
//

#ifndef TEST_H
#define TEST_H
#include "os.h"


namespace iedb {
    static constexpr int test_data_size = 16 * 1024 * 1024;
    static void * get_test_data() {
        static uint8 buff[test_data_size];
        static constexpr char test_filename[] = "/home/user/random.bin";
        int fd;
        assert(os::open(test_filename,os::open_mode_read,0,fd) == status_ok);
        assert(os::read(fd,0,buff,test_data_size) == status_ok);
        os::close(fd);
        return buff;
    }


}



#endif //TEST_H
