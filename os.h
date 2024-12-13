//
// Created by user on 24-12-5.
//

#ifndef OS_H
#define OS_H
#include "utility.h"

namespace iedb {
    class os {
    public:
        static const int open_mode_read;
        static const int open_mode_write;
        static const int open_mode_append;
        static const int open_mode_read_write;
        static const int open_mode_create;
        static const int open_mode_truncate;
        static const int access_mode_file_exists;
        static const int access_mode_can_read;
        static const int access_mode_can_write;
        static const int access_mode_can_execute;
        static const int seek_mode_set;
        static const int seek_mode_current;
        static const int seek_mode_end;
        static int open(const char *path,int mode,int flags,int& out_fd);
        static int access(const char *path,int mode);
        static int close(int fd);
        static int seek(int fd,int64 offset,int mode,int64 & out_current_offset);
        static int write(int fd,int64 offset,const void *buf,uint64 count);
        static int write(int fd,const void *buf,uint64 count);
        static int read(int fd,int64 offset,void *buf,uint64 count);
        static int fallocate(int fd,int64 offset,int64 length);
        static int fdatasync(int fd);
        static int unlink(const char *path);
        static int get_file_size(int fd,uint64 & out_size);
        //默认映射区为可读写且修改将被同步到文件中
        // static int mmap(int fd,int64 offset,uint64 length,void* & out_start);
        // static int munmap(void * start,uint64 length);
        static int ftruncate(int fd, int64 length);

    };
}


#endif //OS_H
