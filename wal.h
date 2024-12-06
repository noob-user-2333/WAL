#ifndef WAL_ITEM_H
#define WAL_ITEM_H
#include "utility.h"
/*  整个WAL机制可分为3个部分
 *  分别为:
 *      1、原文件，所有数据最终都会写入该文件
 *      2、WAL日志，由一个文件头与多个数据帧组成，每个数据帧对应一个对原文件一个数据块被修改后的结果
 *      3、WAL-index，内存映射文件，由一个文件头和一个hash表组成，用于快速索引WAL日志中保存的原文件数据
 *                   注：该文件无需保证正确性，当检测到损坏时会从WAL日志重建
 *
 *
 */
namespace iedb {
    enum class wal_status {
        begin,
        normal,
        transaction,
        error
    };


    class wal {
    private:
    private:
        std::string file_name;
        int fd;
        int fd_wal;
        int fd_index;
        wal_status status;


        wal(std::string file_name, int fd, int fd_wal,int fd_index);

    public:
        ~wal();

        int begin_transaction();
        int commit_transaction();
        int read(uint32 start_page_no,uint32 page_count,void * buf);
        int write(uint32 start_page_no,uint32 page_count,const void *buf);
        //若不存在filename对应文件的WAL日志则创建页大小为page_size的日志
        //否则根据对应WAL日志恢复文件
        static std::unique_ptr<wal> create(const char *filename);
        static std::unique_ptr<wal> create(const char *filename,int fd);
    };
}
#endif //WAL_ITEM_H
