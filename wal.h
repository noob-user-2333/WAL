#ifndef WAL_ITEM_H
#define WAL_ITEM_H
#include <unordered_map>

#include "utility.h"
/*  整个WAL机制可分为3个部分
 *  分别为:
 *      1、原文件，所有数据最终都会写入该文件
 *      2、WAL日志，由一个文件头与多个数据帧组成，每个数据帧对应一个对原文件一个数据块被修改后的结果
 *      3、WAL-index，一个hash表
 */
namespace iedb {
    enum  class wal_status {
        error,
        normal,
        transaction
    };
    class wal {
    private:
        std::string file_name;
        int fd;
        int fd_wal;
        wal_status status;
        uint8 private_data[128];
        //将原文件数据页号映射为WAL数据页号
        std::unordered_map<uint32,uint32> wal_page_map;

        wal(std::string file_name, int fd, int fd_wal);

        //计算该日志中包含了多少数据页
        static uint32 get_wal_page_count(uint64 wal_size);
        //计算对应wal页在日志中的偏移量
        static int64 get_wal_page_offset(uint32 page_no);
        //给定wal有效部分的起始映射地址和有效部分长度，计算对应页面在内存的起始地址
        //注:页码为从有效部分末尾到开头依次递增，从0开始
        static uint32 compute_check_sum(const void * page_start);
        static int validate(const void * wal_data);
        static int fault_recovery(int fd,int fd_wal,void * meta_data);
    public:
        ~wal();

        int commit_changes();
        int begin_transaction();
        int commit_transaction();
        int read(uint32 page_no,void * buf);
        int write(uint32 page_no,const void *buf);
        //若不存在filename对应文件的WAL日志则创建页大小为page_size的日志
        //否则根据对应WAL日志恢复文件
        static std::unique_ptr<wal> create(const char *filename);
        static std::unique_ptr<wal> create(const char *filename,int fd);
    };
}
#endif //WAL_ITEM_H
