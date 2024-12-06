//
// Created by user on 24-12-5.
//

#include "wal.h"
#include "os.h"

namespace iedb {
    static uint8 page_map[ (1L << 32) / page_size / 8];

    struct wal_meta {
        char magic_string[16]; //用于验证该文件是否为有效wal日志
        uint64 source_file_size; //原文件大小
        uint64 change_count; //文件修改次数，每次提交事务+1
        uint64 wal_size; //表示日志文件有效数据到多少偏移量为止
                            //当恢复时直接从该处开始，从后向前扫描
        uint64 format_version;
        wal_meta(uint64 source_file_size, uint64 change_count,uint64 valid_size):magic_string(),
        source_file_size(source_file_size),change_count(change_count),wal_size(valid_size),
        format_version(current_format_version) {
            memcpy(magic_string,wal_magic_string,sizeof(wal_magic_string));
        }
        wal_meta() = default;
    };
    struct wal_index_header {
        char magic_string[16];
        uint64 change_count;
        uint64 file_size; //包含有效数据的文件大小
        uint64 page_count;//hash表中保存的能有效索引的页面的数量
        wal_index_header() = default;
        wal_index_header(uint64 change_count,uint64 file_size,uint64 page_count):
        magic_string(), change_count(change_count), file_size(file_size), page_count(page_count) {
            memcpy(magic_string, wal_index_magic_string, sizeof(wal_index_magic_string));
        }
    };



    wal::wal(std::string file_name, int fd, int fd_wal)
        : file_name(std::move(file_name))
          , fd(fd), fd_wal(fd_wal),
          status(wal_status::begin) {
    }

    wal::~wal() {
        os::close(fd);
        os::close(fd_wal);
    }



    std::unique_ptr<wal> wal::create(const char *filename) {
        //检查原文件是否存在
        auto exists = os::access(filename, os::access_mode_file_exists) == status_ok;
        if (exists == false) {
            fprintf(stderr, "原文件不存在 %s\n", filename);
            return nullptr;
        }
        //开启对应文件
        int fd;
        const int status = os::open(filename, os::open_mode_read_write | os::open_mode_create, 0666, fd);
        if (status != status_ok) {
            fprintf(stderr, "创建或开启原文件失败 %s\n", filename);
            return nullptr;
        }
        return create(filename,fd);
    }

    std::unique_ptr<wal> wal::create(const char *filename,int fd) {
        assert(filename && fd > 0);
        auto file_name = std::string(filename);
        auto wal_name = file_name + "-wal";
        auto index_name = file_name + "-index";
        int fd_wal,fd_index,status;
        uint64 sour_file_size;
        bool exists;
        //首先检查是否存在对应日志文件
        exists = os::access(wal_name.c_str(), os::access_mode_file_exists) == 0;
        status = os::open(wal_name.c_str(), os::open_mode_read_write | os::open_mode_create, 0666, fd_wal);
        if (status != status_ok)
            goto error_handle;
        status = os::open(index_name.c_str(), os::open_mode_read_write | os::open_mode_create, 0666, fd_index);
        if (status!= status_ok)
            goto error_handle;
        status = os::get_file_size(fd,sour_file_size);
        if (status!= status_ok)
            goto error_handle;
        //如果是新建日志文件
        if (exists == false) {
            //初始化日志文件
            wal_meta meta(sour_file_size,0,sizeof(wal_meta));
            status = os::write(fd_wal, 0, &meta, sizeof(meta));
            if (status != status_ok)
                goto error_handle;
            status = os::fdatasync(fd_wal);
            if (status != status_ok)
                goto error_handle;
            //初始化索引文件
            wal_index_header index_header(0,wal_index_hash_size,0);
            memcpy(page_map,&index_header,sizeof(wal_index_header));
            status = os::write(fd_index,0,page_map,wal_index_hash_size);
            memset(page_map,0,sizeof(wal_index_header));
            if (status != status_ok)
                goto error_handle;
        }else {
            //如果非新建日志文件
            //检查日志和索引文件是否正常
            wal_meta meta{};
            wal_index_header header{};
            status = os::read(fd_wal, 0, &meta, sizeof(meta));
            if (status!= status_ok)
                goto error_handle;
            if (memcmp(meta.magic_string,wal_magic_string, sizeof(wal_magic_string))!= 0) {
                fprintf(stderr, "wal magic_string 错误\n");
                goto error_handle;
            }
            if (meta.source_file_size!= sour_file_size) {
                fprintf(stderr, "wal source_file_size和原文件大小不一，错误\n");
                goto error_handle;
            }
            status = os::read(fd_index,0,&header, sizeof(header));
            if (status!= status_ok)
                goto error_handle;
            if (memcmp(header.magic_string,wal_index_magic_string, sizeof(wal_index_magic_string))!= 0) {
                fprintf(stderr, "wal index magic_string 错误,请删除该index文件或更换正确的index文件\n");
                goto error_handle;
            }
            //检查二者chang_count是否一致，如不一致则仅从wal日志中恢复




        }

        return std::make_unique<wal>(std::move(file_name), fd, fd_wal);
    error_handle:
        if (fd > 0) {
            os::close(fd);
        }
        if (fd_wal > 0) {
            os::close(fd_wal);
            //创建日志后失败需删除对应日志文件
            if (exists == false) {
                os::unlink(wal_name.c_str());
            }
        }
        if (fd_index > 0) {
            os::close(fd_index);
            //创建日志后失败需删除对应索引文件
            if (exists == false) {
                os::unlink(wal_name.c_str());
            }
        }
        fprintf(stderr, "error_code = %d 无法创建WAL_item\n", status);
        return nullptr;
    }
}
