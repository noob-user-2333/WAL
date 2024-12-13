//
// Created by user on 24-12-5.
//

#include "wal.h"
#include "os.h"

namespace iedb {
#define maybe_error(x) {status = x;if (x != status_ok) goto error_handle;}
#define judge_error(x) {status = x;if (status != status_ok) return status;}
    static uint8 page_map[(1L << 32) / page_size / 8];
    //将page_no对应位设置为1，成功返回status_ok,失败返回status_error
    static int page_map_set(const uint32 page_no) {
        auto byte = page_no / 8;
        auto bit = page_no % 8;
        auto mask = 1U << bit;
        if (page_map[byte] & mask)
            return status_error;
        page_map[byte] |= mask;
        return status_ok;
    }
    struct wal_meta {
        char magic_string[16]; //用于验证该文件是否为有效wal日志
        uint64 source_file_size; //原文件大小
        uint64 change_count; //文件修改次数，每次提交事务+1
        uint64 wal_size; //表示日志文件有效数据到多少偏移量为止
                        //当恢复时直接从该处开始，从后向前扫描
        uint64 contain_page;//该日志中包含的有效数据页
        uint64 format_version;

        wal_meta(const uint64 source_file_size, const uint64 change_count, const uint64 valid_size, const uint64 contain_page): magic_string(),
            source_file_size(source_file_size), change_count(change_count), wal_size(valid_size),
            contain_page(contain_page),format_version(current_format_version) {
            memcpy(magic_string, wal_magic_string, sizeof(wal_magic_string));
        }

        wal_meta() = default;
    };

    struct wal_page_meta {
        uint32 page_no;
        uint32 check_sum;
        wal_page_meta() = default;
        wal_page_meta(uint32 page_no,uint32 check_sum): page_no(page_no), check_sum(check_sum) {}
    };

    static constexpr int wal_page_size = sizeof(wal_page_meta) + page_size;

    wal::wal(std::string file_name,int fd,int fd_wal)
        : file_name(std::move(file_name)),fd(fd)
          , fd_wal(fd_wal),status(wal_status::normal),wal_page_map(),private_data(){
    }

    wal::~wal() {
        os::close(fd);
        os::close(fd_wal);
    }

    int wal::validate(const void *wal_data) {
        auto meta = static_cast<const wal_meta*>(wal_data);
        if (memcmp(meta->magic_string, wal_magic_string, sizeof(wal_magic_string))!= 0) {
            fprintf(stderr, "wal头部magic_string不匹配\n");
            return status_error;
        }
        return status_ok;
    }

    uint32 wal::get_wal_page_count(uint64 wal_size) {
        return (wal_size - sizeof(wal_meta)) / (page_size + sizeof(wal_page_meta));
    }
    int64 wal::get_wal_page_offset(uint32 wal_page_no) {
        return sizeof(wal_meta) + wal_page_no * (page_size + sizeof(wal_page_meta));
    }
    uint32 wal::compute_check_sum(const void *page_start) {
        uint32 check_sum = 0;
        for (auto i = 0; i < page_size / sizeof(uint32); i++) {
            check_sum += static_cast<const uint32*>(page_start)[i];
        }
        return check_sum;
    }
    /*
     *  该函数通过遍历日志的方式进行故障恢复
     *  为了便于恢复，日志的有效数据将被映射到内存中
     */
    int wal::fault_recovery(int fd, int fd_wal,void* meta_data) {
        assert(fd > 0 && fd_wal >0);
        static constexpr int wal_page_size = sizeof(wal_page_meta) + page_size;
        auto meta = static_cast<wal_meta*>(meta_data);
        uint64 file_size = 0;
        //首先获取源文件大小
        int status = os::get_file_size(fd,file_size);
        uint8 buff[wal_page_size];
        for ( auto offset = static_cast<int64>(meta->wal_size) - wal_page_size;
            offset > 0 ;offset -= wal_page_size){
            judge_error(os::read(fd_wal,offset,buff,wal_page_size));
            auto page_meta = static_cast<wal_page_meta*>(static_cast<void *>(buff));
            auto page_start = buff + sizeof(wal_page_meta);
            //检查该页是否有效
            if(page_meta->check_sum != compute_check_sum(page_start)) {
                return status_invalid_wal_page;
            }
            //检查该页是否已被写入
            status = page_map_set(page_meta->page_no);
            //对应位设置失败说明该页已被写入，略过
            if (status!= status_ok)
                continue;
            //将该页写回原文件
            //首先确定是否需要扩展文件
            auto page_count = file_size / page_size;
            if (page_count <= page_meta->page_no)
                os::fallocate(fd,page_size * page_meta->page_no,page_size);
            status = os::write(fd,page_meta->page_no * page_size,page_start,page_size);
            if (status!= status_ok) {
                fprintf(stderr, "写回原文件失败 page_no=%d\n", page_meta->page_no);
                break;
            }
        }
        memset(page_map,0,sizeof(page_map));
        //写入完毕后清空日志
        meta->source_file_size = file_size;
        meta->change_count ++;
        meta->contain_page = 0;
        meta->wal_size = sizeof(wal_meta);
        judge_error(os::write(fd_wal,0,&meta,sizeof(wal_meta)));
        return status;
    }


    std::unique_ptr<wal> wal::create(const char *filename) {
        //开启对应文件
        int fd;
        const int status = os::open(filename, os::open_mode_read_write | os::open_mode_create, 0666, fd);
        if (status != status_ok) {
            fprintf(stderr, "创建或开启原文件失败 %s\n", filename);
            return nullptr;
        }
        return create(filename, fd);
    }

    std::unique_ptr<wal> wal::create(const char *filename, int fd) {
        assert(filename && fd > 0);
        auto file_name = std::string(filename);
        auto wal_name = file_name + "-wal";
        int fd_wal,status;
        uint64 sour_file_size;
        //首先检查是否存在对应日志文件
        bool exists = os::access(wal_name.c_str(), os::access_mode_file_exists) == 0;
        maybe_error(os::open(wal_name.c_str(),
            os::open_mode_read_write | os::open_mode_create, 0666, fd_wal));
        maybe_error(os::get_file_size(fd,sour_file_size));
        //如果是新建日志文件
        if (exists == false) {
            //初始化日志文件
            const wal_meta meta(sour_file_size, 0, sizeof(wal_meta),0);
            maybe_error(os::write(fd_wal, 0, &meta, sizeof(meta)));
            maybe_error(os::fdatasync(fd_wal));
        } else {
            //如果非新建日志文件
            //检查日志
            wal_meta meta{};
            maybe_error(os::read(fd_wal, 0, &meta, sizeof(meta)));
            maybe_error(wal::validate(&meta));
            //如果日志中存在数据，则将数据写入原文件
            if (meta.contain_page) {
                maybe_error(wal::fault_recovery(fd, fd_wal, &meta));
            }
        }
        return std::unique_ptr<wal>(new wal(std::move(file_name), fd, fd_wal));
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
        fprintf(stderr, "error_code = %d 无法创建WAL_item\n", status);
        return nullptr;
    }

    int wal::begin_transaction() {
        assert(status == wal_status::normal);
        //读取元数据并切换状态
        auto _status = os::read(fd_wal,0,private_data,sizeof(wal_meta));
        if(_status != status_ok)
            return _status;
        status = wal_status::transaction;
        return status_ok;
    }
    int wal::commit_transaction() {
        assert(status == wal_status::transaction);
        auto meta = (wal_meta*)(private_data);
        meta->change_count++;
        auto _status = os::write(fd_wal,0,private_data,sizeof(wal_meta));
        if (_status != status_ok)
            return _status;
        _status = os::fdatasync(fd_wal);
        if (_status != status_ok)
            return _status;
        status = wal_status::normal;
        return status_ok;
    }

    int wal::commit_changes() {
        assert(status == wal_status::normal);
        auto meta = (wal_meta*)(private_data);
        uint8 buff[wal_page_size];
        //文件恢复到原长度
        os::ftruncate(fd,meta->source_file_size);
        //遍历hash
        for (auto it :wal_page_map) {
            auto sour_dest_page = it.first;
            auto wal_sour_page = it.second;
            //准备写入
            auto sour_offset = page_size * sour_dest_page;
            auto wal_offset = get_wal_page_offset(wal_sour_page);
            os::read(fd_wal,wal_offset,buff,wal_page_size);
            auto page_meta = (wal_page_meta*)buff;
            auto page_start = buff + sizeof(wal_page_meta);
            if (page_meta->check_sum != compute_check_sum(page_start))
                return status_invalid_wal_page;
            os::write(fd,sour_offset,page_start,page_size);
        }
        //修改meta并写入
        meta->change_count++;
        meta->contain_page = 0;
        meta->wal_size = sizeof(wal_meta);
        auto _status = os::write(fd_wal,0,private_data,sizeof(wal_meta));
        if (_status != status_ok)
            return _status;
        wal_page_map.clear();
        return status_ok;
    }



    int wal::read(uint32 page_no,void * buf) {
        assert(status == wal_status::transaction);
        const auto it = wal_page_map.find(page_no);
        //如果未被缓存到hash中，则从原文件读取
        if (it == wal_page_map.end())
            return os::read(fd, page_size * page_no, buf, page_size);
        //如果已被缓存到hash中，则直接返回
        auto offset = get_wal_page_offset(it->second) + sizeof(wal_page_meta);
        return os::read(fd_wal, offset, buf, page_size);
    }
    int wal::write(uint32 page_no,const void *buf) {
        assert(status == wal_status::transaction);
        auto meta = (wal_meta*)private_data;
        auto count = get_wal_page_count(meta->wal_size);
        auto offset = static_cast<int64>(meta->wal_size);
        //检查WAL中是否存在该页面
        auto it = wal_page_map.find(page_no);
        if (it == wal_page_map.end())
            meta->contain_page++;
        //修改映射
        wal_page_map[page_no] = count;
        //如果不存在，则
        uint8 buff[wal_page_size];
        auto page_meta = (wal_page_meta*)buff;
        auto page_start = buff + sizeof(wal_page_meta);
        memcpy(page_start,buf,page_size);
        page_meta->page_no = page_no;
        page_meta->check_sum = compute_check_sum(page_start);
        meta->wal_size += wal_page_size;
        return os::write(fd_wal,offset,buff,wal_page_size);

    }




}
