//
// Created by user on 24-12-5.
//

#include "os.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace iedb {
    const int os::open_mode_read = O_RDONLY;
    const int os::open_mode_write = O_WRONLY;
    const int os::open_mode_append = O_APPEND;
    const int os::open_mode_read_write = O_RDWR;
    const int os::open_mode_create = O_CREAT;
    const int os::open_mode_truncate = O_TRUNC;
    const int os::access_mode_file_exists = F_OK;
    const int os::access_mode_can_read = R_OK;
    const int os::access_mode_can_write = W_OK;
    const int os::access_mode_can_execute = X_OK;


    static int errno_to_status_code(const int error_number) {
        switch (error_number) {
            case 0:
                return status_ok;
            case ENOSPC:
                return status_no_space;
            case EACCES:
                return status_not_access;
            case EIO:
                return status_io_error;
            case EINVAL:
                return status_invalid_argument;
            case EBADF:
                return status_invalid_fd;
            case EOVERFLOW:
                return status_argument_overflow;
            default:
                return status_error;
        }
    }


    int os::open(const char *path, int mode, int flags, int &out_fd) {
        const int fd = ::open(path, mode, flags);
        if (fd < 0)
            return errno_to_status_code(errno);
        out_fd = fd;
        return status_ok;
    }

    int os::access(const char *path, int mode) {
        if (::access(path, mode))
            return status_error;
        return status_ok;
    }



    int os::close(int fd) {
        if (::close(fd) < 0)
            return errno_to_status_code(errno);
        return status_ok;
    }

    int os::write(int fd, int64 offset, const void *buf, uint64 count) {
        auto result = lseek(fd, offset, SEEK_SET);
        int error;
        if (result == -1)
            return errno_to_status_code(errno);
        do {
            result = ::write(fd, buf, count);
            error = errno;
        }while (result == -1 && error == EINTR);
        if (result == -1)
            return errno_to_status_code(error);
        return status_ok;
    }

    int os::write(int fd, const void *buf, uint64 count) {
        int error;
        int64 result;
        do {
            result = ::write(fd, buf, count);
            error = errno;
        }while (result == -1 && error == EINTR);
        if (result == -1)
            return errno_to_status_code(error);
        return status_ok;
    }
    int os::read(int fd, int64 offset, void *buf, uint64 count) {
    auto result = lseek(fd, offset, SEEK_SET);
        if (result == -1)
            return errno_to_status_code(errno);
        do {
            result = ::read(fd, buf, count);
        }while (result == -1 && errno == EINTR);
        if (result == -1)
            return errno_to_status_code(errno);
        return status_ok;


    }

    int os::fallocate(int fd, int64 offset, int64 length) {
        int error;
        do {
            error = posix_fallocate(fd, offset, length);
        } while (error == EINTR);
        return errno_to_status_code(error);
    }
    int os::fdatasync(int fd) {
        int status,error = 0;
        do {
            status = ::fdatasync(fd);
            error = errno;
        }while (status < 0 && error == EINTR);
        if (status < 0)
            return errno_to_status_code(error);
        return status_ok;
    }
    int os::unlink(const char *path) {
        auto status = ::unlink(path);
        if (status < 0)
            return errno_to_status_code(errno);
        return status_ok;
    }
    int os::get_file_size(int fd, uint64 &out_size) {
        assert(fd > 0);
        struct stat st{};
        if (::fstat(fd, &st) < 0)
            return errno_to_status_code(errno);
        out_size = st.st_size;
        return status_ok;
    }

}
