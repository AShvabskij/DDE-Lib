#pragma once
#include <string>
#include <chrono>
#include <cstddef>
#include "dde/types.hpp"

namespace dde {

class ITransport {
public:
    virtual ~ITransport() = default;

    // Открыть транспорт. endpoint — имя shm-сегмента или "host:port".
    virtual Status open(const std::string& endpoint) = 0;

    // Закрыть транспорт и освободить ресурсы.
    virtual void close() = 0;

    // Отправить один фрейм: [MsgHeader][payload].
    // Возвращает Busy, если буфер полон; Timeout — если не удалось за отведённое время.
    virtual Status send(const MsgHeader& hdr,
                        const void* payload, size_t payload_len) = 0;

    // Принять один фрейм. payload пишется в buf (не больше cap).
    // Блокирует до timeout. Timeout — если данных нет.
    virtual Status recv(MsgHeader& hdr,
                        void* buf, size_t cap, size_t& out_len,
                        std::chrono::milliseconds timeout) = 0;

    virtual bool is_connected() const = 0;
};

} // namespace dde