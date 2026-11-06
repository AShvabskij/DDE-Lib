#pragma once
#include "dde/itransport.hpp"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <vector>
#include <cstring>

namespace dde {

// Канал связи, который разделяют два транспорта
struct InProcessChannel {
public:
    struct Message {
        MsgHeader hdr;
        std::vector<std::byte> payload;
    };

    // Очереди: 0 = UpToDown, 1 = DownToUp
    std::queue<Message> up_down_queue;
    std::queue<Message> down_up_queue;

    std::mutex mtx;
    std::condition_variable up_down_cv;
    std::condition_variable down_up_cv;

    bool closed = false;
};

class InProcessTransport : public ITransport {
public:
    enum class Level {
        Upper = 0, // Верхний уровень, потребитель данных 
        Lower  // Нижний уровень, отправитель данных
    };

    // channel: общий указатель на канал связи.
    explicit InProcessTransport(std::shared_ptr<InProcessChannel> channel, Level dir);

    Status open(const std::string& ) override { return Status::Ok; } // Заглушка
    void close() override;

    Status send(const MsgHeader& hdr, const void* payload, size_t payload_len) override;
    Status recv(MsgHeader& hdr, void* buf, size_t cap, size_t& out_len,
                std::chrono::milliseconds timeout) override;

    bool is_connected() const override { return !m_channel->closed; }

private:
    std::shared_ptr<InProcessChannel> m_channel;
    Level m_level;

};

} // namespace dde