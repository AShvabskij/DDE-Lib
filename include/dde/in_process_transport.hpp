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
class InProcessChannel {
public:
    struct Message {
        MsgHeader hdr;
        std::vector<std::byte> payload;
    };

    // Очереди: 0 = UpToDown, 1 = DownToUp
    std::queue<Message> queues[2];
    std::mutex mtx;
    std::condition_variable cv[2];
    bool closed = false;
};

class InProcessTransport : public ITransport {
public:
    // dir: 0 (UpToDown) или 1 (DownToUp). 
    // channel: общий указатель на канал связи.
    explicit InProcessTransport(std::shared_ptr<InProcessChannel> channel, TransportDir dir);

    Status open(const std::string& ) override { return Status::Ok; } // Заглушка
    void close() override;

    Status send(const MsgHeader& hdr, const void* payload, size_t payload_len) override;
    Status recv(MsgHeader& hdr, void* buf, size_t cap, size_t& out_len,
                std::chrono::milliseconds timeout) override;

    bool is_connected() const override { return !m_channel->closed; }

private:
    std::shared_ptr<InProcessChannel> m_channel;
    int m_tx_idx; // индекс очереди для отправки (0 или 1)
    int m_rx_idx; // индекс очереди для приема (1 или 0)
};

} // namespace dde