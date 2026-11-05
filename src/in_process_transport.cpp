#include "dde/in_process_transport.hpp"

namespace dde {

InProcessTransport::InProcessTransport(std::shared_ptr<InProcessChannel> channel, TransportDir dir)
    : m_channel(std::move(channel)) 
{
    // Если мы Верхний (dir=0), то пишем в 0, читаем из 1.
    // Если мы Нижний (dir=1), то пишем в 1, читаем из 0.
    m_tx_idx = (dir == TransportDir::Up) ? 0 : 1; 
    m_rx_idx = (dir == TransportDir::Down) ? 0 : 1;
}

void InProcessTransport::close() {
    std::lock_guard<std::mutex> lock(m_channel->mtx);
    m_channel->closed = true;
    m_channel->cv[0].notify_all();
    m_channel->cv[1].notify_all();
}

Status InProcessTransport::send(const MsgHeader& hdr, const void* payload, size_t payload_len) {
    if (m_channel->closed) return Status::Disconnected;

    InProcessChannel::Message msg;
    msg.hdr = hdr;
    msg.payload.resize(payload_len);
    if (payload_len > 0 && payload != nullptr) {
        std::memcpy(msg.payload.data(), payload, payload_len);
    }

    {
        std::lock_guard<std::mutex> lock(m_channel->mtx);
        // В памяти мы не ограничены размером слота, поэтому всегда Ok
        m_channel->queues[m_tx_idx].push(std::move(msg));
    }
    
    m_channel->cv[m_tx_idx].notify_one();
    return Status::Ok;
}

Status InProcessTransport::recv(MsgHeader& hdr, void* buf, size_t cap, size_t& out_len,
                               std::chrono::milliseconds timeout) 
{
    std::unique_lock<std::mutex> lock(m_channel->mtx);

    auto start = std::chrono::steady_clock::now();

    while (m_channel->queues[m_rx_idx].empty()) {
        if (m_channel->closed) return Status::Disconnected;

        auto now = std::chrono::steady_clock::now();
        if (now - start >= timeout) {
            return Status::Timeout;
        }

        m_channel->cv[m_rx_idx].wait_for(lock, timeout - (now - start));
    }

    auto& msg = m_channel->queues[m_rx_idx].front();
    
    if (msg.payload.size() > cap) {
        // Буфер получателя слишком мал. Выкидываем сообщение, чтобы не застрять.
        m_channel->queues[m_rx_idx].pop();
        return Status::Error;
    }

    hdr = msg.hdr;
    out_len = msg.payload.size();
    if (out_len > 0 && buf != nullptr) {
        std::memcpy(buf, msg.payload.data(), out_len);
    }

    m_channel->queues[m_rx_idx].pop();
    return Status::Ok;
}

} // namespace dde