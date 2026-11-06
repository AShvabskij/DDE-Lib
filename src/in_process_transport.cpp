#include "dde/in_process_transport.hpp"

namespace dde {

InProcessTransport::InProcessTransport(std::shared_ptr<InProcessChannel> channel, Level level)
    : m_channel(std::move(channel)) 
{
    m_level = level;
}

void InProcessTransport::close() {
    std::lock_guard<std::mutex> lock(m_channel->mtx);
    m_channel->closed = true;
    m_channel->up_down_cv.notify_all();
    m_channel->down_up_cv.notify_all();
}

Status InProcessTransport::send(const MsgHeader& hdr, const void* payload, size_t payload_len) {
    if (m_channel->closed) return Status::Disconnected;

    auto& tx_queue = m_level == Level::Upper ? m_channel->up_down_queue : m_channel->down_up_queue;
    auto& cv = m_level == Level::Upper ? m_channel->up_down_cv : m_channel->down_up_cv;

    InProcessChannel::Message msg;
    msg.hdr = hdr;
    msg.payload.resize(payload_len);
    if (payload_len > 0 && payload != nullptr) {
        std::memcpy(msg.payload.data(), payload, payload_len);
    }

    {
        std::lock_guard<std::mutex> lock(m_channel->mtx);
        
        tx_queue.push(std::move(msg));
        if (tx_queue.size() > Max_Msg_Count) {
            cv.notify_one();
            return Status::Overflow;
        }
    }
    
    cv.notify_one();
    return Status::Ok;
}

Status InProcessTransport::recv(MsgHeader& hdr, void* buf, size_t cap, size_t& out_len,
                               std::chrono::milliseconds timeout) 
{
    std::unique_lock<std::mutex> lock(m_channel->mtx);

    auto& rx_queue = (m_level == Level::Upper) ? m_channel->down_up_queue : m_channel->up_down_queue;
    auto& cv = (m_level == Level::Upper) ? m_channel->down_up_cv : m_channel->up_down_cv;

    auto start = std::chrono::steady_clock::now();

    while (rx_queue.empty()) {
        if (m_channel->closed) return Status::Disconnected;

        auto now = std::chrono::steady_clock::now();
        if (now - start >= timeout) {
            return Status::Timeout;
        }

        cv.wait_for(lock, timeout - (now - start));
    }

    auto& msg = rx_queue.front();
    
    if (msg.payload.size() > cap) {
        rx_queue.pop(); // // Выкидываем слишком большое сообщение
        return Status::Error; // Буфер получателя слишком мал
    }

    hdr = msg.hdr;
    out_len = msg.payload.size();
    if (out_len > 0 && buf != nullptr) {
        std::memcpy(buf, msg.payload.data(), out_len);
    }

    rx_queue.pop();
    return Status::Ok;
}

} // namespace dde