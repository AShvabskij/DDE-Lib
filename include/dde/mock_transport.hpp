#pragma once
#include "dde/itransport.hpp"
#include <deque>
#include <mutex>
#include <vector>
#include <cstring>

namespace dde {

class MockTransport : public ITransport {
public:
    struct Message {
        MsgHeader hdr;
        std::vector<std::byte> payload;
    };

    Status open(const std::string& endpoint) override {
        m_endpoint = endpoint;
        m_connected = true;
        return Status::Ok;
    }

    void close() override {
        m_connected = false;
    }

    Status send(const MsgHeader& hdr, const void* payload, size_t payload_len) override {
        if (!m_connected) return Status::Disconnected;
        if (payload_len > Max_Msg_Payload) return Status::Error;

        std::lock_guard<std::mutex> lock(m_mtx);
        
        // Сохраняем сообщение в историю отправленных
        std::vector<std::byte> data(payload_len);
        if (payload_len > 0 && payload != nullptr) {
            std::memcpy(data.data(), payload, payload_len);
        }
        m_sent.push_back({hdr, data});
        
        return Status::Ok;
    }

    Status recv(MsgHeader& hdr, void* buf, size_t cap, size_t& out_len,
                std::chrono::milliseconds timeout) override {
        if (!m_connected) return Status::Disconnected;

        std::unique_lock<std::mutex> lock(m_mtx);
        
        // Ждём сообщения до таймаута
        bool ready = m_cv.wait_for(lock, timeout, [this] { return !m_received.empty(); });
        
        if (!ready) return Status::Timeout;

        // Забираем сообщение
        auto msg = std::move(m_received.front());
        m_received.pop_front();

        if (msg.payload.size() > cap) return Status::Error;

        hdr = msg.hdr;
        out_len = msg.payload.size();
        if (out_len > 0 && buf != nullptr) {
            std::memcpy(buf, msg.payload.data(), out_len);
        }

        return Status::Ok;
    }

    bool is_connected() const override { return m_connected; }

    // Только для тестов. Эмулировать получение сообщения от "удаленной стороны"
    void inject_recv(const MsgHeader& hdr, void* payload, size_t payload_len)
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        std::vector<std::byte> data(payload_len);

        if (payload != nullptr && payload_len > 0) {
            std::memcpy(data.data(), payload, payload_len);
        };

        m_received.push_back({hdr, data});
    }

    // Получить историю отправленных сообщений
    auto get_sent_copy() {
        std::lock_guard<std::mutex> lock(m_mtx);
        return m_sent;
    }
    
    void clear_sent() {
        std::lock_guard<std::mutex> lock(m_mtx);
        m_sent.clear();
    }

private:
    std::string m_endpoint;
    bool m_connected = false;

    std::mutex m_mtx;
    std::condition_variable m_cv;
    
    std::deque<Message> m_received; // входящие (эмулируем)
    std::deque<Message> m_sent;     // исходящие (записываем для проверки)
};

} // namespace dde