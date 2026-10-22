#pragma once
#include <thread>

namespace dde {

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
DataExchangeService<TData, TCommand, DataSer, CmdSer>::DataExchangeService(
    std::unique_ptr<ITransport> t)
    : m_transport(std::move(t)) {}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::init(const std::string& endpoint) {
    Status s = m_transport->open(endpoint);
    if (s == Status::Ok) {
        m_running.store(true);
    }
    return s;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
void DataExchangeService<TData, TCommand, DataSer, CmdSer>::stop() {
    m_running.store(false);
    m_transport->close();
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::write_data(const TData* arr, size_t count) {
    if (!m_running.load()) return Status::Disconnected;
    if (count == 0 || arr == nullptr) return Status::Error;

    auto payload = DataSer::pack(arr, count);
    MsgHeader hdr{MsgKind::DataPush, static_cast<uint16_t>(count), static_cast<uint32_t>(payload.size())};
    
    return m_transport->send(hdr, payload.data(), payload.size());
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::read_data(
    TData* out_arr, size_t max_count, size_t& out_count, std::chrono::milliseconds timeout) 
{
    if (!m_running.load()) return Status::Disconnected;

    MsgHeader hdr;
    uint8_t buf[Max_Msg_Payload];
    size_t out_len = 0;

    Status s = m_transport->recv(hdr, buf, sizeof(buf), out_len, timeout);
    if (s != Status::Ok) return s;

    if (hdr.kind != MsgKind::DataPush) return Status::Error;

    // Распаковываем массив
    if (!DataSer::unpack(buf, out_len, out_arr, max_count, out_count)) {
        return Status::Error;
    }
    
    if (out_count != hdr.count) return Status::Error; // Data Integrity check!

    return Status::Ok;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::poll_data(TData* out_arr, size_t max_count, size_t& out_count) {
    return read_data(out_arr, max_count, out_count, std::chrono::milliseconds(0));
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::send_cmd(const TCommand& c) {
    if (!m_running.load()) return Status::Disconnected;

    std::lock_guard<std::mutex> lock(m_mtx);
    m_outbox.push_back(c);
    return Status::Ok;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::recv_cmd(TCommand* out_arr, size_t max_count, size_t& out_count, std::chrono::milliseconds timeout) {
    if (!m_running.load()) return Status::Disconnected;

    MsgHeader hdr;
    uint8_t buf[Max_Msg_Payload];
    size_t out_len = 0;

    Status s = m_transport->recv(hdr, buf, sizeof(buf), out_len, timeout);
    if (s != Status::Ok) return s;

    if (hdr.kind != MsgKind::CommandBatch) return Status::Error;

    size_t n = 0;
    if (!CmdSer::unpack(buf, out_len, out_arr, max_count, out_count)) return Status::Error;

    return Status::Ok;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
void DataExchangeService<TData, TCommand, DataSer, CmdSer>::async_flush() {
    if (!m_running.load()) return;

    std::vector<TCommand> batch;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_outbox.empty()) return;
        
        // Перекладываем всё из очереди в локальный вектор
        batch.assign(m_outbox.begin(), m_outbox.end());
        m_outbox.clear();
    }

    Status s = try_send_batch(batch);
    
    if (s != Status::Ok) {
        // Если отправить не удалось — возвращаем команды обратно в очередь
        std::lock_guard<std::mutex> lock(m_mtx);
        m_outbox.insert(m_outbox.begin(), batch.begin(), batch.end());
    }
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::sync_flush(std::chrono::milliseconds timeout) {
    if (!m_running.load()) return Status::Disconnected;

    auto start = std::chrono::steady_clock::now();
    
    while (true) {
        std::vector<TCommand> batch;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_outbox.empty()) return Status::Ok;
            
            batch.assign(m_outbox.begin(), m_outbox.end());
            m_outbox.clear();
        }

        Status s = try_send_batch(batch);
        
        if (s == Status::Ok) continue; // отправлено, возможно в очереди есть еще

        // Ошибка при отправке
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_outbox.insert(m_outbox.begin(), batch.begin(), batch.end());
        }

        auto now = std::chrono::steady_clock::now();
        if (now - start >= timeout) {
            return Status::Timeout;
        }
        
        // Ждем перед следующей попыткой (backoff)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::try_send_batch(const std::vector<TCommand>& batch) {
    if (batch.empty()) return Status::Ok;

    auto payload = CmdSer::pack(batch.data(), batch.size());
    MsgHeader hdr{MsgKind::CommandBatch, static_cast<uint16_t>(batch.size()), static_cast<uint32_t>(payload.size())};
    
    return m_transport->send(hdr, payload.data(), payload.size());
}

} // namespace dde