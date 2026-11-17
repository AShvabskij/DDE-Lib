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

    if (!m_running.exchange(false)) {
        return;
    };

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

    {
        std::lock_guard<std::mutex> lock(m_pebding_data_mtx);
        if (!m_pending_data.empty()) {
            out_count = std::min(m_pending_data.size(), max_count);
            std::copy(m_pending_data.begin(), m_pending_data.begin() + out_count, out_arr);
            m_pending_data.erase(m_pending_data.begin(), m_pending_data.begin() + out_count);

            if (!m_pending_data.empty()) {
                return Status::Partial;
            }
            return Status::Ok;
        }
    }

    MsgHeader hdr;
    uint8_t buf[Max_Msg_Payload];
    size_t out_len = 0;

    Status s = m_transport->recv(hdr, buf, sizeof(buf), out_len, timeout);

    if (s != Status::Ok) return s;

    if (hdr.kind != MsgKind::DataPush) return Status::Error;

    std::vector<TData> full_batch(hdr.count);
    size_t actual_count = 0;

    if (!DataSer::unpack(buf, out_len, full_batch.data(), full_batch.size(), actual_count)) {
        return Status::Error;
    }
    
    if (actual_count != hdr.count) return Status::Error; // Data Integrity check!

    out_count = std::min(actual_count, max_count);
    std::copy(full_batch.begin(), full_batch.begin() + out_count, out_arr);

    if (actual_count > out_count) { // не всё влезло
        std::lock_guard<std::mutex> lock(m_pebding_data_mtx);
        if (m_pending_data.size() +  (actual_count - out_count) > Max_Pending_Data) {
            return Status::Overflow;
        }
        m_pending_data.insert(m_pending_data.end(), full_batch.begin() + out_count, full_batch.begin() + actual_count);
        return Status::Partial;
    }

    return Status::Ok;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::poll_data(TData* out_arr, size_t max_count, size_t& out_count) {
    return read_data(out_arr, max_count, out_count, std::chrono::milliseconds(0));
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::send_cmd(const TCommand& c) {
    if (!m_running.load()) return Status::Disconnected;

    std::lock_guard<std::mutex> lock(m_outbox_cmd_mtx);
    m_outbox_cmd.push_back(c);
    return Status::Ok;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::recv_cmd(TCommand* out_arr, size_t max_count, size_t& out_count, std::chrono::milliseconds timeout) {
    if (!m_running.load()) return Status::Disconnected;

    {
        std::lock_guard<std::mutex> lock(m_pending_cmds_mtx);
        if (!m_pending_cmds.empty()) {
            out_count = std::min(m_pending_cmds.size(), max_count);
            std::copy(m_pending_cmds.begin(), m_pending_cmds.begin() + out_count, out_arr);
            m_pending_cmds.erase(m_pending_cmds.begin(), m_pending_cmds.begin() + out_count);

            if (!m_pending_cmds.empty()) {
                return Status::Partial;
            }
            return Status::Ok;
        }
    }

    MsgHeader hdr;
    uint8_t buf[Max_Msg_Payload];
    size_t out_len = 0;

    Status res = m_transport->recv(hdr, buf, sizeof(buf), out_len, timeout);
    if (res != Status::Ok) return res;

    if (hdr.kind != MsgKind::CommandBatch) return Status::Error;

    std::vector<TCommand> full_batch(hdr.count);
    size_t actual_count = 0;

    if (!CmdSer::unpack(buf, out_len, full_batch.data(), full_batch.size(), actual_count)) {
        return Status::Error;
    }

    out_count = std::min(actual_count, max_count);
    std::copy(full_batch.begin(), full_batch.begin() + out_count, out_arr);

    if (actual_count > out_count) { // не всё влезло
        std::lock_guard<std::mutex> lock(m_pending_cmds_mtx);

        if (m_pending_cmds.size() +  (actual_count - out_count) > Max_Pending_Cmds) {
            return Status::Overflow;
        }

        m_pending_cmds.insert(m_pending_cmds.end(), full_batch.begin() + out_count, full_batch.begin() + actual_count);
        return Status::Partial;
    }

    return Status::Ok;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::async_flush() {
    if (!m_running.load()) return Status::Error;

    std::vector<TCommand> batch;
    size_t max_count = CmdSer::max_count(Max_Msg_Payload);
    size_t count_to_send = 0;

    {
        std::lock_guard<std::mutex> lock(m_outbox_cmd_mtx);
        if (m_outbox_cmd.empty()) return Status::Ok;

        count_to_send = std::min(m_outbox_cmd.size(), max_count);
        batch.assign(m_outbox_cmd.begin(), m_outbox_cmd.begin() + count_to_send);
    }

    Status res = try_send_batch(batch);
    
    if (res == Status::Ok) {
        std::lock_guard<std::mutex> lock(m_outbox_cmd_mtx);
        m_outbox_cmd.erase(m_outbox_cmd.begin(), std::next(m_outbox_cmd.begin(), count_to_send));
    } 

    return res;
}

template<typename TData, typename TCommand, typename DataSer, typename CmdSer>
Status DataExchangeService<TData, TCommand, DataSer, CmdSer>::sync_flush(std::chrono::milliseconds timeout) {
    if (!m_running.load()) return Status::Disconnected;

    auto start = std::chrono::steady_clock::now();

    size_t max_count = CmdSer::max_count(Max_Msg_Payload);;
    size_t count_to_send = 0;

    while (true) {

        std::vector<TCommand> batch;
        {
            std::lock_guard<std::mutex> lock(m_outbox_cmd_mtx);
            if (m_outbox_cmd.empty()) return Status::Ok;

            count_to_send = std::min(m_outbox_cmd.size(), max_count);
            batch.assign(m_outbox_cmd.begin(), m_outbox_cmd.begin() + count_to_send);
        }

        Status res = try_send_batch(batch);

        if (res == Status::Ok) {
                std::lock_guard<std::mutex> lock(m_outbox_cmd_mtx);
                m_outbox_cmd.erase(m_outbox_cmd.begin(), std::next(m_outbox_cmd.begin(), count_to_send)); // убираем отправленные команды из очереди
                continue; // возможно в очереди есть еще, пробуем сразу
        } else if (res == Status::Busy) {
            auto now = std::chrono::steady_clock::now();
            if (now - start >= timeout) {
                return Status::Timeout;
            }
            
            // Ждем перед следующей попыткой (backoff)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        } else {
            return res; // return error status
        }
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