#pragma once
#include "dde/itransport.hpp"
#include "dde/serialization.hpp"
#include <deque>
#include <mutex>
#include <atomic>
#include <memory>
#include <vector>

namespace dde {

template<typename TData, typename TCommand,
         typename DataSer = PodSerializer<TData>,
         typename CmdSer  = PodSerializer<TCommand>>
class DataExchangeService {
public:
    explicit DataExchangeService(std::unique_ptr<ITransport> t);
    
    Status init(const std::string& endpoint);
    void   stop();

    // --- Поток данных ---
    Status write_data(const TData* arr, size_t count);
    Status poll_data(TData* out_arr, size_t max_count, size_t& out_count); // забрать пакет без ожидания
    Status read_data(TData* out_arr, size_t max_count, size_t& out_count, std::chrono::milliseconds timeout); // забрать пакет с ожиданием

    // --- Поток команд ---
    Status send_cmd(const TCommand& c);
    Status recv_cmd(TCommand* out_arr, size_t max_count, size_t& out_count, std::chrono::milliseconds timeout);

    // --- Служебные ---
    Status sync_flush(std::chrono::milliseconds timeout);
    void   async_flush();

private:
    Status try_send_batch(const std::vector<TCommand>& batch);

    std::unique_ptr<ITransport> m_transport;
    std::atomic<bool> m_running{false};
    
    // Очередь команд
    std::mutex m_mtx;
    std::deque<TCommand> m_outbox;
};

} // namespace dde

// Подключаем реализацию шаблона
#include "dde/data_exchange_service.ipp" 