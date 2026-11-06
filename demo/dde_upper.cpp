#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include "dde/data_exchange_service.hpp"
#include "dde/shm_transport.hpp"
#include "types.hpp"

using namespace dde;
using namespace std::chrono_literals;

int main() {
    auto transport = std::make_unique<ShmTransport>(Level::Upper, false);
    DataExchangeService<Parameter, Command> service(std::move(transport));

    if (service.init("/dde_demo") != Status::Ok) {
        std::cerr << "Failed to connect to shm. Is Lower running?\n";
        return 1;
    }

    std::cout << "[Upper] Connected.\n";

    // Фоновый поток для отправки очереди команд
    std::atomic<bool> running{true};
    std::thread flush_thread( [&service, &running]() {
        while (running.load()) {

        for (uint16_t mod = 0; mod < DEMO_MODULES_COUNT; ++mod) {
            for (uint16_t p = 0; p < DEMO_PARAMS_PER_MODULE; ++p) {
                Command cmd{
                    DEMO_DEVICE_ID, 
                    mod, 
                    p, 
                    0, 
                    0 // nRW = 0 (read)
                };
                service.send_cmd(cmd);
            }
        }
                    
        service.async_flush();
        std::this_thread::sleep_for(50ms); // Тик каждые 50 мс
        }
        
    });

    int tick = 0;
    while (running.load()) {
        // 1. Читаем данные (пакеты)
        constexpr size_t max_data_packet = Max_Msg_Payload / sizeof(Parameter) + 1;
        Parameter batch[max_data_packet];
        size_t count = 0;
        
        // Вычитываем всё, что накопилось в буфере
        while (true) {
            Status rs = service.poll_data(batch, max_data_packet, count);
            if (rs == Status::Timeout) break; // poll_data использует таймаут 0, вернет Timeout если пусто

            std::cout << "[Upper] Got batch of " << count << " params. First val: " << batch[0].value << "\n";
            
            // Формируем read-команды для каждого полученного параметра
            for (size_t i = 0; i < count; ++i) {
                Command cmd{
                    batch[i].device_id, 
                    batch[i].module_id, 
                    batch[i].param_id, 
                    0, 
                    0 // nRW = 0 (read)
                };
                service.send_cmd(cmd);
            }
        }

        // 2. Раз в секунду шлем тестовую команду на запись
        if (tick % 10 == 0) {
            Command write_cmd{DEMO_DEVICE_ID, 0, 0, 999, 1}; // write 999 to param 0
            service.send_cmd(write_cmd);
            std::cout << "[Upper] Sent WRITE CMD to Lower.\n";
        }

        std::this_thread::sleep_for(100ms);
        tick++;
    }

    running.store(false);
    flush_thread.join();

    return 0;
}