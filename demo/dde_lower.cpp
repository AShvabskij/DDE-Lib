#include <unistd.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include "dde/data_exchange_service.hpp"
#include "dde/shm_transport.hpp"
#include "types.hpp"

using namespace dde;
using namespace std::chrono_literals;

int main() {
    auto transport = std::make_unique<ShmTransport>(Level::Lower, true);
    DataExchangeService<Parameter, Command> service(std::move(transport));

    if (service.init("/dde_demo") != Status::Ok) {
        std::cerr << "Failed to init shm\n";
        return 1;
    }
    
    std::cout << "[Lower] Started. PID=" << getpid() << "\n";
    std::cout << "[Lower] Simulating " << DEMO_MODULES_COUNT << " modules...\n";

    float base_value = 0.0f;
    int tick = 0;

    while (true) {
        // 1. Симулируем опрос модулей (собираем данные в пакет)
        for (uint16_t mod = 0; mod < DEMO_MODULES_COUNT; ++mod) {
            Parameter batch[DEMO_PARAMS_PER_MODULE];
            for (uint16_t p = 0; p < DEMO_PARAMS_PER_MODULE; ++p) {
                batch[p] = {
                    DEMO_DEVICE_ID, 
                    mod, 
                    p, 
                    base_value + mod * 100 + p, 
                    std::chrono::system_clock::now().time_since_epoch().count()
                };
            }
            
            // Отправляем весь пакет (батч) модуля за один вызов
            Status s = service.write_data(batch, DEMO_PARAMS_PER_MODULE);
            if (s == Status::Ok) {
                std::cout << "[Lower] Sent batch of " << DEMO_PARAMS_PER_MODULE 
                          << " params. Mod=" << mod 
                          << " First val=" << batch[0].value << "\n";
            } else if (s == Status::Busy) {
                std::cerr << "[Lower] Failed to send data, the buffer is busy\n";
            }
            else {
                std::cerr << "[Lower] Failed to send data\n";
            }
        }
        base_value += 1.5f;

        // 2. Читаем команды от верхнего уровня

        constexpr size_t max_cmd_packet = Max_Msg_Payload / sizeof(Command) + 1;
        Command cmd_batch[max_cmd_packet];
        size_t cmd_count = 0;
        
        // Крутимся в цикле до тех пор, пока есть данные. 
        // Как только буфер опустеет, recv_cmd вернет Timeout, и мы выйдем.
        while (true) {
            Status rs = service.recv_cmd(cmd_batch, max_cmd_packet, cmd_count, 0ms);
            
            if (rs == Status::Timeout) {
                break; // Данных больше нет, выходим
            }
            
            if (rs == Status::Ok) {
                // Обрабатываем полученную пачку
                for (size_t i = 0; i < cmd_count; ++i) {
                    const auto& cmd = cmd_batch[i];
                    if (cmd.nRW == 0) {
                        std::cout << "[Lower] READ CMD: Mod=" << cmd.module_id << " param=" << cmd.param_id << "\n";
                    } else {
                        std::cout << "[Lower] WRITE CMD: Mod=" << cmd.module_id<< " param=" << cmd.param_id << "\n"
                                  << " value=" << cmd.value << "\n";
                    }
                }
            } 
            else if (rs == Status::Error) {
                // Ошибка: либо пакет битый, либо (теоретически) больше max_cmd_packet. 
                std::cerr << "[Lower] Error unpacking command batch! Overflow or corruption.\n";
                break; // Чтобы не зациклиться навсегда на одном и том же битом пакете
            }
        }

        // Опрос устройств раз в 100 мс
        std::this_thread::sleep_for(100ms);
        tick++;
    }

    return 0;
}