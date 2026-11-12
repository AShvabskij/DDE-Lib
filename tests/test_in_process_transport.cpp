#include <gtest/gtest.h>
#include "dde/data_exchange_service.hpp"
#include "dde/in_process_transport.hpp"
#include <thread>
#include <chrono>

using namespace dde;
using namespace std::chrono_literals;

// Реальные структуры проекта
struct Parameter {
    uint16_t device_id, module_id, param_id;
    float value;
    int64_t timestamp_ns;

    bool operator==(const Parameter& other) const {
        return device_id == other.device_id && module_id == other.module_id 
            && param_id == other.param_id && value == other.value 
            && timestamp_ns == other.timestamp_ns;
    }
};

struct Command {
    uint16_t device_id, module_id, param_id;
    uint32_t value;
    uint8_t nRW;
};

// Алиас для нашего сервиса
using TestService = DataExchangeService<Parameter, Command>;

TEST(InMemoryTransportTest, ExchangeInOneProcess) {
    auto channel = std::make_shared<InProcessChannel>();

    // Верхний уровень
    auto upper_transport = std::make_unique<InProcessTransport>(channel, Level::Upper);
    TestService upper_service(std::move(upper_transport));
    ASSERT_EQ(upper_service.init("test"), Status::Ok);

    // Нижний уровень
    auto lower_transport = std::make_unique<InProcessTransport>(channel, Level::Lower);
    TestService lower_service(std::move(lower_transport));
    ASSERT_EQ(lower_service.init("test"), Status::Ok);

    // 1. Тестируем поток данных (Нижний -> Верхний)
    // Нижний отправил пакет из 2 параметров
    Parameter send_data[2] = {
        {1, 1, 1, 3.14f, 1000},
        {1, 1, 2, 2.71f, 2000}
    };
    ASSERT_EQ(lower_service.write_data(send_data, 2), Status::Ok);

    // Верхний читает
    Parameter recv_data[5];
    size_t data_count = 0;
    ASSERT_EQ(upper_service.poll_data(recv_data, 5, data_count), Status::Ok);
    
    ASSERT_EQ(data_count, 2);
    EXPECT_EQ(recv_data[0], send_data[0]);
    EXPECT_EQ(recv_data[1], send_data[1]);

    // 2. Тестируем поток команд (Верхний -> Нижний)
    ASSERT_EQ(upper_service.send_cmd({1, 0, 5, 100, 1}), Status::Ok); // write
    ASSERT_EQ(upper_service.send_cmd({1, 0, 6, 200, 1}), Status::Ok); // write
    ASSERT_EQ(upper_service.async_flush(), Status::Ok); // Отправляем пакет

    Command recv_cmd[5];
    size_t cmd_count = 0;
    ASSERT_EQ(lower_service.recv_cmd(recv_cmd, 5, cmd_count, 10ms), Status::Ok);
    
    ASSERT_EQ(cmd_count, 2);
    EXPECT_EQ(recv_cmd[0].param_id, 5);
    EXPECT_EQ(recv_cmd[0].value, 100);
    EXPECT_EQ(recv_cmd[1].param_id, 6);
    EXPECT_EQ(recv_cmd[1].value, 200);
}

TEST(InMemoryTransportTest, MultiThreadedExchange) {
    auto channel = std::make_shared<InProcessChannel>();

    auto upper_transport = std::make_unique<InProcessTransport>(channel, Level::Upper);
    TestService upper_service(std::move(upper_transport));
    ASSERT_EQ(upper_service.init("test"), Status::Ok);

    auto lower_transport = std::make_unique<InProcessTransport>(channel, Level::Lower);
    TestService lower_service(std::move(lower_transport));
    ASSERT_EQ(lower_service.init("test"), Status::Ok);

    std::atomic<int> received_param_count{0};
    std::atomic<int> received_cmd_count{0};    
    std::atomic<bool> running{true};

    // Поток Верхнего уровня
    std::thread upper_thread([&]() {
        while (running.load()) {
            // Читаем параметры
            Parameter batch[10];
            size_t count = 0;
            if (upper_service.poll_data(batch, 10, count) == Status::Ok) {
                received_param_count += count;
            }

            // Раз в цикл шлем read-команду
            upper_service.send_cmd({1, 2, 1, 7, 1});
            upper_service.async_flush();
            
            std::this_thread::sleep_for(1ms);
        }
    });

    // Поток Нижнего уровня
    int sent_count = 0;
    std::thread lower_thread([&]() {
        while (running.load()) {
            // Отправляем пачку из 8 параметров (симуляция опроса модуля)
            Parameter batch[8];
            for (int i = 0; i < 8; ++i) {
                batch[i] = {1, 0, (uint16_t)i, static_cast<float>(sent_count++), 0};
            }
            lower_service.write_data(batch, 8);

            // Читаем команды
            Command cmds[10];
            size_t cmd_count = 0;
            while (lower_service.recv_cmd(cmds, 10, cmd_count, 1ms) == Status::Ok) {
                for (size_t i = 0; i < cmd_count; ++i) {
                    // Проверяем, что команда именно та, которую мы ждем
                    EXPECT_EQ(cmds[i].device_id, 1);
                    EXPECT_EQ(cmds[i].module_id, 2);
                    EXPECT_EQ(cmds[i].param_id, 1);
                    EXPECT_EQ(cmds[i].value, 7);
                    EXPECT_EQ(cmds[i].nRW, 1); // write cmd
                }
                received_cmd_count += cmd_count;
            }

            std::this_thread::sleep_for(1ms);
        }
    });

    std::this_thread::sleep_for(100ms); // Работаем 100 мс
    running.store(false);

    upper_thread.join();
    lower_thread.join();

    // Проверяем, что обмен данными реально шел
    EXPECT_GT(received_param_count.load(), 0);
}