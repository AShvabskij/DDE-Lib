#include <gtest/gtest.h>
#include "dde/data_exchange_service.hpp"
#include "dde/mock_transport.hpp"
#include <thread>

using namespace dde;
using namespace std::chrono_literals;

// Реальные структуры проекта (из README)
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

    bool operator==(const Command& other) const {
        return device_id == other.device_id && module_id == other.module_id 
            && param_id == other.param_id && value == other.value 
            && nRW == other.nRW;
    }
};

// Создаем алиас для нашего сервиса
using TestService = DataExchangeService<Parameter, Command>;

TEST(DataExchangeServiceTest, WriteAndReadDataBatch) {
    auto mock = std::make_unique<MockTransport>();
    auto* mock_ptr = mock.get();
    
    TestService service(std::move(mock));
    ASSERT_EQ(service.init("test"), Status::Ok);

    // Нижний уровень отправляет пакет из 2 параметров
    Parameter batch[2] = {
        {1, 2, 3, 42.5f, 1000},
        {1, 2, 4, 43.5f, 2000}
    };
    ASSERT_EQ(service.write_data(batch, 2), Status::Ok);

    // Проверяем, что в транспорте лежит 1 сообщение (пакет)
    auto sent = mock_ptr->get_sent_copy();
    ASSERT_EQ(sent.size(), 1);
    EXPECT_EQ(sent[0].hdr.kind, MsgKind::DataPush);
    EXPECT_EQ(sent[0].hdr.count, 2);

    // Эмулируем получение этого пакета Верхним уровнем
    mock_ptr->inject_recv(sent[0].hdr, sent[0].payload.data(), sent[0].payload.size());

    // Верхний уровень читает
    Parameter out_batch[3]; // выделяем с запасом
    size_t out_count = 0;
    Status s = service.poll_data(out_batch, 3, out_count);
    
    ASSERT_EQ(s, Status::Ok);
    EXPECT_EQ(out_count, 2);
    EXPECT_EQ(out_batch[0], batch[0]);
    EXPECT_EQ(out_batch[1], batch[1]);
}

TEST(DataExchangeServiceTest, SendCmdAndFlush) {
    auto mock = std::make_unique<MockTransport>();
    auto* mock_ptr = mock.get();
    
    TestService service(std::move(mock));
    ASSERT_EQ(service.init("test"), Status::Ok);

    // Отправляем 2 команды в очередь
    service.send_cmd({1, 0, 5, 100, 1}); // write
    service.send_cmd({1, 0, 6, 200, 1}); // write

    // В транспорте пока пусто
    EXPECT_TRUE(mock_ptr->get_sent_copy().empty());

    // Сливаем очередь
    service.async_flush();

    // Проверяем, что в транспорте появился 1 пакет (батч)
    auto sent = mock_ptr->get_sent_copy();
    ASSERT_EQ(sent.size(), 1);
    
    EXPECT_EQ(sent[0].hdr.kind, MsgKind::CommandBatch);
    EXPECT_EQ(sent[0].hdr.count, 2);

    // Проверяем содержимое
    size_t n = 0;
    Command unpacked[2];
    bool ok = PodSerializer<Command>::unpack(sent[0].payload.data(), sent[0].payload.size(), unpacked, 2, n);
    
    ASSERT_TRUE(ok);
    EXPECT_EQ(n, 2);
    EXPECT_EQ(unpacked[0].param_id, 5);
    EXPECT_EQ(unpacked[1].param_id, 6);
}