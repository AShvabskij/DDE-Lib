#pragma once
#include <cstdint>
#include <cstddef>

namespace dde {

// Коды возврата операций
enum class Status {
    Ok = 0,             // операция выполнена
    Busy,               // буфер получателя заполнен, попробуйте позже
    Timeout,            // операция не завершилась за отведённое время
    Disconnected,       // соединение разорвано
    Error               // ошибка или некорректные данные
};

enum class MsgKind : uint16_t {
    DataPush = 1,
    CommandBatch
};

// Заголовок сообщения. Передаётся перед payload'ом.
#pragma pack(push, 1)
struct MsgHeader {
    MsgKind  kind;          // тип сообщения
    uint16_t count;         // число элементов в payload
    uint32_t payload_len;   // длина payload в байтах
};
static_assert(sizeof(MsgHeader) == 8, "MsgHeader must be 8 bytes for ABI stability");

#pragma pack(pop)

// Константы по умолчанию
constexpr size_t Max_Msg_Payload = 4096;   // максимальный размер payload
constexpr size_t Ring_Capacity = 64;      // число слотов в ring buffer'е
}

