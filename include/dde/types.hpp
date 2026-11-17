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
    Overflow,
    Partial,            // Данные прочитаны, но поместились не все (есть остаток в inbox)
    Error,              // ошибка или некорректные данные
};

enum class MsgKind : uint16_t {
    DataPush = 1,
    CommandBatch
};

enum class Level {
    Upper = 0, // Верхний уровень, потребитель данных 
    Lower  // Нижний уровень, отправитель данных
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
constexpr size_t Max_Msg_Payload = 4096;  // максимальный размер payload
constexpr size_t Ring_Capacity = 64;      // число слотов в ring buffer'е
constexpr size_t Max_Msg_Count = 1000;    // максимальный размер очереди сообщений
constexpr size_t Max_Pending_Cmds = 1024; // максимальный размер вх. кеша команд
constexpr size_t Max_Pending_Data = 1024; // максимальный размер вх. кеша данных

}

