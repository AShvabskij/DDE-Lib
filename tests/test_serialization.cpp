#include <gtest/gtest.h>
#include <cstdint>
#include "dde/serialization.hpp"

struct Parameter {                       // = TData
    uint16_t device_id, module_id, param_id;
    float value;
    int64_t  timestamp_ns;

    // Оператор равенства для удобной проверки в EXPECT_EQ
    bool operator==(const Parameter& other) const {
        return device_id == other.device_id 
        && module_id == other.module_id 
        && param_id == other.param_id
        && value == other.value 
        && timestamp_ns == other.timestamp_ns;
    }

};

// Успешная упаковка и распаковка
TEST(PodSerializerTest, PackUnpackSuccess) {
    Parameter original[] = {
        {1, 1, 1, 3.14f, 1000},
        {1, 1, 2, 2.71f, 2000}
    };

    // Упаковываем 2 элемента
    auto packed = dde::PodSerializer<Parameter>::pack(original, 2);
    

    ASSERT_EQ(packed.size(), 2 * sizeof(Parameter));


    Parameter unpacked[2];
    size_t n = 0;
    bool ok = dde::PodSerializer<Parameter>::unpack(
        packed.data(), packed.size(), unpacked, 2, n
    );

    // Проверяем результат
    ASSERT_TRUE(ok);
    ASSERT_EQ(n, 2);
    EXPECT_EQ(unpacked[0], original[0]);
    EXPECT_EQ(unpacked[1], original[1]);
}

// Отказ, если данных больше, чем вмещает буфер
TEST(PodSerializerTest, UnpackOverflowFails) {
    Parameter original[3] = {{1, 1, 1, 1.0f, 1}, {1, 1, 2, 2.0f, 2}, {1, 2, 3, 3.0f, 3}};
    auto packed = dde::PodSerializer<Parameter>::pack(original, 3);

    Parameter small_buffer[2]; 
    size_t n = 0;
    
    // Пытаемся распаковать 3 элемента в буфер на 2
    bool ok = dde::PodSerializer<Parameter>::unpack(
        packed.data(), packed.size(), small_buffer, 2, n
    );

    EXPECT_FALSE(ok); // переполнение
}

// Пустой пакет
TEST(PodSerializerTest, PackEmpty) {
    auto packed = dde::PodSerializer<Parameter>::pack(nullptr, 0);
    EXPECT_TRUE(packed.empty());
}

// 4. Распаковка пустых данных (n == 0)
TEST(PodSerializerTest, UnpackEmpty) {
    Parameter buffer[2];
    size_t n = 99; // Специально мусорное значение
    bool ok = dde::PodSerializer<Parameter>::unpack(
        nullptr, 0, buffer, 2, n
    );
    
    EXPECT_TRUE(ok);
    EXPECT_EQ(n, 0);
}