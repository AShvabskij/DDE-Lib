#pragma once
#include <cstdint>

struct Parameter {
    uint16_t device_id, module_id, param_id;
    float value;
    int64_t timestamp_ns;
};

struct Command {
    uint16_t device_id, module_id, param_id;
    uint32_t value;
    uint8_t nRW; // 0 - read, 1 - write
};

// Симуляция: 1 устройство, 2 модуля, по 8 параметров в каждом
constexpr uint16_t DEMO_DEVICE_ID = 1;
constexpr uint16_t DEMO_MODULES_COUNT = 2;
constexpr uint16_t DEMO_PARAMS_PER_MODULE = 8;