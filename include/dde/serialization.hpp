#pragma once
#include <cstddef>
#include <type_traits>
#include <vector>
#include <cstring>

namespace dde {

// Сериализатор по умолчанию: работает для любых trivially copyable типов.
template<typename T> 
struct PodSerializer {
    static_assert(std::is_trivially_copyable_v<T>, "Not trivially copyable type");

    static std::vector<std::byte> pack(const T* arr, size_t n) {
        if (n == 0) return {};
        if (arr == nullptr) return {};


        std::vector<std::byte> res(n * sizeof(T));
        std::memcpy(res.data(), arr, res.size());
        return res;
    }

    static bool unpack(const void* buf, size_t len, T* arr_out, size_t arr_cap, size_t& n) {
        if (len % sizeof(T) != 0) return false;

        n = len/sizeof(T);
        if (n == 0) return true; // pscket is empty
        if (n > arr_cap) return false;

        if (buf == nullptr || arr_out == nullptr) return false;

        std::memcpy(arr_out, buf, len);
        return true;
    }
};
}
