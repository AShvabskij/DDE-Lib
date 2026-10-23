#pragma once
#include "dde/itransport.hpp"
#include <string>

namespace dde {

constexpr size_t ShmSlotPayload = Max_Msg_Payload;

#pragma pack(push, 1)
struct ShmSlot {
    MsgHeader hdr;
    uint32_t  payload_len;
    uint8_t   payload[ShmSlotPayload];
};

// Структура одного независимого сегмента shared memory
struct ShmLayout {
    pthread_mutex_t mtx;
    pthread_cond_t  cv_not_empty;
    pthread_cond_t  cv_not_full;
    
    uint32_t magic;   // 0x44444531 ("DDE1")
    uint32_t version; // 1
    
    uint32_t head;
    uint32_t tail;
    bool     closed;
    
    ShmSlot slots[Ring_Capacity];
};
#pragma pack(pop)

enum class ShmDirection {
    UpToDown, // Верхний уровень: пишет в UpToDown, читает из DownToUp
    DownToUp  // Нижний уровень: пишет в DownToUp, читает из UpToDown
};

enum class RingType {
    Tx, // Transmit (куда пишем)
    Rx  // Receive  (откуда читаем)
};

class ShmTransport : public ITransport {
public:
    explicit ShmTransport(ShmDirection dir, bool is_owner = false);
    ~ShmTransport() override;

    Status open(const std::string& endpoint) override;
    void   close() override;

    Status send(const MsgHeader& hdr, const void* payload, size_t payload_len) override;
    Status recv(MsgHeader& hdr, void* buf, size_t cap, size_t& out_len,
                std::chrono::milliseconds timeout) override;

    bool        is_connected() const override;
    const char* kind() const override { return "shm"; }

private:
    Status open_segment(const std::string& base_name, RingType type);
    void close_segment(int& fd, ShmLayout*& layout);

    ShmDirection m_dir;
    bool m_is_owner;
    std::string m_base_name;

    // Указатели на наши "роли" (физические сегменты, в которые мы пишем/читаем)
    int m_tx_fd = -1;
    ShmLayout* m_tx_ring = nullptr; // transmit (куда пишем)

    int m_rx_fd = -1;
    ShmLayout* m_rx_ring = nullptr; // receive (откуда читаем)
};

} // namespace dde