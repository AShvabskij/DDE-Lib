#include "dde/shm_transport.hpp"
#include <cstring>

#ifdef __linux__
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <mutex>
#include <chrono>
#endif

namespace dde {

#ifdef __linux__

constexpr uint32_t MAGIC = 0x44444531;
constexpr uint32_t CURRENT_VERSION = 1;

static int lock_robust(pthread_mutex_t* m) {
    int rc = pthread_mutex_lock(m);
    if (rc == EOWNERDEAD) {
        pthread_mutex_consistent(m);
        rc = 0;
    }
    return rc;
}

ShmTransport::ShmTransport(Level level, bool is_owner) 
    : m_level(level), m_is_owner(is_owner) {}

ShmTransport::~ShmTransport() { close(); }

Status ShmTransport::open_segment(const std::string& base_name, RingType type) {
    bool is_tx = (type == RingType::Tx);

    // Определяем суффикс: 
    // Верхний уровень: пишет в up_to_down, читает из down_to_up
    // Нижний уровень: пишет в down_to_up, читает из up_to_down

    std::string suffix = "";
    if (is_tx) {
        suffix = (m_level == Level::Upper ? "_up_to_down" : "_down_to_up");        
    } else {
        suffix = (m_level == Level::Upper ? "_down_to_up" : "_up_to_down");
    }

    std::string full_name = base_name + suffix;

    int fd = shm_open(full_name.c_str(), O_RDWR| O_CREAT, 0666); // открыть или создать

    if (fd < 0) return Status::Error;

    int rc = flock(fd, LOCK_EX);
    if (rc == -1) {
        std::cerr << "Open_segment failed: " << strerror(errno) << "\n";
        ::close(fd);
        return Status::Error;
    }

    if (ftruncate(fd, sizeof(ShmLayout)) != 0) {
        std::cerr << "Open_segment failed: " << strerror(errno) << "\n";
        ::close(fd);
        return Status::Error;
    }

    void* p = mmap(nullptr, sizeof(ShmLayout), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { 
        std::cerr << "Open_segment failed: " << strerror(errno) << "\n";
        ::close(fd); 
        return Status::Error; 
    }
            
    ShmLayout* layout = static_cast<ShmLayout*>(p);

    if (layout->magic != MAGIC) { // структура не инициализирована
            std::memset(layout, 0, sizeof(ShmLayout));
            
            pthread_mutexattr_t ma;
            pthread_mutexattr_init(&ma);
            pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
            pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
            pthread_mutex_init(&layout->mtx, &ma);
            pthread_mutexattr_destroy(&ma);

            pthread_condattr_t ca;
            pthread_condattr_init(&ca);
            pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
            pthread_condattr_setpshared(&ca, PTHREAD_PROCESS_SHARED);
            pthread_cond_init(&layout->cv_not_empty, &ca);
            pthread_cond_init(&layout->cv_not_full, &ca);
            pthread_condattr_destroy(&ca);

            layout->magic = MAGIC;
            layout->version = CURRENT_VERSION;
            layout->closed = false;
    }

    if (layout->version != CURRENT_VERSION ) {
        std::cerr << "Open_segment failed: shm version mismatch\n";
        ::close(fd);
        return Status::Error;
    }

    if (fd < 0) return Status::Error;

    if (is_tx) {
        m_tx_fd = fd;
        m_tx_ring = static_cast<ShmLayout*>(layout);
    } else {
        m_rx_fd = fd;
        m_rx_ring = static_cast<ShmLayout*>(layout);
    }
    
    flock(fd, LOCK_UN);

    return Status::Ok;
}

Status ShmTransport::open(const std::string& endpoint) {
    m_base_name = endpoint;
    if (m_base_name.empty() || m_base_name[0] != '/') {
        m_base_name = "/" + m_base_name;
    }

    Status s = open_segment(m_base_name, RingType::Tx);
    if (s != Status::Ok) return s;

    s = open_segment(m_base_name, RingType::Rx); 
    if (s != Status::Ok) return s;

    return Status::Ok;
}

void ShmTransport::close_segment(int& fd, ShmLayout*& layout) {
    if (layout) {
        munmap(layout, sizeof(ShmLayout));
        layout = nullptr;
    }
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}


void ShmTransport::notify_close() {
    if (m_tx_ring) {
        pthread_mutex_lock(&m_tx_ring->mtx);
        m_tx_ring->closed = true;
        pthread_cond_broadcast(&m_tx_ring->cv_not_empty);
        pthread_cond_broadcast(&m_tx_ring->cv_not_full);
        pthread_mutex_unlock(&m_tx_ring->mtx);
    }

    if (m_rx_ring) {
        pthread_mutex_lock(&m_rx_ring->mtx);
        m_rx_ring->closed = true;        
        pthread_cond_broadcast(&m_rx_ring->cv_not_empty);
        pthread_cond_broadcast(&m_rx_ring->cv_not_full);
        pthread_mutex_unlock(&m_rx_ring->mtx);
    }
}

void ShmTransport::close() {
    // двухфазная остановка

    notify_close(); // фаза 1: пробудить все потоки, завершить текущие операции
    
    std::unique_lock<std::shared_mutex> lock(m_stop_mtx); // ждать завершения всех операций
    
    // фаза 2: удалить ресурсы
    close_segment(m_tx_fd, m_tx_ring);
    close_segment(m_rx_fd, m_rx_ring);
    
    if (m_is_owner) {
        shm_unlink((m_base_name + "_up_to_down").c_str());
        shm_unlink((m_base_name + "_down_to_up").c_str());
    }
}

bool ShmTransport::is_connected() const {
    return m_tx_ring != nullptr && m_rx_ring != nullptr 
        && !m_tx_ring->closed && !m_rx_ring->closed;
}

Status ShmTransport::send(const MsgHeader& hdr, const void* payload, size_t payload_len) {
    if (!m_tx_ring) return Status::Error;
    if (payload_len > Max_Msg_Payload) return Status::Overflow;

    std::shared_lock<std::shared_mutex> lock(m_stop_mtx); 

    if (lock_robust(&m_tx_ring->mtx) != 0) return Status::Error;

    uint32_t next_head = (m_tx_ring->head + 1) % Ring_Capacity;
    if (next_head == m_tx_ring->tail) {
        pthread_mutex_unlock(&m_tx_ring->mtx);
        return Status::Busy;
    }

    ShmSlot& slot = m_tx_ring->slots[m_tx_ring->head];
    slot.hdr = hdr;
    slot.payload_len = static_cast<uint32_t>(payload_len);
    if (payload_len > 0 && payload != nullptr) {
        std::memcpy(slot.payload, payload, payload_len);
    }

    m_tx_ring->head = next_head;
    pthread_cond_signal(&m_tx_ring->cv_not_empty);
    
    pthread_mutex_unlock(&m_tx_ring->mtx);
    return Status::Ok;
}

Status ShmTransport::recv(MsgHeader& hdr, void* buf, size_t cap, size_t& out_len,
                          std::chrono::milliseconds timeout) {
    if (!m_rx_ring) return Status::Error;

    std::shared_lock<std::shared_mutex> lock(m_stop_mtx); 

    auto start = std::chrono::steady_clock::now();

    if (lock_robust(&m_rx_ring->mtx) != 0) return Status::Error;

    if (m_rx_ring->closed) {
        pthread_mutex_unlock(&m_rx_ring->mtx);
        return Status::Disconnected;
    }

    while (true) {
        if (m_rx_ring->closed) {
            pthread_mutex_unlock(&m_rx_ring->mtx);
            return Status::Disconnected;
        }

        if (m_rx_ring->head != m_rx_ring->tail) {
            ShmSlot& slot = m_rx_ring->slots[m_rx_ring->tail];
            if (slot.payload_len > cap) {
                pthread_mutex_unlock(&m_rx_ring->mtx);
                return Status::Error;
            }

            hdr = slot.hdr;
            out_len = slot.payload_len;
            if (out_len > 0 && buf != nullptr) {
                std::memcpy(buf, slot.payload, out_len);
            }

            m_rx_ring->tail = (m_rx_ring->tail + 1) % Ring_Capacity;
            pthread_cond_signal(&m_rx_ring->cv_not_full);
            
            pthread_mutex_unlock(&m_rx_ring->mtx);
            return Status::Ok;
        }

        auto now = std::chrono::steady_clock::now();
        if (now - start >= timeout) {
            pthread_mutex_unlock(&m_rx_ring->mtx);
            return Status::Timeout;
        }
        auto remaining = timeout - std::chrono::duration_cast<std::chrono::milliseconds>(now - start);

        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);           // текущий момент + сколько ждать
        ts.tv_sec  += remaining.count() / 1000;          
        ts.tv_nsec += (remaining.count() % 1000) * 1'000'000;
        if (ts.tv_nsec >= 1'000'000'000) {             // нормализация
            ts.tv_sec  += 1;
            ts.tv_nsec -= 1'000'000'000;
        }

        int rc = pthread_cond_timedwait(&m_rx_ring->cv_not_empty, &m_rx_ring->mtx, &ts);
        
        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&m_rx_ring->mtx);
            return Status::Timeout;
        }
    }
}

#else 
// ==========================================
// Заглушка для Windows
// ==========================================
ShmTransport::ShmTransport(Level level, bool is_owner) : m_level(level), m_is_owner(is_owner) {}
ShmTransport::~ShmTransport() {}
Status ShmTransport::open_segment(const std::string&, RingType) { return Status::Error; }
Status ShmTransport::open(const std::string&) { return Status::Error; }
void ShmTransport::close_segment(int&, ShmLayout*&) {}
void ShmTransport::close() {}
bool ShmTransport::is_connected() const { return false; }
Status ShmTransport::send(const MsgHeader&, const void*, size_t) { return Status::Error; }
Status ShmTransport::recv(MsgHeader&, void*, size_t, size_t&, std::chrono::milliseconds) { return Status::Error; }

#endif

} // namespace dde