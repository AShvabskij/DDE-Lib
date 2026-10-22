#include <gtest/gtest.h>
#include "dde/mock_transport.hpp"

using namespace dde;

TEST(MockTransportTest, SendAndReceive) {
    MockTransport t;
    ASSERT_EQ(t.open("test"), Status::Ok);

    // 1. Проверяем send
    MsgHeader out_hdr{MsgKind::DataPush, 2, 8};
    int out_data[2] = {42, 43};
    ASSERT_EQ(t.send(out_hdr, out_data, sizeof(out_data)), Status::Ok);

    auto sent = t.get_sent_copy();
    ASSERT_EQ(sent.size(), 1);
    MockTransport::Message msg = sent.front();
    EXPECT_EQ(msg.hdr.kind, MsgKind::DataPush);
    EXPECT_EQ(msg.payload.size(), 8);

    // 2. Проверяем recv (через inject)
    MsgHeader in_hdr{MsgKind::CommandBatch, 1, 4};
    int in_data = 100;
    t.inject_recv(in_hdr, &in_data, sizeof(in_data));

    MsgHeader actual_hdr;
    int actual_data = 0;
    size_t actual_len = 0;
    
    Status s = t.recv(actual_hdr, &actual_data, sizeof(actual_data), actual_len, std::chrono::milliseconds(10));
    
    ASSERT_EQ(s, Status::Ok);
    EXPECT_EQ(actual_hdr.kind, MsgKind::CommandBatch);
    EXPECT_EQ(actual_len, sizeof(in_data));
    EXPECT_EQ(actual_data, 100);
}

TEST(MockTransportTest, RecvTimeout) {
    MockTransport t;
    ASSERT_EQ(t.open("test"), Status::Ok);

    MsgHeader hdr;
    int buf = 0;
    size_t len = 0;
    
    // Ждём 10мс, но ничего не инжектировали
    Status s = t.recv(hdr, &buf, sizeof(buf), len, std::chrono::milliseconds(10));
    
    EXPECT_EQ(s, Status::Timeout);
}