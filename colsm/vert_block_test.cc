//
// Created by harper on 12/5/20.
//

#include "table/block.h"

#include <gtest/gtest.h>

#include "byteutils.h"
#include "vert_block.h"
#include "vert_block_builder.h"

using namespace leveldb;
using namespace leveldb::vert;

TEST(VertBlockMetaTest, Read) {
    char buffer[897];
    memset(buffer, 0, 897);

    // Fill in the buffer
    auto pointer = buffer;
    *(uint32_t *) pointer = 100;
    pointer += 4;
    for (int i = 0; i < 100; ++i) {
        *(uint64_t *) pointer = i;
        pointer += 8;
    }
    *(int32_t *) pointer = 377;
    pointer += 4;

    *(int8_t *) pointer = 7;
    pointer += 1;

    VertBlockMeta meta;
    meta.Read(buffer);

    EXPECT_EQ(897, meta.EstimateSize());
}

TEST(VertBlockMetaTest, Write) {
    VertBlockMeta meta;
    for (int i = 0; i < 100; ++i) {
        meta.AddSection(2 * i, i + 300);
    }
    meta.Finish();
    // 9 + 100*8 + 700bit = 809 + 88
    EXPECT_EQ(897, meta.EstimateSize());

    char buffer[897];
    memset(buffer, 0, 897);
    meta.Write(buffer);

    auto pointer = buffer;
    EXPECT_EQ(100, *(uint32_t *) pointer);
    pointer += 4;
    for (auto i = 0; i < 100; ++i) {
        EXPECT_EQ(2 * i, *(uint64_t *) pointer);
        pointer += 8;
    }
    EXPECT_EQ(300, *(int32_t *) pointer);
    pointer += 4;
    EXPECT_EQ(7, *(uint8_t *) pointer);
    pointer++;

    uint8_t pack_buffer[88];
    memset(pack_buffer, 0, 88);
    uint32_t plain[100];
    for (auto i = 0; i < 100; ++i) plain[i] = i;
    sboost::byteutils::bitpack(plain, 100, 7, pack_buffer);

    for (auto i = 0; i < 88; ++i) {
        EXPECT_EQ(pack_buffer[i], (uint8_t) * (pointer++)) << i;
    }
}

TEST(VertBlockMetaTest, Search) {
    char buffer[897];
    memset(buffer, 0, 897);

    // Fill in the buffer
    auto pointer = buffer;
    *(uint32_t *) pointer = 100;
    pointer += 4;
    for (int i = 0; i < 100; ++i) {
        *(uint64_t *) pointer = i * 50;
        pointer += 8;
    }
    *(int32_t *) pointer = 377;
    pointer += 4;

    *(int8_t *) pointer = 7;
    pointer += 1;

    uint32_t plain[100];
    for (auto i = 0; i < 100; ++i) {
        plain[i] = i;
    }
    sboost::byteutils::bitpack(plain, 100, 7, (uint8_t *) pointer);

    VertBlockMeta meta;
    meta.Read(buffer);

    EXPECT_EQ(750, meta.Search(392));  // 15 in entrie
}

TEST(VertSectionTest, Write) {
    VertSection section(Encodings::PLAIN);
    char v[4];
    Slice value(v, 4);  // Dummy
    section.StartValue(3);
    for (auto i = 0; i < 100; ++i) {
        section.Add(i * 2 + 3, value);
    }
    auto size = section.EstimateSize();
    // 114 data, 800 value
    EXPECT_EQ(914, size);
    EXPECT_EQ(100, section.NumEntry());
    char buffer[size];
    memset(buffer, 0, size);

    section.Dump(buffer);

    auto pointer = buffer;
    EXPECT_EQ(100, *(uint32_t *) pointer);
    pointer += 4;

    EXPECT_EQ(3, *(int32_t *) pointer);
    pointer += 4;

    EXPECT_EQ(8, *(uint8_t *) pointer);
    pointer++;

    uint32_t plain_buffer[100];
    uint8_t packed_buffer[104];
    memset(packed_buffer, 0, 104);
    for (auto i = 0; i < 100; ++i) {
        plain_buffer[i] = 2 * i;
    }
    sboost::byteutils::bitpack(plain_buffer, 100, 8, packed_buffer);

    for (auto i = 0; i < 104; ++i) {
        EXPECT_EQ(packed_buffer[i], (uint8_t)(*pointer++));
    }
}

TEST(VertSectionTest, Read) {
    char buffer[113];
    memset(buffer, 0, 113);
    auto pointer = buffer;

    *(uint32_t *) pointer = 100;
    pointer += 4;
    *(int32_t *) pointer = 234;
    pointer += 4;
    *(uint8_t *) pointer = 8;
    pointer++;

    uint32_t plain_buffer[100];
    for (auto i = 0; i < 100; ++i) {
        plain_buffer[i] = 2 * i;
    }
    sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t *) pointer);

    VertSection section;
    section.Read(buffer);
    EXPECT_EQ(234, section.StartValue());
    EXPECT_EQ(113, section.EstimateSize());
    EXPECT_EQ(8, section.BitWidth());
    EXPECT_EQ((const uint8_t *) pointer, section.KeysData());
}

TEST(VertSectionTest, Find) {
    char buffer[113];
    memset(buffer, 0, 113);
    auto pointer = buffer;

    *(uint32_t *) pointer = 100;
    pointer += 4;
    *(int32_t *) pointer = 234;
    pointer += 4;
    *(uint8_t *) pointer = 8;
    pointer++;

    uint32_t plain_buffer[100];
    for (auto i = 0; i < 100; ++i) {
        plain_buffer[i] = 2 * i;
    }
    sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t *) pointer);

    VertSection section;
    section.Read(buffer);
    // diff is 124, location is 62
    EXPECT_EQ(62, section.Find(358));

    EXPECT_EQ(-1, section.Find(329));

}

TEST(VertSectionTest, FindStart) {
    char buffer[113];
    memset(buffer, 0, 113);
    auto pointer = buffer;

    *(uint32_t *) pointer = 100;
    pointer += 4;
    *(int32_t *) pointer = 234;
    pointer += 4;
    *(uint8_t *) pointer = 8;
    pointer++;

    uint32_t plain_buffer[100];
    for (auto i = 0; i < 100; ++i) {
        plain_buffer[i] = 2 * i;
    }
    sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t *) pointer);

    VertSection section;
    section.Read(buffer);
    // diff is 124, location is 62
    EXPECT_EQ(62, section.FindStart(358));
    // Next is 330, diff is 96, location is 48
    EXPECT_EQ(48, section.FindStart(329));
}

TEST(VertBlockTest, Build) {
    Options option;
    VertBlockBuilder builder(&option);
    builder.encoding_ = Encodings::LENGTH;

    int buffer = 0;
    Slice key((const char *) &buffer, 4);
    for (uint32_t i = 0; i < 1000; ++i) {
        buffer = i;
        builder.Add(key, key);
    }
    auto result = builder.Finish();
    // section_size = 128, 9 sections
    // meta = 9 + 9 * 8 + 4 * 9/8 = 86
    // section =
    EXPECT_EQ(9049, result.size());
}

TEST(VertBlockTest, Next) {
    Options option;
    VertBlockBuilder builder(&option);
    builder.encoding_ = Encodings::LENGTH;

    int buffer = 0;
    Slice key((const char *) &buffer, 4);
    for (uint32_t i = 0; i < 1000; ++i) {
        buffer = i;
        builder.Add(key, key);
    }
    auto result = builder.Finish();

    BlockContents content;
    content.data = result;
    content.cachable = false;
    content.heap_allocated = false;
    VertBlock block(content);

    auto ite = block.NewIterator(NULL);
    for (int i = 0; i < 1000; ++i) {
        ite->Next();
        auto key = ite->key();
        auto value = ite->value();
        EXPECT_EQ(4, key.size());
        EXPECT_EQ(i, *((int32_t *) key.data()));
        EXPECT_EQ(4, value.size());
        EXPECT_EQ(i, *((int32_t *) value.data()));
    }
}

TEST(VertBlockTest, Seek) {
    // Scenario: two blocks, first has data 0-100
}


TEST(VertBlockTest, SeekThenNext) {

}

// LevelDB test did not use gtest_main
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}