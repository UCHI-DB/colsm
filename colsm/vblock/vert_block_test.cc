//
// Created by harper on 12/5/20.
//

#include "vert_block.h"

#include <gtest/gtest.h>
#include <immintrin.h>

#include "table/block.h"

#include "byteutils.h"
#include "vert_block_builder.h"

using namespace leveldb;
using namespace colsm;

TEST(VertBlockMeta, Read) {
  uint8_t buffer[897];
  memset(buffer, 0, 897);

  // Fill in the buffer
  auto pointer = buffer;
  *(uint32_t*)pointer = 100;
  pointer += 4;
  for (int i = 0; i < 100; ++i) {
    *(uint64_t*)pointer = i;
    pointer += 8;
  }
  *(uint32_t*)pointer = 377;
  pointer += 4;

  *(int8_t*)pointer = 7;
  pointer += 1;

  VertBlockMeta meta;
  meta.Read(buffer);

  EXPECT_EQ(897, meta.EstimateSize());
}

TEST(VertBlockMeta, Write) {
  VertBlockMeta meta;
  for (int i = 0; i < 100; ++i) {
    meta.AddSection(2 * i, i + 300);
  }
  meta.Finish();
  // 9 + 100*8 + 700bit = 809 + 88
  EXPECT_EQ(897, meta.EstimateSize());

  uint8_t buffer[897];
  memset(buffer, 0, 897);
  meta.Write(buffer);

  auto pointer = buffer;
  EXPECT_EQ(100, *(uint32_t*)pointer);
  pointer += 4;
  for (auto i = 0; i < 100; ++i) {
    EXPECT_EQ(2 * i, *(uint64_t*)pointer);
    pointer += 8;
  }
  EXPECT_EQ(300, *(int32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(7, *(uint8_t*)pointer);
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

TEST(VertBlockMeta, SingleEntry) {
  VertBlockMeta meta;
  meta.AddSection(45, 300);
  meta.Finish();

  EXPECT_EQ(17, meta.EstimateSize());

  uint8_t buffer[17];
  memset(buffer, 0, 17);
  meta.Write(buffer);

  auto pointer = buffer;
  EXPECT_EQ(1, *(uint32_t*)pointer);
  pointer += 4;

  EXPECT_EQ(45, *(uint64_t*)pointer);
  pointer += 8;

  EXPECT_EQ(300, *(uint32_t*)pointer);
  pointer += 4;
  EXPECT_EQ(0, *(uint8_t*)pointer);

  VertBlockMeta readMeta;
  readMeta.Read(buffer);

  ASSERT_EQ(0, readMeta.Search(122));
  ASSERT_EQ(0, readMeta.Search(900));
}

TEST(VertBlockMeta, Search) {
  uint8_t buffer[897];

  {
    memset(buffer, 0, 897);
    // Fill in the buffer
    auto pointer = buffer;
    *(uint32_t*)pointer = 100;
    pointer += 4;
    for (int i = 0; i < 100; ++i) {
      *(uint64_t*)pointer = i * 50;
      pointer += 8;
    }
    *(uint32_t*)pointer = 377;
    pointer += 4;

    *(int8_t*)pointer = 7;
    pointer += 1;

    uint32_t plain[100];
    for (auto i = 0; i < 100; ++i) {
      plain[i] = i * 2 + 10;
    }
    sboost::byteutils::bitpack(plain, 100, 7, (uint8_t*)pointer);

    VertBlockMeta meta;
    meta.Read(buffer);

    EXPECT_EQ(0, meta.Search(9));
    EXPECT_EQ(17, meta.Search(422));
    EXPECT_EQ(17, meta.Search(421));
    EXPECT_EQ(18, meta.Search(423));
    EXPECT_EQ(99, meta.Search(841));
  }
  // Test large uint numbers
  {
    memset(buffer, 0, 897);
    // Fill in the buffer
    auto pointer = buffer;
    *(uint32_t*)pointer = 100;
    pointer += 4;
    for (int i = 0; i < 100; ++i) {
      *(uint64_t*)pointer = i * 50;
      pointer += 8;
    }
    *(uint32_t*)pointer = 0xF0180179;
    pointer += 4;

    *(int8_t*)pointer = 7;
    pointer += 1;

    uint32_t plain[100];
    for (auto i = 0; i < 100; ++i) {
      plain[i] = i * 2 + 10;
    }
    sboost::byteutils::bitpack(plain, 100, 7, (uint8_t*)pointer);

    VertBlockMeta meta;
    meta.Read(buffer);

    EXPECT_EQ(0, meta.Search(9));
    EXPECT_EQ(0, meta.Search(0xF0180009));
    EXPECT_EQ(17, meta.Search(0xF01801A6));
    EXPECT_EQ(17, meta.Search(0xF01801A5));
    EXPECT_EQ(18, meta.Search(0xF01801A7));
    EXPECT_EQ(99, meta.Search(0xF0180349));
  }
}

TEST(VertBlockMeta, SearchLargeBits) {
  uint8_t buffer[10000];
  // 28 bit values
  {
    memset(buffer, 0, 10000);
    uint8_t* pointer = buffer;
    *((uint32_t*)pointer) = 250;
    pointer += 4;
    for (auto i = 0; i < 250; ++i) {
      *((uint64_t*)pointer) = i * 160000;
      pointer += 8;
    }
    *((uint32_t*)pointer) = 0;
    pointer += 4;
    *(pointer++) = 29;
    std::vector<uint32_t> data;
    for (auto i = 0; i < 250; ++i) {
      data.push_back(0x1FFFFFFF - (249 - i) * 1000);
    }
    sboost::byteutils::bitpack(data.data(), data.size(), 29, pointer);

    VertBlockMeta meta;
    meta.Read(buffer);
    ASSERT_EQ(meta.NumSection(), 250);
    for (int i = 0; i < 250; ++i) {
      ASSERT_EQ(meta.SectionOffset(i), i * 160000);
    }

    for (int i = 0; i < 250; ++i) {
      ASSERT_EQ(i, meta.Search(0x1FFFFFFF - (249 - i) * 1000));
    }
  }
  // 32 bit values
  {
    memset(buffer, 0, 10000);
    uint8_t* pointer = buffer;
    *((uint32_t*)pointer) = 250;
    pointer += 4;
    for (auto i = 0; i < 250; ++i) {
      *((uint64_t*)pointer) = i * 160000;
      pointer += 8;
    }
    *((uint32_t*)pointer) = 0;
    pointer += 4;
    *(pointer++) = 32;
    for (auto i = 0; i < 250; ++i) {
      *((uint32_t*)pointer) = 0xFFFFFFFF-(249-i)*10000;
      pointer+=4;
    }

    VertBlockMeta meta;
    meta.Read(buffer);
    ASSERT_EQ(meta.NumSection(), 250);
    for (int i = 0; i < 250; ++i) {
      ASSERT_EQ(meta.SectionOffset(i), i * 160000);
    }

    for (int i = 0; i < 250; ++i) {
      ASSERT_EQ(i, meta.Search(0xFFFFFFFF - (249 - i) * 10000));
    }
  }
}

TEST(VertSection, Read) {
  uint8_t buffer[150];
  memset(buffer, 0, 150);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(uint32_t*)pointer = 0xFF051234;
  pointer += 4;

  *(uint32_t*)pointer = 201;
  pointer += 4;
  *(pointer++) = BITPACK;
  *(uint32_t*)pointer = 202;
  pointer += 4;
  *(pointer++) = PLAIN;
  *(uint32_t*)pointer = 204;
  pointer += 4;
  *(pointer++) = RUNLENGTH;
  *(uint32_t*)pointer = 208;
  pointer += 4;
  *(pointer++) = LENGTH;

  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  EXPECT_EQ(0xFF051234, section.StartValue());
  EXPECT_EQ(8, section.BitWidth());
  EXPECT_EQ((const uint8_t*)pointer, section.KeysData());
}

TEST(VertSection, Find) {
  uint8_t buffer[200];
  memset(buffer, 0, 200);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(uint32_t*)pointer = 0xFF0500EA;
  pointer += 4;

  *(uint32_t*)pointer = 201;
  pointer += 4;
  *(pointer++) = BITPACK;
  *(uint32_t*)pointer = 202;
  pointer += 4;
  *(pointer++) = PLAIN;
  *(uint32_t*)pointer = 204;
  pointer += 4;
  *(pointer++) = RUNLENGTH;
  *(uint32_t*)pointer = 208;
  pointer += 4;
  *(pointer++) = LENGTH;

  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  // diff is 124, location is 62
  EXPECT_EQ(62, section.Find(0xFF050166));
  EXPECT_EQ(-1, section.Find(0xFF050149));
  EXPECT_EQ(-1, section.Find(0xFF0501F4));
}

TEST(VertSection, FindStart) {
  uint8_t buffer[200];
  memset(buffer, 0, 200);
  auto pointer = buffer;

  *(uint32_t*)pointer = 100;
  pointer += 4;
  *(uint32_t*)pointer = 0xFF0500EA;
  pointer += 4;

  //  *(uint32_t*)pointer = 201;
  //  pointer += 4;
  //  *(pointer++) = BITPACK;
  //  *(uint32_t*)pointer = 202;
  //  pointer += 4;
  //  *(pointer++) = PLAIN;
  //  *(uint32_t*)pointer = 204;
  //  pointer += 4;
  //  *(pointer++) = RUNLENGTH;
  //  *(uint32_t*)pointer = 208;
  //  pointer += 4;
  //  *(pointer++) = LENGTH;

  *(pointer + 4) = BITPACK;
  pointer += 5;
  *(pointer + 4) = PLAIN;
  pointer += 5;
  *(pointer + 4) = RUNLENGTH;
  pointer += 5;
  *(pointer + 4) = LENGTH;
  pointer += 5;

  *(uint8_t*)pointer = 8;
  pointer++;

  uint32_t plain_buffer[100];
  for (auto i = 0; i < 100; ++i) {
    plain_buffer[i] = 2 * i;
  }
  sboost::byteutils::bitpack(plain_buffer, 100, 8, (uint8_t*)pointer);

  VertSection section;
  section.Read(buffer);
  EXPECT_EQ(0, section.FindStart(0xFF0500B1));
  //   diff is 124, location is 62
  EXPECT_EQ(62, section.FindStart(0xFF050166));
  //   Next is 330, diff is 96, location is 48
  EXPECT_EQ(48, section.FindStart(0xFF050149));
  // Larger than end
  EXPECT_EQ(-1, section.FindStart(0xFF050900));
}

TEST(VertSection, SingleEntry) {
  VertSectionBuilder builder;
  builder.Open(111);
  int value = 111;
  builder.Add(
      ParsedInternalKey(Slice((char*)&value, 4), 1000, ValueType::kTypeValue),
      Slice("This is a value"));
  uint8_t buffer[100];
  memset(buffer, 0, 100);
  builder.Close();
  builder.Dump(buffer);

  VertSection section;
  section.Read(buffer);

  ASSERT_EQ(0, section.BitWidth());
  ASSERT_EQ(1, section.NumEntry());
  ASSERT_EQ(111, section.StartValue());

  ASSERT_EQ(0, section.KeyDecoder()->DecodeU32());
  ASSERT_EQ(1000, section.SeqDecoder()->DecodeU64());
  ASSERT_EQ(1, section.TypeDecoder()->DecodeU8());
}

TEST(VertSection, Search32Bit) {}

TEST(VertSection, LargeBitWidthSearch) {
  VertSectionBuilder builder;

  uint32_t start = 3324630729;
  uint32_t data[233];

  {
    data[0] = 0;
    data[1] = 3693625;
    data[2] = 5192894;
    data[3] = 6140473;
    data[4] = 13780215;
    data[5] = 16830756;
    data[6] = 19277604;
    data[7] = 22971229;
    data[8] = 24470498;
    data[9] = 25418077;
    data[10] = 26917346;
    data[11] = 33057819;
    data[12] = 36108360;
    data[13] = 38555208;
    data[14] = 43748102;
    data[15] = 44695681;
    data[16] = 46194950;
    data[17] = 52335423;
    data[18] = 63025706;
    data[19] = 73072905;
    data[20] = 86210036;
    data[21] = 92350509;
    data[22] = 94797357;
    data[23] = 99990251;
    data[24] = 105487640;
    data[25] = 111628113;
    data[26] = 113127382;
    data[27] = 114074961;
    data[28] = 119267855;
    data[29] = 120215434;
    data[30] = 121714703;
    data[31] = 124765244;
    data[32] = 130905717;
    data[33] = 132404986;
    data[34] = 133352565;
    data[35] = 134851834;
    data[36] = 138545459;
    data[37] = 140992307;
    data[38] = 144042848;
    data[39] = 151682590;
    data[40] = 157823063;
    data[41] = 170960194;
    data[42] = 181007393;
    data[43] = 189594714;
    data[44] = 194144524;
    data[45] = 200284997;
    data[46] = 202731845;
    data[47] = 207924739;
    data[48] = 208872318;
    data[49] = 219562601;
    data[50] = 221061870;
    data[51] = 222009449;
    data[52] = 227202343;
    data[53] = 229649191;
    data[54] = 238840205;
    data[55] = 240339474;
    data[56] = 244033099;
    data[57] = 246479947;
    data[58] = 247336132;
    data[59] = 257170230;
    data[60] = 259617078;
    data[61] = 265757551;
    data[62] = 266613736;
    data[63] = 275804750;
    data[64] = 278894682;
    data[65] = 285891340;
    data[66] = 288941881;
    data[67] = 295082354;
    data[68] = 297529202;
    data[69] = 305168944;
    data[70] = 308219485;
    data[71] = 310666333;
    data[72] = 314359958;
    data[73] = 315859227;
    data[74] = 316806806;
    data[75] = 327497089;
    data[76] = 333637562;
    data[77] = 335136831;
    data[78] = 338187372;
    data[79] = 345827114;
    data[80] = 346774693;
    data[81] = 351967587;
    data[82] = 354414435;
    data[83] = 355270620;
    data[84] = 365104718;
    data[85] = 373692039;
    data[86] = 374548224;
    data[87] = 383739238;
    data[88] = 391378980;
    data[89] = 393825828;
    data[90] = 396876369;
    data[91] = 403016842;
    data[92] = 405463690;
    data[93] = 413103432;
    data[94] = 416153973;
    data[95] = 419847598;
    data[96] = 422294446;
    data[97] = 423793715;
    data[98] = 432984729;
    data[99] = 434483998;
    data[100] = 435431577;
    data[101] = 440624471;
    data[102] = 441480656;
    data[103] = 441572050;
    data[104] = 443071319;
    data[105] = 453761602;
    data[106] = 454709181;
    data[107] = 459902075;
    data[108] = 460758260;
    data[109] = 461705839;
    data[110] = 462348923;
    data[111] = 463205108;
    data[112] = 480035864;
    data[113] = 480983443;
    data[114] = 482482712;
    data[115] = 491673726;
    data[116] = 499313468;
    data[117] = 501760316;
    data[118] = 504810857;
    data[119] = 508504482;
    data[120] = 510951330;
    data[121] = 521641613;
    data[122] = 527782086;
    data[123] = 529281355;
    data[124] = 530228934;
    data[125] = 531085119;
    data[126] = 531728203;
    data[127] = 540919217;
    data[128] = 548558959;
    data[129] = 549415144;
    data[130] = 549506538;
    data[131] = 550362723;
    data[132] = 551005807;
    data[133] = 567836563;
    data[134] = 568692748;
    data[135] = 569640327;
    data[136] = 570283411;
    data[137] = 571139596;
    data[138] = 587970352;
    data[139] = 588917931;
    data[140] = 589561015;
    data[141] = 590417200;
    data[142] = 596557673;
    data[143] = 599608214;
    data[144] = 607247956;
    data[145] = 610298497;
    data[146] = 616438970;
    data[147] = 617295155;
    data[148] = 617938239;
    data[149] = 618885818;
    data[150] = 636572759;
    data[151] = 637215843;
    data[152] = 638163422;
    data[153] = 639019607;
    data[154] = 648853705;
    data[155] = 655850363;
    data[156] = 656493447;
    data[157] = 657349632;
    data[158] = 657441026;
    data[159] = 658297211;
    data[160] = 665936953;
    data[161] = 675127967;
    data[162] = 675771051;
    data[163] = 676627236;
    data[164] = 677574815;
    data[165] = 685214557;
    data[166] = 695904840;
    data[167] = 698351688;
    data[168] = 705095854;
    data[169] = 707542702;
    data[170] = 715182444;
    data[171] = 724373458;
    data[172] = 725872727;
    data[173] = 726820306;
    data[174] = 743651062;
    data[175] = 744507247;
    data[176] = 745150331;
    data[177] = 746097910;
    data[178] = 746954095;
    data[179] = 752146989;
    data[180] = 763784851;
    data[181] = 764427935;
    data[182] = 765284120;
    data[183] = 765375514;
    data[184] = 766231699;
    data[185] = 771424593;
    data[186] = 772372172;
    data[187] = 773871441;
    data[188] = 783062455;
    data[189] = 784561724;
    data[190] = 787008572;
    data[191] = 790702197;
    data[192] = 793752738;
    data[193] = 801392480;
    data[194] = 803839328;
    data[195] = 809979801;
    data[196] = 813030342;
    data[197] = 815477190;
    data[198] = 823116932;
    data[199] = 832307946;
    data[200] = 833164131;
    data[201] = 841751452;
    data[202] = 851585550;
    data[203] = 852441735;
    data[204] = 854888583;
    data[205] = 860081477;
    data[206] = 861029056;
    data[207] = 871719339;
    data[208] = 873218608;
    data[209] = 874166187;
    data[210] = 879359081;
    data[211] = 881805929;
    data[212] = 890049364;
    data[213] = 890996943;
    data[214] = 892496212;
    data[215] = 896189837;
    data[216] = 898636685;
    data[217] = 901687226;
    data[218] = 909326968;
    data[219] = 911773816;
    data[220] = 917914289;
    data[221] = 920964830;
    data[222] = 927961488;
    data[223] = 931051420;
    data[224] = 940242434;
    data[225] = 941098619;
    data[226] = 947239092;
    data[227] = 949685940;
    data[228] = 959520038;
    data[229] = 960376223;
    data[230] = 962823071;
    data[231] = 966516696;
    data[232] = 968015965;
  }
  builder.Open(start);
  for (auto i : data) {
    uint32_t value = i + start;
    builder.Add(
        ParsedInternalKey(Slice((char*)&value, 4), 1000, ValueType::kTypeValue),
        Slice("This is a value"));
  }
  size_t buffer_size = builder.EstimateSize();
  uint8_t buffer[buffer_size];
  memset(buffer, 0, buffer_size);
  builder.Close();
  builder.Dump(buffer);

  VertSection section;
  section.Read(buffer);

  ASSERT_EQ(30, section.BitWidth());
  ASSERT_EQ(233, section.NumEntry());
  ASSERT_EQ(start, section.StartValue());

  for (auto i = 0; i < 233; ++i) {
    auto target = data[i];

    auto index = section.FindStart(target + start);
    ASSERT_EQ(index, i);
  }
}

TEST(VertBlock, SeekToFirst) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((uint32_t*)buffer) = 0xFF000000 + i;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  ParsedInternalKey pkey;
  {
    auto ite = block.NewIterator(NULL);

    ite->SeekToFirst();
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(0xFF000000, *((uint32_t*)pkey.user_key.data()));
    EXPECT_EQ(4, pkey.user_key.size());
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);

    EXPECT_EQ(12, value.size());
    EXPECT_EQ(0xFF000000, *((uint32_t*)value.data()));

    delete ite;
  }
}

TEST(VertBlock, SeekToLast) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = 0xF0000000 + i;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  ParsedInternalKey pkey;
  {
    auto ite = block.NewIterator(NULL);

    ite->SeekToLast();
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(0xF00F423F, *((uint32_t*)pkey.user_key.data()));
    EXPECT_EQ(4, pkey.user_key.size());
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);

    EXPECT_EQ(12, value.size());
    EXPECT_EQ(0xF00F423F, *((uint32_t*)value.data()));

    delete ite;
  }
}

TEST(VertBlock, Next) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = i;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  ParsedInternalKey pkey;
  auto ite = block.NewIterator(NULL);
  ite->SeekToFirst();
  for (int i = 0; i < 1000000; ++i) {
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size()) << i;
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(i, *((int32_t*)pkey.user_key.data())) << i;
    EXPECT_EQ(4, pkey.user_key.size()) << i;
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);

    EXPECT_EQ(12, value.size()) << i;
    EXPECT_EQ(i, *((int32_t*)value.data())) << i;
    ite->Next();
  }
  delete ite;
}

TEST(VertBlock, Seek) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = 2 * i + 10;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);

  ParsedInternalKey pkey;
  {
    // Test exact match on first section
    auto ite = block.NewIterator(NULL);
    int target_key = 30;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(30, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(30, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test exact match on following section
    auto ite = block.NewIterator(NULL);
    int target_key = 390;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(390, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(390, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, within sections
    auto ite = block.NewIterator(NULL);
    int target_key = 79;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(80, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(80, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, smaller than all
    auto ite = block.NewIterator(NULL);
    int target_key = 5;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(10, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(10, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, larger than all
    auto ite = block.NewIterator(NULL);
    int target_key = 5000000;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().IsNotFound());
    delete ite;
  }
  {
    // Test no match, between the first two sections
    auto ite = block.NewIterator(NULL);
    int target_key = 265;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(266, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(266, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, between the last two sections
    auto ite = block.NewIterator(NULL);
    int target_key = 1999881;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    auto key = ite->key();
    auto value = ite->value();
    EXPECT_EQ(12, key.size());
    ParseInternalKey(key, &pkey);
    EXPECT_EQ(1999882, *((int32_t*)pkey.user_key.data()));
    EXPECT_EQ(1350, pkey.sequence);
    EXPECT_EQ(ValueType::kTypeValue, pkey.type);
    EXPECT_EQ(12, value.size());
    EXPECT_EQ(1999882, *((int32_t*)value.data()));
    delete ite;
  }
  {
    // Test no match, after the last sections
    auto ite = block.NewIterator(NULL);
    int target_key = 3999879;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().IsNotFound());

    delete ite;
  }
}

TEST(VertBlock, SeekThenNext) {
  Options option;
  VertBlockBuilder builder(&option, LENGTH);

  char buffer[12];
  Slice key((const char*)buffer, 12);
  for (uint32_t i = 0; i < 1000000; ++i) {
    *((int32_t*)buffer) = 2 * i;
    EncodeFixed64(buffer + 4, (1350 << 8) | ValueType::kTypeValue);
    builder.Add(key, key);
  }
  auto result = builder.Finish();

  BlockContents content;
  content.data = result;
  content.cachable = false;
  content.heap_allocated = false;
  VertBlockCore block(content);
  ParsedInternalKey pkey;

  {
    auto ite = block.NewIterator(NULL);
    int target_key = 10;
    Slice target((const char*)&target_key, 4);
    ite->Seek(target);
    EXPECT_TRUE(ite->status().ok());
    int i = 0;
    while (ite->Valid()) {
      auto key = ite->key();
      auto value = ite->value();
      ASSERT_EQ(12, key.size());
      ParseInternalKey(key, &pkey);
      ASSERT_EQ((5 + i) * 2, *((int32_t*)pkey.user_key.data())) << i;
      ASSERT_EQ(1350, pkey.sequence);
      ASSERT_EQ(ValueType::kTypeValue, pkey.type);
      ASSERT_EQ(12, value.size());
      ASSERT_EQ((5 + i) * 2, *((int32_t*)value.data())) << i;
      ite->Next();
      i++;
    }
    EXPECT_EQ(i, 999995);
    delete ite;
  }
}

// LevelDB test did not use gtest_main
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}