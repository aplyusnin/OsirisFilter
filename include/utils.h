/*
 * This file is part of OsirisFilter <https://github.com/aplyusnin/OsirisFilter>.
 * Copyright (C) 2024 Artem Plyusnin.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef OSIRIS_UTILS_H
#define OSIRIS_UTILS_H

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
// utility MACRO

#ifdef OSIRIS_ENABLE_DEBUG
#include <iostream>
template<typename ...Args>
void debugPrint(Args ...args)
{
    std::cout << '\n';
	(std::cout << ... << args);
	std::cout << '\n';
}
#define OSIRIS_DEBUG_PRINT(...) debugPrint(__VA_ARGS__)
#else
#define OSIRIS_DEBUG_PRINT(...) ((void)0)
#endif

#define UPDATE_HASH(hash, seed, bit, hash_id, h1, h2) { \
		(h1) = nextHash((seed), (hash_id)++); \
        (h1) = nextHash((h1), (hash_id)++); \
		(h2) = nextHash((h1), (hash_id)++); \
		(hash) ^= (bit) ? (h2) : (h1); \
		(seed) = (h2); \
	}

#define NEXT_BIT_IN_LOOP(key, pos, byte, bit, length) {\
			if ((bit) == 1) { \
				(pos)++; \
				(bit) = 128; \
				if ((pos) == (length)) break; \
				(byte) = (key)[(pos)]; \
			} else { \
				(bit) >>= 1; \
			} \
		}

#define NEXT_BITS_IN_LOOP(left_key, left_byte, right_key, right_byte, pos, bit, length) {\
			if ((bit) == 1) { \
				(pos)++; \
				(bit) = 128; \
				if ((pos) == (length)) break; \
				(left_byte) = (left_key)[(pos)]; \
				(right_byte) = (right_key)[(pos)]; \
			} else { \
				(bit) >>= 1; \
			} \
		}

namespace osiris
{
    // precompute reverse bit order
    const static uint8_t rev_bit_order[256 + 128] =
            {
                    0x1, 0x81, 0x41, 0xc1, 0x21, 0xa1, 0x61, 0xe1, 0x11, 0x91, 0x51, 0xd1, 0x31, 0xb1, 0x71, 0xf1,
                    0x9, 0x89, 0x49, 0xc9, 0x29, 0xa9, 0x69, 0xe9, 0x19, 0x99, 0x59, 0xd9, 0x39, 0xb9, 0x79, 0xf9,
                    0x5, 0x85, 0x45, 0xc5, 0x25, 0xa5, 0x65, 0xe5, 0x15, 0x95, 0x55, 0xd5, 0x35, 0xb5, 0x75, 0xf5,
                    0xd, 0x8d, 0x4d, 0xcd, 0x2d, 0xad, 0x6d, 0xed, 0x1d, 0x9d, 0x5d, 0xdd, 0x3d, 0xbd, 0x7d, 0xfd,
                    0x3, 0x83, 0x43, 0xc3, 0x23, 0xa3, 0x63, 0xe3, 0x13, 0x93, 0x53, 0xd3, 0x33, 0xb3, 0x73, 0xf3,
                    0xb, 0x8b, 0x4b, 0xcb, 0x2b, 0xab, 0x6b, 0xeb, 0x1b, 0x9b, 0x5b, 0xdb, 0x3b, 0xbb, 0x7b, 0xfb,
                    0x7, 0x87, 0x47, 0xc7, 0x27, 0xa7, 0x67, 0xe7, 0x17, 0x97, 0x57, 0xd7, 0x37, 0xb7, 0x77, 0xf7,
                    0xf, 0x8f, 0x4f, 0xcf, 0x2f, 0xaf, 0x6f, 0xef, 0x1f, 0x9f, 0x5f, 0xdf, 0x3f, 0xbf, 0x7f, 0xff,
                    0x0, 0x80, 0x40, 0xc0, 0x20, 0xa0, 0x60, 0xe0, 0x10, 0x90, 0x50, 0xd0, 0x30, 0xb0, 0x70, 0xf0,
                    0x8, 0x88, 0x48, 0xc8, 0x28, 0xa8, 0x68, 0xe8, 0x18, 0x98, 0x58, 0xd8, 0x38, 0xb8, 0x78, 0xf8,
                    0x4, 0x84, 0x44, 0xc4, 0x24, 0xa4, 0x64, 0xe4, 0x14, 0x94, 0x54, 0xd4, 0x34, 0xb4, 0x74, 0xf4,
                    0xc, 0x8c, 0x4c, 0xcc, 0x2c, 0xac, 0x6c, 0xec, 0x1c, 0x9c, 0x5c, 0xdc, 0x3c, 0xbc, 0x7c, 0xfc,
                    0x2, 0x82, 0x42, 0xc2, 0x22, 0xa2, 0x62, 0xe2, 0x12, 0x92, 0x52, 0xd2, 0x32, 0xb2, 0x72, 0xf2,
                    0xa, 0x8a, 0x4a, 0xca, 0x2a, 0xaa, 0x6a, 0xea, 0x1a, 0x9a, 0x5a, 0xda, 0x3a, 0xba, 0x7a, 0xfa,
                    0x6, 0x86, 0x46, 0xc6, 0x26, 0xa6, 0x66, 0xe6, 0x16, 0x96, 0x56, 0xd6, 0x36, 0xb6, 0x76, 0xf6,
                    0xe, 0x8e, 0x4e, 0xce, 0x2e, 0xae, 0x6e, 0xee, 0x1e, 0x9e, 0x5e, 0xde, 0x3e, 0xbe, 0x7e, 0xfe,
                    0x1, 0x81, 0x41, 0xc1, 0x21, 0xa1, 0x61, 0xe1, 0x11, 0x91, 0x51, 0xd1, 0x31, 0xb1, 0x71, 0xf1,
                    0x9, 0x89, 0x49, 0xc9, 0x29, 0xa9, 0x69, 0xe9, 0x19, 0x99, 0x59, 0xd9, 0x39, 0xb9, 0x79, 0xf9,
                    0x5, 0x85, 0x45, 0xc5, 0x25, 0xa5, 0x65, 0xe5, 0x15, 0x95, 0x55, 0xd5, 0x35, 0xb5, 0x75, 0xf5,
                    0xd, 0x8d, 0x4d, 0xcd, 0x2d, 0xad, 0x6d, 0xed, 0x1d, 0x9d, 0x5d, 0xdd, 0x3d, 0xbd, 0x7d, 0xfd,
                    0x3, 0x83, 0x43, 0xc3, 0x23, 0xa3, 0x63, 0xe3, 0x13, 0x93, 0x53, 0xd3, 0x33, 0xb3, 0x73, 0xf3,
                    0xb, 0x8b, 0x4b, 0xcb, 0x2b, 0xab, 0x6b, 0xeb, 0x1b, 0x9b, 0x5b, 0xdb, 0x3b, 0xbb, 0x7b, 0xfb,
                    0x7, 0x87, 0x47, 0xc7, 0x27, 0xa7, 0x67, 0xe7, 0x17, 0x97, 0x57, 0xd7, 0x37, 0xb7, 0x77, 0xf7,
                    0xf, 0x8f, 0x4f, 0xcf, 0x2f, 0xaf, 0x6f, 0xef, 0x1f, 0x9f, 0x5f, 0xdf, 0x3f, 0xbf, 0x7f, 0xff
            };

    const static uint8_t* rev_bit = rev_bit_order + 128;

    // most significant bit in a byte
    const static int8_t msb[256] =
            {
                    -1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
                    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
                    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
                    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
            };

    // least significant bit in a byte
    const static int8_t lsb[256] =
            {
                    -1, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
            };

    typedef uint64_t hash_t;

    struct location_t
    {
        uint32_t position[4] = { 0, 0, 0, 0 };
        uint32_t first_bucket = 0;
    };

    inline size_t split(const std::vector<std::string>& data, size_t pos, size_t l, size_t r)
    {
        size_t byte = pos >> 3;
        size_t bit = 7 - (pos & 7);
        size_t l1 = l, r1 = r;
        while (r1 - l1 > 1)
        {
            size_t m = l1 + (r1 - l1) / 2;
            if ((data[m][byte] >> bit) & 1)
            {
                r1 = m;
            }
            else
            {
                l1 = m;
            }
        }
        return l1;
    }

    inline size_t findCommonPrefix(const std::string& l, const std::string& r, size_t pos)
    {
        size_t byte = pos >> 3;
        while (byte < l.size())
        {
            uint8_t res = rev_bit[l[byte]] ^ rev_bit[r[byte]];
            if (res != 0) return byte * 8 + lsb[res];
            byte++;
        }
        return l.size() * 8;
    }

    inline size_t getSize(size_t len)
    {
        size_t bits = 8;

        while ((1ull << bits) <= len)
        {
            bits += 8;
        }

        return bits;
    }

    // in practice, number of buckets rarely exceeds 550 and never exceeds 600
    inline size_t cntB[1000];
    inline size_t totB[1000];

    inline void radixSortBucket(std::pair<location_t, uint8_t*>* loc, size_t n)
    {
        auto* buffer = new std::pair<location_t, uint8_t*>[n];
        memset(cntB, 0, sizeof(cntB));
        memset(totB, 0, sizeof(totB));
        for (size_t i = 0; i < n; ++i)
        {
            cntB[loc[i].first.first_bucket] ++;
        }

        for (size_t i = 1; i < 1000; ++i)
        {
            totB[i] = totB[i - 1] + cntB[i - 1];
        }
        for (size_t i = 0; i < n; ++i)
        {
            buffer[totB[loc[i].first.first_bucket]++] = std::move(loc[i]);
        }
        for (size_t i = 0; i < n; ++i)
        {
            loc[i] = std::move(buffer[i]);
        }
        delete[] buffer;
    }

}
#endif