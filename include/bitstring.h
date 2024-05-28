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

#ifndef OSIRIS_BITSTRING_H
#define OSIRIS_BITSTRING_H

#ifndef OSIRIS_HEAP_ALLOCATION_THRESHOLD
#define OSIRIS_HEAP_ALLOCATION_THRESHOLD 8
#endif

#include "math_utils.h"
#include <cstring>

namespace osiris
{
	struct bitstring
	{
	private:

		inline static uint8_t* buf = nullptr;

        inline static size_t buf_size = 0;
        inline static size_t next = 0;

	public:
		size_t begin = 0;
		size_t length = 0;
		uint8_t* start = nullptr;

		bitstring() = default;

		bitstring(size_t size, size_t beg)
		{
			this->length = size;
			this->begin = beg;
			if (length <= 8)
			{
				start = (uint8_t*)&this->begin;
			}
			else
			{
				start = buf + begin;
			}
		}

		bitstring(const bitstring& other)
		{
			this->length = other.length;
			this->begin = other.begin;

			if (length <= OSIRIS_HEAP_ALLOCATION_THRESHOLD)
			{
				start = (uint8_t*)&this->begin;
			}
			else
			{
				start = buf + begin;
			}
		}

		bitstring& operator=(const bitstring& other)
		{
			this->length = other.length;
			this->begin = other.begin;

			if (length <= OSIRIS_HEAP_ALLOCATION_THRESHOLD)
			{
				start = (uint8_t*)&this->begin;
			}
			else
			{
				start = buf + begin;
			}
			return *this;
		}

		inline bool operator[](size_t pos) const
		{
			return (start[pos >> 3] >> (pos & 7)) & 1;
		}

		inline void set(size_t pos, bool value) const
		{
			start[pos >> 3] = start[pos >> 3] | (value << (pos & 7));
		}

		inline void set(size_t pos, uint8_t value, size_t bits) const
		{
			// number of free bits in byte
			size_t b1 = 8 - (pos & 7);
			size_t id = pos >> 3;
			if (bits < b1)
			{
				start[id] = start[id] | (value << bits);
			}
			else
			{
				start[id] = start[id] | (value << b1);
				start[id + 1] = start[id + 1] | (value >> b1);
			}
		}

		inline void setByte(size_t pos, uint8_t value) const
		{
			if (pos & 7)
			{
				size_t r = pos & 7;
				size_t l = 8 - r;
				size_t p = pos >> 3;
				start[p++] |= value << r;
				start[p] = value >> l;
			}
			else
			{
				start[pos >> 3] = value;
			}
		}

		inline bool get(size_t pos)
		{
			return (start[pos >> 3] >> (pos & 7)) & 1;
		}

		inline size_t size() const
		{
			return length;
		}

		inline uint8_t* data() const
		{
			return start;
		}

		inline bool operator==(const bitstring& b) const
		{
			return length == b.length && begin == b.begin;
		}

		static void init(size_t size)
		{
			//delete[] buf;
			buf = new uint8_t[size];
			memset(buf, 0, size);
			buf_size = size;
		}


		static void clear()
		{
			delete[] buf;
			next = 0;
			buf_size = 0;
		}

		static bitstring heapString(size_t size)
		{
			size_t bytes = ((size + 7) >> 3);
			if (next + bytes > buf_size)
			{
				buf = expand(buf, buf_size);
			}

			next += bytes;
			return { size, next - bytes };
		}

		static bitstring stackString(size_t size)
		{
			return { size, 0 };
		}

		static bitstring select(size_t size)
		{
			if (size <= OSIRIS_HEAP_ALLOCATION_THRESHOLD) return stackString(size);
			return heapString(size);
		}

	private:
		static uint8_t* expand(uint8_t* ptr, size_t& old_size)
		{
			auto new_size = (size_t)((double)old_size * 1.5);
			auto* ptr1 = (uint8_t*)realloc(ptr, new_size);
			old_size = new_size;
			return ptr1;
		}
	};

};

#endif 
