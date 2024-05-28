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

#ifndef OSIRIS_MATH_UTILS_H
#define OSIRIS_MATH_UTILS_H

#include <algorithm>
#include <cstdint>
#include <cmath>

#include "utils.h"

#define BITS_TO_BYTES(x) (((x) + 7) >> 3)

namespace osiris
{

	inline uint64_t nextRand(uint64_t value)
	{
		value ^= value << 13;
		value ^= value >> 7;
		value ^= value << 17;
		return value;
	}

	inline size_t calculateSegmentLengthLog(size_t size)
	{
		if (size == 1) return 1;
		return std::max(1, (int)floor(log((double)size) / log(2.91) - 0.5));
	}

	inline double calculateSizeFactor(size_t size) {
		if (size <= 2) { size = 2; }
		return fmax(1.075, 0.77 + 0.305 * log(600000.0) / log((double)size));
	}
	
	inline uint64_t rotl64(uint64_t n, unsigned int offset)
	{
		return (n << offset) | (n >> (64 - offset));
	}

}

#endif