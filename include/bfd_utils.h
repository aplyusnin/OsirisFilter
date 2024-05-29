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

#ifndef OSIRIS_BFD_UTILS_H
#define OSIRIS_BFD_UTILS_H

#include "bitstring.h"

namespace osiris
{
	struct DataLayout
	{
		// length in bytes
		uint64_t total_size_in_bytes = 0;

		// segment length
		uint64_t segment_length = 0;
		// mask for the segment
		uint64_t segment_mask = 0;

		// number of segments, where the first blocks can be located
		uint64_t segments_count = 0;

		// total number of segments
		uint64_t total_segments = 0;

		// total number of pages
		uint64_t total_pages = 0;

		// number of keys in the dictionary
		uint32_t keys = 0;

		uint32_t len_in_bits = 0;
		uint32_t len_in_bytes = 0;

		// binary log of the segment length
		uint32_t segment_length_log = 0;
	};

	inline DataLayout prepareLayout(size_t keys, size_t length_in_bits)
	{
		DataLayout result;

		result.keys = keys;

		result.segment_length_log = calculateSegmentLengthLog(keys);
		result.segment_length = 1LL << result.segment_length_log;

		result.segment_mask = result.segment_length - 1;

		double sizeFactor = calculateSizeFactor(keys);
		OSIRIS_DEBUG_PRINT("length factor for ", keys, " keys is ", sizeFactor);
		auto capacity = (size_t)((double)keys * sizeFactor);

		// capacity rounded up to the closest
		size_t segmentCount = std::max(4ull, (capacity + result.segment_length - 1) / result.segment_length);

		result.segments_count = segmentCount - 3;
		result.total_segments = segmentCount;
		result.total_pages = result.total_segments * result.segment_length;
		OSIRIS_DEBUG_PRINT("Total segments: ", result.total_segments, ", number of pages: ", result.total_pages);

		result.len_in_bits = length_in_bits;
        result.len_in_bytes = BITS_TO_BYTES(length_in_bits);

		result.total_size_in_bytes = BITS_TO_BYTES(length_in_bits * result.total_pages);

		return result;
	}

	inline location_t hashToLocation(hash_t hash, DataLayout& layout)
	{
		location_t res;
		size_t lengthLog = layout.segment_length_log;
		size_t segmentMask = layout.segment_mask;
		res.first_bucket = (hash >> lengthLog) % layout.segments_count;

		size_t offset = ((size_t)res.first_bucket) << lengthLog;

		res.position[0] = (hash & segmentMask) + offset;
		res.position[1] = (rotl64(hash, (uint32_t)(lengthLog)) & segmentMask) + (1ULL << lengthLog) + offset;
		res.position[2] = (rotl64(hash, (uint32_t)(2ull * lengthLog)) & segmentMask) + (2ULL << lengthLog) + offset;
		res.position[3] = (rotl64(hash, (uint32_t)(3ull * lengthLog)) & segmentMask) + (3ULL << lengthLog) + offset;

		return res;
	}

	inline bool peel(
		std::pair<location_t, uint8_t*>* info, size_t size,
		size_t* pos, size_t* id, size_t* sets, size_t* st, DataLayout& data_layout)
	{
		size_t p = 0;

		// sort goes here
		radixSortBucket(info, size);

		/*std::sort(info, info + size, [](std::pair<location_t, uint8_t*>& l, std::pair<location_t, uint8_t*>& r)
		{
				return l.first.first_bucket < r.first.first_bucket;
		});*/

		size_t q = 0;

		for (size_t it = 0; it < size; ++it)
		{
			for (size_t i : info[it].first.position)
			{
				sets[i << 1]++;
				sets[i << 1 | 1] ^= it;
			}
		}

		for (size_t i = 0; i < data_layout.total_pages; i++)
		{
			if (sets[i << 1] == 1)
			{
				st[q] = i;
				q++;
			}
		}

		while (q > 0)
		{
			q--;
			size_t u = st[q];
			size_t v = sets[u << 1 | 1];
			if (sets[u << 1] == 0)
			{
				continue;
			}

			pos[p] = u;
			id[p] = v;
			p++;

			for (size_t it : info[v].first.position)
			{
				sets[it << 1]--;
				sets[it << 1 | 1] ^= v;
				if (sets[it << 1] == 1)
				{
					st[q] = it;
					q++;
				}
			}
		}

		return p == size;
	}
}

#endif