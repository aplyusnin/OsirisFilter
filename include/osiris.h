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

#ifndef OSIRIS_H
#define OSIRIS_H

#include "fixed_filter.h"
#include "no_prefix_filter.h"
#include "common_filter.h"

namespace osiris
{

    inline OsirisFilter* build(const std::vector<std::string>& keys)
	{
		KeySetInfo info = check(keys);
		switch (info.type)
		{
		case 0:
			return new FixedLengthFilter(keys, info);
		case 1:
			return new NoPrefixFilter(keys, info);
		case 2:
			return new CommonFilter(keys, info);
		default:
			return nullptr;
			break;
		}
	}

    inline OsirisFilter* deserialize(uint8_t* buffer)
	{
		switch (buffer[0])
		{
		case 1:
			return new FixedLengthFilter(buffer + 1);
		case 2:
			return new NoPrefixFilter(buffer + 1);
		case 3:
			return new CommonFilter(buffer + 1);
		default:
			return nullptr;
		}
	}
}

#endif