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

#include "bench_utils.h"
#include <chrono>
#include <fstream>
#include <utility>
#include <iostream>

std::string generate(size_t minLen, size_t maxLen)
{
    size_t len = rndNext(minLen, maxLen);
    std::string res = std::string(len, '0');
    for (size_t i = 0; i < len; i++)
    {
        res[i] = (char) rndNext(0, 255);
    }
    return res;
}

osiris::OsirisFilter* buildAndSerial(std::vector<std::string>* data, size_t repeat, testResult* result)
{
    osiris::OsirisFilter* filter = nullptr;
    for (size_t i = 0; i < repeat; i++)
    {
        if (i)
        {
            delete filter;
        }
        auto buildStart = std::chrono::high_resolution_clock::now();
        filter = osiris::build(*data);
        auto buildEnd = std::chrono::high_resolution_clock::now();
        result->buildTime.push_back((buildEnd - buildStart).count());
    }
    for (size_t i = 0; i < repeat; i++)
    {
        auto serialStart = std::chrono::high_resolution_clock::now();
        auto res = filter->serialize();
        auto serialEnd = std::chrono::high_resolution_clock::now();
        std::cout << res.second * 1.0 / (data->size()) << std::endl;

        result->serializationTime.push_back((serialEnd - serialStart).count());

        auto deserializeStart = std::chrono::high_resolution_clock::now();
        auto kek = osiris::deserialize(res.first);
        auto deserializeEnd = std::chrono::high_resolution_clock::now();

        result->deserializationTime.push_back((deserializeEnd - deserializeStart).count());
        delete kek;
        delete res.first;
    }
    return filter;
}

testDataPoint preparePointTest(size_t size, size_t minLen, size_t maxLen, int setType, int queryType)
{
    testDataPoint testData;

    std::set<std::string, cmp> res = generateData(size, minLen, maxLen, setType);

    testData.data = std::vector<std::string>(res.begin(), res.end());

    if (queryType == 0)
    {
        for (size_t i = 0; i < 5'000'0; i++)
        {
            testData.pointQueries.push_back(testData.data[rndNext(0, size - 1)]);
            testData.pointResult.push_back(true);
        }
    }
    else if (queryType == 1)
    {
        for (size_t i = 0; i < 5'000'0; i++)
        {
            while (true)
            {
                std::string q = generate(minLen, maxLen);
                if (!res.count(q))
                {
                    testData.pointQueries.push_back(q);
                    testData.pointResult.push_back(false);
                    break;
                }
            }
        }
    }
    else
    {
        for (size_t i = 0; i < 5'000'0; i++)
        {
            std::string q = generate(minLen, maxLen);
            testData.pointQueries.push_back(q);
            testData.pointResult.push_back(res.count(q));
        }
    }
    return testData;
}

testDataRange prepareRangeTest(size_t size, size_t minLen, size_t maxLen, int setType, int queryType)
{
    testDataRange testData;

    std::set<std::string, cmp> res = generateData(size, minLen, maxLen, setType);

    testData.data = std::vector<std::string>(res.begin(), res.end());

    if (queryType == 0)
    {
        for (size_t i = 0; i < 5'000'0; i++)
        {
            rangeQuery q;
            size_t l = rndNext(0, size - 1);
            size_t r = rndNext(0, size - 1);
            while (l == r)
            {
                l = rndNext(0, size - 1);
                r = rndNext(0, size - 1);
            }
            if (r < l) std::swap(l, r);
            q.left = testData.data[l];
            q.right = testData.data[r];
            if (rndNext(0, 1))
            {
                q.includeRight = true;
            }
            else
            {
                q.includeLeft = true;
            }
            q.expectedResult = true;
            testData.rangeQueries.push_back(q);
        }
    }
    else if (queryType == 1)
    {
        for (size_t i = 0; i < 5'000'0; i++)
        {
            size_t id = rndNext(0, size - 2);
            rangeQuery q;
            q.left = testData.data[id];
            q.right = testData.data[id + 1];
            testData.rangeQueries.push_back(q);
        }
    }
    else
    {
        cmp c;
        for (size_t i = 0; i < 5'000'0; i++)
        {
            rangeQuery q;
            q.left = generate(0, 2 * maxLen);
            q.right = generate(0, 2 * maxLen);
            if (i & 1)
            {
                q.includeRight = true;
            }
            else
            {
                q.includeLeft = true;
            }
            if (!c(q.left, q.right)) std::swap(q.left, q.right);
            testData.rangeQueries.push_back(q);
        }
    }
    return testData;
}


bool isPrefix(const std::string& a, const std::string& b)
{
    for (size_t i = 0; i < std::min(a.size(), b.size()); i++)
    {
        if (a[i] != b[i]) return false;
    }
    return true;
}

std::set<std::string, cmp> generateData(size_t size, size_t minLen, size_t maxLen, int setType)
{
    std::set<std::string, cmp> res;
    if (setType == 0)
    {
        while (res.size() < size)
        {
            std::string kek = generate(minLen, maxLen);
            if (!res.count(kek))
            {
                res.insert(kek);
            }
        }
        return res;
    }
    if (setType == 1)
    {
        while (res.size() < size)
        {
            std::string kek = generate(minLen, maxLen);
            auto x = res.lower_bound(kek);
            if (x == res.end() || !isPrefix(kek, *x))
            {
                res.insert(kek);
            }
        }
        return res;
    }
    while (res.size() + 1 < size)
    {
        std::string kek = generate(minLen, maxLen);
        if (!res.count(kek))
        {
            res.insert(kek);
        }
    }
    for (auto x : res)
    {
        x += (char) rndNext(0, 255);
        if (!res.count(x))
        {
            res.insert(x);
            break;
        }
    }
    return res;
}

testResult evaluatePoint(testDataPoint* testData, size_t repeat, std::string id, std::string name, bool verify) {
    testResult result;
    result.id = std::move(id);
    result.name = std::move(name);
    result.keysNum = testData->data.size();

    osiris::OsirisFilter* filter = buildAndSerial(&testData->data, repeat, &result);

    for (size_t i = 0; i < testData->pointQueries.size(); i++)
    {
        auto queryStart = std::chrono::high_resolution_clock::now();
        auto kek = filter->pointQuery(testData->pointQueries[i]);
        auto queryEnd = std::chrono::high_resolution_clock::now();

        result.queryTime.push_back((queryEnd - queryStart).count());
        if (verify)
        {
            result.success += kek == testData->pointResult[i];
            if (kek != testData->pointResult[i])
            {
                auto kek1 = filter->pointQuery(testData->pointQueries[i]);
                std::cout << kek1;
                filter->pointQuery(testData->pointQueries[i]);
            }
            //assert(kek == testData->pointResult[i]);
        }
    }

    delete filter;

    return result;
}

testResult evaluateRange(testDataRange* testData, size_t repeat, std::string id, std::string name, bool verify) {
    testResult result;
    result.id = std::move(id);
    result.name = std::move(name);
    result.keysNum = testData->data.size();

    osiris::OsirisFilter* filter = buildAndSerial(&testData->data, repeat, &result);

    for (auto& x : testData->rangeQueries)
    {
        auto queryStart = std::chrono::high_resolution_clock::now();
        auto kek = filter->rangeQuery(x.left, x.includeLeft, x.right, x.includeRight);
        auto queryEnd = std::chrono::high_resolution_clock::now();

        result.queryTime.push_back((queryEnd - queryStart).count());
        if (verify)
        {
            result.success += kek == x.expectedResult;
            assert(kek == x.expectedResult);
        }
    }
    delete filter;

    return result;
}


void saveQueryReport(std::vector<testResult>& result, const std::string& filename)
{
    std::ofstream out(filename + "_queries" + ".csv");
    out << "Id,Number of Keys,Time\n";
    for (auto& x : result)
    {
        for (size_t i = 0; i < x.queryTime.size(); i++)
        {
            out << x.id << "," << x.keysNum << "," << x.queryTime[i] << "\n";
        }
    }
    out.close();
}

void saveBuildReport(std::vector<testResult>& result, const std::string& filename)
{
    std::ofstream out(filename + "_build" + ".csv");
    out << "Id,Number of Keys,Time\n";
    for (auto& x : result)
    {
        for (size_t i = 0; i < x.buildTime.size(); i++)
        {
            out << x.id << "," << x.keysNum << "," << x.buildTime[i] << "\n";
        }
    }
    out.close();
}

void saveSerialReport(std::vector<testResult>& result, const std::string& filename)
{
    std::ofstream out(filename + "_serial" + ".csv");
    out << "Id,Number of Keys,Serial Time,Deserialize Time\n";
    for (auto& x : result)
    {
        for (size_t i = 0; i < x.serializationTime.size(); i++)
        {
            out << x.id << "," << x.keysNum << "," << x.serializationTime[i] << "," << x.deserializationTime[i] << "\n";
        }
    }
    out.close();
}