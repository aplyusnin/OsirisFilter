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
#include <random>
#include <iostream>

using namespace std;
using namespace osiris;

void benchFixedCorrect(const string& name) {
    vector<size_t> sizes = { 1000000 };

    vector<testResult> results;
    std::cout << "Testing TP point queries" << std::endl;
    for (auto x : sizes) {
        testDataPoint test = preparePointTest(x, 8, 8, 0, 0);
        results.emplace_back(evaluatePoint(&test, 1, "0", name));
    }
    std::cout << "Testing FN point queries" << std::endl;
    for (auto x : sizes)
    {
        testDataPoint test = preparePointTest(x, 8, 8, 0, 1);
        results.emplace_back(evaluatePoint(&test, 1, "1", name));
    }
    std::cout << "Testing random point queries" << std::endl;
    for (auto x : sizes)
    {
        testDataPoint test = preparePointTest(x, 8, 8, 0, 2);
        results.emplace_back(evaluatePoint(&test, 1, "2", name, false));
    }
    std::cout << "Testing TP range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 8, 8, 0, 0);
        results.emplace_back(evaluateRange(&test, 1, "3", name));
    }
    std::cout << "Testing FN range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 8, 8, 0, 1);
        results.emplace_back(evaluateRange(&test, 1, "4", name));
    }
    std::cout << "Testing random range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 8, 8, 0, 2);
        results.emplace_back(evaluateRange(&test, 1, "5", name, false));
    }
    saveQueryReport(results, "fixed");
    saveBuildReport(results, "fixed");
    saveSerialReport(results, "fixed");
}

void benchNoPrefixCorrect(const string& name) {
    vector<size_t> sizes = { 1000000 };
    vector<testResult> results;
    std::cout << "Testing TP point queries" << std::endl;
    for (auto x : sizes) {
        testDataPoint test = preparePointTest(x, 32, 64, 1, 0);
        results.emplace_back(evaluatePoint(&test, 1, "0", name));
    }
    std::cout << "Testing FN point queries" << std::endl;
    for (auto x : sizes)
    {
        testDataPoint test = preparePointTest(x, 32, 64, 1, 1);
        results.emplace_back(evaluatePoint(&test, 1, "1", name));
    }
    std::cout << "Testing random point queries" << std::endl;
    for (auto x : sizes)
    {
        testDataPoint test = preparePointTest(x, 32, 64, 1, 2);
        results.emplace_back(evaluatePoint(&test, 1, "2", name, false));
    }
    std::cout << "Testing TP range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 32, 64, 1, 0);
        results.emplace_back(evaluateRange(&test, 1, "3", name));
    }
    std::cout << "Testing FN range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 32, 64, 1, 1);
        results.emplace_back(evaluateRange(&test, 1, "4", name));
    }
    std::cout << "Testing random range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 32, 64, 0, 2);
        results.emplace_back(evaluateRange(&test, 1, "5", name, false));
    }
    saveQueryReport(results, "no_prefix");
    saveBuildReport(results, "no_prefix");
    saveSerialReport(results, "no_prefix");
}


void benchCommonCorrect(const string& name) {
    vector<size_t> sizes = { 1000000 };
    vector<testResult> results;
    std::cout << "Testing TP point queries" << std::endl;
    for (auto x : sizes) {
        testDataPoint test = preparePointTest(x, 32, 64, 2, 0);
        results.emplace_back(evaluatePoint(&test, 1, "0", name));
    }
    std::cout << "Testing FN point queries" << std::endl;
    for (auto x : sizes)
    {
        testDataPoint test = preparePointTest(x, 32, 64, 2, 1);
        results.emplace_back(evaluatePoint(&test, 1, "1", name));
    }
    std::cout << "Testing random point queries" << std::endl;
    for (auto x : sizes)
    {
        testDataPoint test = preparePointTest(x, 32, 64, 2, 2);
        results.emplace_back(evaluatePoint(&test, 1, "2", name, false));
    }
    std::cout << "Testing TP range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 32, 64, 2, 0);
        results.emplace_back(evaluateRange(&test, 1, "3", name));
    }
    std::cout << "Testing FN range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 32, 64, 2, 1);
        results.emplace_back(evaluateRange(&test, 1, "4", name));
    }
    std::cout << "Testing random range queries" << std::endl;
    for (auto x : sizes)
    {
        testDataRange test = prepareRangeTest(x, 32, 64, 2, 2);
        results.emplace_back(evaluateRange(&test, 1, "5", name, false));
    }
    saveQueryReport(results, "common");
    saveBuildReport(results, "common");
    saveSerialReport(results, "common");
}

int main() {

    benchFixedCorrect("fixed");
    benchNoPrefixCorrect("no_prefix");
    benchCommonCorrect("common");
    return 0;
}