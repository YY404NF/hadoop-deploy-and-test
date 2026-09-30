#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef __APPLE__
#include <Accelerate/Accelerate.h>
#else
#include <cblas.h>
#endif

// 解码 Python 生成的 Base64 分块数据
static std::vector<unsigned char> decode64(const std::string& text) {
    static const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::vector<unsigned char> bytes;
    int value = 0;
    int bits = -8;
    for (unsigned char c : text) {
        if (c == '=') break;
        value = (value << 6) + static_cast<int>(chars.find(c));
        bits += 6;
        if (bits >= 0) {
            bytes.push_back(static_cast<unsigned char>((value >> bits) & 255));
            bits -= 8;
        }
    }
    return bytes;
}

// 在本机计算参考矩阵，仅用于校验 Hadoop 输出，不记录本地耗时
static int verify_run(const char* input_path, const char* output_path, int expected_n) {
    std::ifstream input(input_path, std::ios::binary);
    int32_t n = 0;
    input.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!input || n <= 0 || n != expected_n) throw std::runtime_error("矩阵文件头无效或规格不符");

    const int cells = n * n;
    std::vector<float> a(cells), b(cells), c(cells);
    input.read(reinterpret_cast<char*>(a.data()), cells * sizeof(float));
    input.read(reinterpret_cast<char*>(b.data()), cells * sizeof(float));
    if (!input) throw std::runtime_error("矩阵文件读取失败");
    cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, n, n, n,
                1.0f, a.data(), n, b.data(), n, 0.0f, c.data(), n);

    std::ofstream output(output_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(&n), sizeof(n));
    output.write(reinterpret_cast<const char*>(c.data()), cells * sizeof(float));
    if (!output) throw std::runtime_error("参考矩阵写入失败");
    return 0;
}

// 在云端比较已上传的本地参考矩阵和 Hadoop 输出，允许浮点运算误差
static int compare_run(const char* reference_path, const char* result_path, int n, int block) {
    std::ifstream reference(reference_path, std::ios::binary);
    int32_t reference_n = 0;
    reference.read(reinterpret_cast<char*>(&reference_n), sizeof(reference_n));
    if (!reference || reference_n != n) throw std::runtime_error("参考矩阵规格无效");
    std::vector<float> expected(n * n);
    reference.read(reinterpret_cast<char*>(expected.data()), expected.size() * sizeof(float));
    if (!reference) throw std::runtime_error("参考矩阵数据不完整");

    std::vector<float> actual(n * n, 0.0f);
    std::vector<bool> seen((n + block - 1) / block * ((n + block - 1) / block), false);
    std::ifstream result(result_path);
    std::string line;
    while (std::getline(result, line)) {
        const auto tab = line.find('\t');
        if (tab == std::string::npos) throw std::runtime_error("Hadoop 输出记录格式无效");
        std::istringstream key(line.substr(0, tab));
        int block_row, block_col;
        char comma;
        if (!(key >> block_row >> comma >> block_col) || comma != ',') {
            throw std::runtime_error("Hadoop 输出块坐标无效");
        }
        const int blocks_per_row = (n + block - 1) / block;
        if (block_row < 0 || block_row >= blocks_per_row || block_col < 0 || block_col >= blocks_per_row) {
            throw std::runtime_error("Hadoop 输出块坐标越界");
        }
        seen[block_row * blocks_per_row + block_col] = true;
        std::istringstream values(line.substr(tab + 1));
        for (int row = 0; row < block; ++row) {
            for (int col = 0; col < block; ++col) {
                float value;
                if (!(values >> value)) throw std::runtime_error("Hadoop 输出块数据不完整");
                const int r = block_row * block + row;
                const int c = block_col * block + col;
                if (r < n && c < n) actual[r * n + c] = value;
            }
        }
    }

    if (!std::all_of(seen.begin(), seen.end(), [](bool value) { return value; })) {
        throw std::runtime_error("Hadoop 输出缺少矩阵块");
    }
    double maximum_error = 0.0;
    for (size_t i = 0; i < expected.size(); ++i) {
        const double difference = std::abs(static_cast<double>(expected[i]) - actual[i]);
        maximum_error = std::max(maximum_error, difference);
        if (difference > 2e-4 + 2e-4 * std::abs(expected[i])) {
            throw std::runtime_error("Hadoop 结果超出浮点容差");
        }
    }
    std::cout << "MAX_ABS_ERROR=" << std::setprecision(10) << maximum_error << '\n';
    return 0;
}

// 对一个输入记录中的 A、B 矩阵块做乘法并输出部分积
static int map_run(int block) {
    std::string line;
    std::cout << std::setprecision(9);
    while (std::getline(std::cin, line)) {
        const auto tab = line.find('\t');
        std::istringstream header(line.substr(0, tab));
        int i, j, k;
        if (!(header >> i >> j >> k) || tab == std::string::npos) {
            throw std::runtime_error("输入记录格式无效");
        }

        auto bytes = decode64(line.substr(tab + 1));
        const int cells = block * block;
        if (bytes.size() != static_cast<size_t>(2 * cells * sizeof(float))) {
            throw std::runtime_error("分块长度无效");
        }
        std::vector<float> a(cells), b(cells), c(cells, 0.0f);
        std::memcpy(a.data(), bytes.data(), cells * sizeof(float));
        std::memcpy(b.data(), bytes.data() + cells * sizeof(float), cells * sizeof(float));
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, block, block, block,
                    1.0f, a.data(), block, b.data(), block, 0.0f, c.data(), block);

        std::cout << i << ',' << j << '\t';
        for (int x = 0; x < cells; ++x) std::cout << (x ? " " : "") << c[x];
        std::cout << '\n';
    }
    return 0;
}

// 将同一输出块对应的各个部分积累加
static int reduce_run(int block) {
    std::string line;
    std::string current_key;
    std::vector<double> sum(block * block, 0.0);
    auto flush = [&]() {
        if (current_key.empty()) return;
        std::cout << current_key << '\t' << std::setprecision(12);
        for (int x = 0; x < block * block; ++x) std::cout << (x ? " " : "") << sum[x];
        std::cout << '\n';
    };

    while (std::getline(std::cin, line)) {
        const auto tab = line.find('\t');
        if (tab == std::string::npos) throw std::runtime_error("Reducer 输入记录格式无效");
        const std::string key = line.substr(0, tab);
        if (key != current_key) {
            flush();
            current_key = key;
            std::fill(sum.begin(), sum.end(), 0.0);
        }
        std::istringstream values(line.substr(tab + 1));
        for (double& total : sum) {
            double value;
            if (!(values >> value)) throw std::runtime_error("Reducer 分块数据不完整");
            total += value;
        }
    }
    flush();
    return 0;
}

int main(int argc, char** argv) {
    try {
        if (argc == 5 && std::string(argv[1]) == "verify") {
            return verify_run(argv[2], argv[3], std::stoi(argv[4]));
        }
        if (argc == 6 && std::string(argv[1]) == "compare") {
            return compare_run(argv[2], argv[3], std::stoi(argv[4]), std::stoi(argv[5]));
        }
        if (argc == 3 && std::string(argv[1]) == "map") return map_run(std::stoi(argv[2]));
        if (argc == 3 && std::string(argv[1]) == "reduce") return reduce_run(std::stoi(argv[2]));
        throw std::runtime_error("参数应为 verify 输入 输出 规格、compare 参考结果 Hadoop结果 规格 分块大小，或 map/reduce 分块大小");
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
