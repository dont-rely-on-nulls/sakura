#include <iostream>
#include <openssl/sha.h>
#include <iomanip>
#include <sstream>
#include <cstring>

#include <sys/stat.h>

extern "C" {
#include "library.h"

    extern void compute_hash(char* computed_hash, const char *content) {
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256_CTX sha256;
        SHA256_Init(&sha256);
        SHA256_Update(&sha256, content, std::strlen(content));
        SHA256_Final(hash, &sha256);
        std::stringstream ss;
        for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
            ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
        std::memcpy(computed_hash, ss.str().c_str(), HASH_SIZE);
    }
}
