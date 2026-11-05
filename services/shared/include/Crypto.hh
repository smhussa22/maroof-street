#ifndef CRYPTO_HH
#define CRYPTO_HH

#include <openssl/evp.h>
#include <stdexcept>
#include <string>

// returns the SHA-256 hex digest of the input string
inline std::string sha256_hex(const std::string& input)
{
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int len = 0;

    if (EVP_Digest(input.data(), input.size(), digest, &len, EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("sha256 failed");

    static const char hex[] = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (unsigned int i = 0; i < len; ++i)
    {
        out += hex[digest[i] >> 4];
        out += hex[digest[i] & 0x0F];
    }
    return out;
}

#endif // CRYPTO_HH
