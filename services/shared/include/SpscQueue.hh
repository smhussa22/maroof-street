#ifndef SPSC_QUEUE_HH
#define SPSC_QUEUE_HH

#include <string>
#include <new>
#include <atomic>
#include <array>
#include <optional>
#include <type_traits>
#include <utility>

template <typename T, size_t N>
class SpscQueue 
{

    static_assert(N >= 2, "SpscQueue: capacity must be at least 2");
    static_assert(std::is_default_constructible_v<T>, "SpscQueue: T must be default constructible");
    static_assert(std::is_move_constructible_v<T>, "SpscQueue: T must be move constructible");
    static_assert((N & (N - 1)) == 0, "SpscQueue: N must be a power of two");

public:

    SpscQueue() = default;
    ~SpscQueue() = default;

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    // producer only ; returns true if successful, returns false if the queue is full
    template <typename Val_T> requires std::is_assignable_v<T&, Val_T&&>
    [[nodiscard]] bool try_push(Val_T&& element) noexcept (std::is_nothrow_assignable_v<T&, Val_T&&>);

    // consumer only; returns nullopt if the queue is emptry
    std::optional<T> try_pop();

private:

    static constexpr size_t cache_line { std::hardware_destructive_interference_size };
    alignas(cache_line) std::atomic<size_t> m_head {}; // total pushes ever completed (producer writes)
    alignas(cache_line) std::atomic<size_t> m_tail {}; // total pops ever completed (consumer writes)
    alignas(cache_line) std::array<T, N> m_buffer {};

};

template <typename T, size_t N>
template <typename Val_T> requires std::is_assignable_v<T&, Val_T&&>
inline bool SpscQueue<T, N>::try_push(Val_T&& element) noexcept (std::is_nothrow_assignable_v<T&, Val_T&&>)
{

    const auto write_index { m_head.load(std::memory_order_relaxed) };
    const auto read_index { m_tail.load(std::memory_order_acquire) };
    
    // cant push; queue is full.
    if (write_index - read_index == N)
    {

        return false;

    }

    m_buffer[write_index & (N - 1)] = std::forward<Val_T>(element);
    m_head.store(write_index + 1, std::memory_order_release);

    return true;

}

template <typename T, size_t N>
inline std::optional<T> SpscQueue<T, N>::try_pop()
{

    const auto write_index { m_head.load(std::memory_order_acquire) };
    const auto read_index { m_tail.load(std::memory_order_relaxed) };

    if (write_index == read_index)
    {

        return std::nullopt;

    }

    std::optional<T> popped { std::move(m_buffer[read_index & (N - 1)]) };
    m_tail.store(read_index + 1, std::memory_order_release);
    return popped;

}

#endif // SPSC_QUEUE_HH