#ifndef KAFKA_PUBLISHER_HH
#define KAFKA_PUBLISHER_HH

#include <librdkafka/rdkafkacpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <atomic>
#include <thread>

#include "SpscQueue.hh"

template <size_t N>
class KafkaPublisher
{
public:

    explicit KafkaPublisher(std::string_view topic, std::string_view brokers);
    ~KafkaPublisher();

    KafkaPublisher(const KafkaPublisher&) = delete;
    KafkaPublisher& operator= (const KafkaPublisher&) = delete;

    KafkaPublisher(KafkaPublisher&&) = delete;
    KafkaPublisher& operator= (KafkaPublisher&&) = delete;

    // serialize and forward a message to the Kafka broker
    void publish(std::string_view bytes);

private:

    // owns the rdkafka producer handle and manages the connection to the broker
    std::unique_ptr<RdKafka::Producer> m_producer;

    // the Kafka topic this publisher writes to, e.g. "events.AAPL"
    std::string m_topic;

    // pending commands waiting to be sent; producer pushes, drain thread pops
    SpscQueue<std::string, N> m_queue {};
    
    // signals the drain thread to keep going or stop. 
    // producer sets it to false on shutdown, drain thread checks it each loop iteration.
    std::atomic<bool> m_running {}; 

    // the background thread that drains the queue and calls rdkafka.
    // starts in the constructor, joined in the destructor.
    std::thread m_drain_thread {}; 

    // loops while running or queue is not empty; pops messages and forwards them to the Kafka producer
    void drain_();

};

template <size_t N>
inline KafkaPublisher<N>::KafkaPublisher(std::string_view topic, std::string_view brokers) : m_topic { topic }
{

    // configure
    std::string errstr;
    auto* conf { RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL) };
    conf->set("bootstrap.servers", std::string(brokers), errstr);
    m_producer.reset(RdKafka::Producer::create(conf, errstr));
    delete conf;

    // start work
    m_running = true;
    m_drain_thread = std::thread(&KafkaPublisher::drain_, this);

}

template <size_t N>
inline KafkaPublisher<N>::~KafkaPublisher()
{

    // stop thread, flus hwhatevers left behind
    m_running = false;
    m_drain_thread.join();
    m_producer->flush(5000);

}

template <size_t N>
inline void KafkaPublisher<N>::publish(std::string_view bytes)
{

    m_queue.try_push(std::string { bytes });

}

template <size_t N>
inline void KafkaPublisher<N>::drain_()
{

    while (m_running || !m_queue.is_empty())
    {

        std::optional<std::string> cmd { m_queue.try_pop() };

        if (!cmd.has_value())
        {

            std::this_thread::yield();
            continue;

        }

        RdKafka::ErrorCode err { m_producer->produce(m_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(cmd->data()), cmd->size(), nullptr, 0, 0, nullptr) };

        if (err != RdKafka::ERR_NO_ERROR)
        {

            std::cerr << "produce failed: " << RdKafka::err2str(err) << "\n";

        }

        // give time for kafka to serve internal stuff
        m_producer->poll(0);

    }

}

#endif // KAFKA_PUBLISHER_HH
