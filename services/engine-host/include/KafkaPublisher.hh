#ifndef KAFKA_PUBLISHER_HH
#define KAFKA_PUBLISHER_HH

#include <librdkafka/rdkafkacpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>

class KafkaPublisher
{
public:

    explicit KafkaPublisher(std::string_view topic, std::string_view brokers);
    ~KafkaPublisher();

    KafkaPublisher(const KafkaPublisher&) = delete;
    KafkaPublisher& operator= (const KafkaPublisher&) = delete;

    KafkaPublisher(KafkaPublisher&&) = delete;
    KafkaPublisher& operator= (KafkaPublisher&&) = delete;

    void publish(std::string_view bytes);

private:
    
    std::unique_ptr<RdKafka::Producer> m_producer;
    std::string m_topic;

};

inline KafkaPublisher::KafkaPublisher(std::string_view topic, std::string_view brokers) : m_topic { topic }
{

    std::string errstr;
    auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", std::string(brokers), errstr);
    m_producer.reset(RdKafka::Producer::create(conf, errstr));
    delete conf;
    
}

inline KafkaPublisher::~KafkaPublisher()
{

    m_producer->flush(5000);

}

inline void KafkaPublisher::publish(std::string_view bytes)
{

    RdKafka::ErrorCode err = m_producer->produce(m_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(bytes.data()), bytes.size(), nullptr, 0, 0, nullptr);
    if (err != RdKafka::ERR_NO_ERROR)
        std::cerr << "produce failed: " << RdKafka::err2str(err) << "\n";
    m_producer->poll(0);

}

#endif // KAFKA_PUBLISHER_HH
