#include <iostream>
#include <memory>
#include <string>

#include <librdkafka/rdkafkacpp.h>

#include "KafkaListener.hh"
#include "KafkaPublisher.hh"
#include "OrderBook.hh"
#include "orders.pb.h"

int main()
{
    // @todo add an environment block in docker-compose and k8s
    const char* symbol = std::getenv("SYMBOL");

    KafkaPublisher<4096> publisher { std::string("events.") + symbol, "localhost:9092" };
    KafkaListener listener { publisher, symbol };
    OrderBook<KafkaListener> order_book { listener };

    std::string errstr;
    std::unique_ptr<RdKafka::Conf> conf { RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL) };
    conf->set("bootstrap.servers", "localhost:9092", errstr);
    conf->set("group.id", std::string("engine-host.") + symbol, errstr);

    std::unique_ptr<RdKafka::KafkaConsumer> consumer { RdKafka::KafkaConsumer::create(conf.get(), errstr) };
    consumer->subscribe({ std::string("commands.") + symbol });

    // --- replay phase: rebuild book from command history ---
    {
        int64_t lo, hi;
        std::string topic_name = std::string("commands.") + symbol;

        std::unique_ptr<RdKafka::Conf> rconf { RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL) };
        rconf->set("bootstrap.servers", "localhost:9092", errstr);
        rconf->set("group.id", std::string("replay.") + symbol, errstr);
        std::unique_ptr<RdKafka::KafkaConsumer> rconsumer { RdKafka::KafkaConsumer::create(rconf.get(), errstr) };

        rconsumer->query_watermark_offsets(topic_name, 0, &lo, &hi, 5000);

        if (hi > 0)
        {
            listener.set_replay(true);

            RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(topic_name, 0, lo);
            rconsumer->assign({ tp });
            delete tp;

            int64_t replayed = 0;
            while (replayed < hi - lo)
            {
                std::unique_ptr<RdKafka::Message> msg { rconsumer->consume(1000) };
                if (msg->err() != RdKafka::ERR_NO_ERROR) continue;

                maroof_street::orders::Command cmd;
                cmd.ParseFromArray(msg->payload(), static_cast<int>(msg->len()));

                if (cmd.has_submit())
                {
                    const auto& s = cmd.submit();
                    Order order {
                        s.client_order_id(), s.price(), static_cast<int32_t>(s.quantity()),
                        s.order_direction() == maroof_street::orders::SIDE_BUY ? OrderDirection::Buy : OrderDirection::Sell
                    };
                    OrderType type = s.order_type() == maroof_street::orders::ORDER_TYPE_FAK ? OrderType::FAK : OrderType::GTC;
                    order_book.submit_order(order, type);
                }
                else if (cmd.has_cancel())
                {
                    order_book.cancel_order(cmd.cancel().order_id());
                }

                ++replayed;
            }

            listener.set_replay(false);
            rconsumer->close();
            std::cout << "replayed " << replayed << " commands, book rebuilt\n";
        }
    }
    // --- end replay phase ---

    std::cout << "engine-host running for " << symbol << "\n";

    for (;;)
    {
        std::unique_ptr<RdKafka::Message> msg { consumer->consume(1000) };

        if (msg->err() != RdKafka::ERR_NO_ERROR)
            continue;

        maroof_street::orders::Command cmd;
        cmd.ParseFromArray(msg->payload(), static_cast<int>(msg->len()));

        if (cmd.has_submit())
        {
            const auto& s = cmd.submit();
            Order order 
            { 
                s.client_order_id(), s.price(), static_cast<int32_t>(s.quantity()), 
                s.order_direction() == maroof_street::orders::SIDE_BUY ? OrderDirection::Buy : OrderDirection::Sell 
            };

            OrderType type = s.order_type() == maroof_street::orders::ORDER_TYPE_FAK ? OrderType::FAK : OrderType::GTC;
            order_book.submit_order(order, type);

        }
        else if (cmd.has_cancel())
        {

            order_book.cancel_order(cmd.cancel().order_id());

        }

    }
    
}
