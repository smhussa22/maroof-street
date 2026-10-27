#ifndef KAFKA_LISTENER_HH
#define KAFKA_LISTENER_HH

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include "Order.hh"
#include "KafkaPublisher.hh"
#include "events.pb.h"

class KafkaListener
{
public:

    explicit KafkaListener(KafkaPublisher& publisher, std::string_view symbol);
    ~KafkaListener() = default;

    KafkaListener(const KafkaListener&) = delete;
    KafkaListener& operator= (const KafkaListener&) = delete;

    KafkaListener(KafkaListener&&) = delete;
    KafkaListener& operator= (KafkaListener&&) = delete;

    void on_trade(uint64_t aggressor_id, uint64_t passive_id, int32_t price, uint32_t quantity, bool aggressor_filled, bool passive_filled);
    void on_order_added(uint64_t order_id, int32_t price, uint32_t quantity, OrderDirection direction);
    void on_order_cancelled(uint64_t order_id);
    void on_order_rejected(uint64_t order_id, OrderRejected reason);
    void on_order_book_update(int32_t price, uint32_t volume, OrderDirection direction);

private:

    KafkaPublisher& m_publisher;
    std::string m_symbol;
    uint64_t m_seq { 0 };

    maroof_street::events::ExchangeEvent make_event();

};

namespace
{

    inline maroof_street::orders::OrderDirection to_proto_direction(OrderDirection d)
    {

        return d == OrderDirection::Buy ? maroof_street::orders::SIDE_BUY : maroof_street::orders::SIDE_SELL;

    }

    inline maroof_street::events::RejectedReason to_proto_reason(OrderRejected r)
    {

        switch (r)
        {

            case OrderRejected::InvalidPrice:    
                return maroof_street::events::REJECTED_REASON_INVALID_PRICE;

            case OrderRejected::InvalidQuantity: 
                return maroof_street::events::REJECTED_REASON_INVALID_QUANTITY;

            case OrderRejected::DuplicateId:    
                return maroof_street::events::REJECTED_REASON_DUPLICATE_ID;

            case OrderRejected::FullPool:        
                return maroof_street::events::REJECTED_REASON_POOL_FULL;

            case OrderRejected::NotFound: 
                return maroof_street::events::REJECTED_REASON_NOT_FOUND;

        }

        return maroof_street::events::REJECTED_REASON_UNSPECIFIED;

    }

} // namespace

inline KafkaListener::KafkaListener(KafkaPublisher& publisher, std::string_view symbol) : m_publisher { publisher } , m_symbol { symbol } {}

inline maroof_street::events::ExchangeEvent KafkaListener::make_event()
{

    maroof_street::events::ExchangeEvent event;
    event.set_seq(++m_seq);
    event.set_symbol(m_symbol);
    event.set_ts_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());

    return event;

}

inline void KafkaListener::on_trade(uint64_t aggressor_id, uint64_t passive_id, int32_t price, uint32_t quantity, bool aggressor_filled, bool passive_filled)
{

    auto event { make_event() };
    auto* fill { event.mutable_trade_fill() };
    fill->set_aggressor_id(aggressor_id);
    fill->set_passive_id(passive_id);
    fill->set_price(price);
    fill->set_quantity(quantity);
    fill->set_aggressor_done(aggressor_filled);
    fill->set_passive_done(passive_filled);

    std::string bytes;
    event.SerializeToString(&bytes);
    m_publisher.publish(bytes);

}

inline void KafkaListener::on_order_added(uint64_t order_id, int32_t, uint32_t, OrderDirection)
{

    auto event = make_event();
    event.mutable_ack()->set_order_id(order_id);

    std::string bytes;
    event.SerializeToString(&bytes);
    m_publisher.publish(bytes);

}

inline void KafkaListener::on_order_cancelled(uint64_t order_id)
{

    auto event { make_event() };
    event.mutable_cancel()->set_order_id(order_id);

    std::string bytes;
    event.SerializeToString(&bytes);
    m_publisher.publish(bytes);

}

inline void KafkaListener::on_order_rejected(uint64_t order_id, OrderRejected reason)
{

    auto event { make_event() };
    auto* reject { event.mutable_reject() } ;
    reject->set_order_id(order_id);
    reject->set_reason(to_proto_reason(reason));

    std::string bytes;
    event.SerializeToString(&bytes);
    m_publisher.publish(bytes);

}

inline void KafkaListener::on_order_book_update(int32_t price, uint32_t volume, OrderDirection direction)
{

    auto event { make_event() };
    auto* update { event.mutable_book_update() };
    update->set_price(price);
    update->set_volume(volume);
    update->set_side(to_proto_direction(direction));

    std::string bytes;
    event.SerializeToString(&bytes);
    m_publisher.publish(bytes);
    
}

#endif // KAFKA_LISTENER_HH
