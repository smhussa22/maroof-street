package street.maroof;

// TWAP bot: institutional-style execution.
// Breaks a large parent order into equal time-sliced child orders.
// Cycles through symbols, alternating buy/sell phases.
// Unlike the reactive bots, this doesn't respond to price at all —
// it just executes a predetermined schedule, creating sustained
// one-sided pressure that the other bots have to react to.

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import maroof_street.orders.OrderServiceGrpc;
import maroof_street.orders.Orders;

import java.util.concurrent.TimeUnit;

public class TwapBot {

    private static final String API_KEY          = "java-bot-key-555";
    private static final String GATEWAY_HOST     = "localhost";
    private static final int    GATEWAY_PORT     = 50051;

    private static final String[] SYMBOLS        = {"AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"};
    private static final int[]    REF_PRICES     = { 150,    250,    400,    300,    170   };

    private static final int TOTAL_QTY           = 500;   // shares in the parent order
    private static final int SLICES              = 10;    // child orders
    private static final int SLICE_QTY           = TOTAL_QTY / SLICES;
    private static final int SLICE_INTERVAL_MS   = 10_000; // 10 seconds between slices

    // the engine only has 1,000,000 id slots, so this bot owns the 100k block starting at 500,000 and wraps
    private static long orderSeq = 0;

    private static long nextOrderId() {
        return 500_000 + (orderSeq++ % 100_000);
    }

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(GATEWAY_HOST, GATEWAY_PORT)
                .usePlaintext()
                .build();

        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER), API_KEY);

        OrderServiceGrpc.OrderServiceBlockingStub stub =
                OrderServiceGrpc.newBlockingStub(channel)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        System.out.println("java twap bot running");

        int symbolIdx = 0;
        boolean buying = true;

        try {
            while (true) {
                String symbol   = SYMBOLS[symbolIdx];
                int    refPrice = REF_PRICES[symbolIdx];

                Orders.OrderDirection dir = buying
                        ? Orders.OrderDirection.SIDE_BUY
                        : Orders.OrderDirection.SIDE_SELL;

                System.out.printf("twap: %s %s x%d in %d slices%n",
                        buying ? "BUY" : "SELL", symbol, TOTAL_QTY, SLICES);

                for (int slice = 0; slice < SLICES; slice++) {
                    // price walks slightly with each slice to reflect market impact
                    int impact = slice / 3;
                    int price  = buying ? refPrice + impact : refPrice - impact;

                    Orders.SubmitOrder req = Orders.SubmitOrder.newBuilder()
                            .setApiKey(API_KEY)
                            .setSymbol(symbol)
                            .setPrice(price)
                            .setQuantity(SLICE_QTY)
                            .setOrderDirection(dir)
                            .setOrderType(Orders.OrderType.ORDER_TYPE_GTC)
                            .setClientOrderId(nextOrderId())
                            .build();

                    try {
                        Orders.OrderAck ack = stub
                                .withDeadlineAfter(5, TimeUnit.SECONDS)
                                .submitOrder(req);
                        System.out.printf("  slice %d/%d %s%n",
                                slice + 1, SLICES,
                                ack.getAccepted() ? "accepted" : "rejected: " + ack.getRejectedReason());
                    } catch (Exception e) {
                        System.err.println("twap error: " + e.getMessage());
                    }

                    Thread.sleep(SLICE_INTERVAL_MS);
                }

                // rotate to next symbol and flip direction
                symbolIdx = (symbolIdx + 1) % SYMBOLS.length;
                buying = !buying;
            }
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
