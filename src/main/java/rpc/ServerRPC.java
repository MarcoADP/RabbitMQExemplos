package rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import utils.FactoryConnectionCreator;

public class ServerRPC {

    private static final String RPC_QUEUE_NAME = "q.rpc";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(RPC_QUEUE_NAME);
        channel.basicQos(1);

        System.out.println(" [x] Awaiting RPC requests");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "";
            try {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                int n = Integer.parseInt(message);

                System.out.println(" [.] fib(" + message + ")");
                response += fib(n);
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e);
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes(StandardCharsets.UTF_8));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {}));

    }


}
