package priorityqueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class ConsumerWorker {

    private final static String QUEUE_NAME = "priority_queue";
    private final static Integer MAX_PRIORITY = 255;

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        Map<String, Object> args = new HashMap<>();
        args.put("x-max-priority", MAX_PRIORITY);
        channel.queueDeclare(QUEUE_NAME, true, false, false, args);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(Utils.getDateNow() + " => [x] Received '" + message + "'");
            try {
                doWork(message);
            } finally {
                System.out.println(Utils.getDateNow() + " => [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });

    }

    private static void doWork(String task) {
        Random random = new Random();
        int max = 5;
        int min = 1;
        int sleepSeconds = random.nextInt(max - min) + min;
        try {
            System.out.printf("Doing the task ~%s~ in %d seconds%n", task, sleepSeconds);
            Thread.sleep(1000 * sleepSeconds);
        } catch (InterruptedException _ignored) {
            Thread.currentThread().interrupt();
        }

    }

}
