package utils;

import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FactoryConnectionCreator {

    public static ConnectionFactory create() {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream input = classloader.getResourceAsStream("config.properties");

        Properties prop = new Properties();

        if (input == null) {
            System.out.println("Sorry, unable to find config.properties");
        } else {
            try {
                prop.load(input);
                factory.setUsername(prop.getProperty("rabbit.user"));
                factory.setPassword(prop.getProperty("rabbit.password"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return factory;
    }

}
