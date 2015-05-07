

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.Random;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;


public class Sender {
    private final static String QUEUE_NAME = "test";
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    JSONParser parser = new JSONParser();
    Iterator i;

    public void run(String inputFileName) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            JSONArray obj = (JSONArray) parser.parse(new FileReader(inputFileName));
            Random random = new Random();
            Object element = obj.get(random.nextInt(obj.size()));
            channel.basicPublish("", QUEUE_NAME, null, element.toString().getBytes());
            System.out.println("SENT : " + element);
            channel.close();
            connection.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] argv) throws Exception {

        if (argv.length <= 0) {
            System.out.println("Usage : Pass filename as parameter to the class");
        } else {
            final String inputFileName = argv[0];

            final Sender send = new Sender();
            final Runnable beeper = new Runnable() {
                public void run() {
                    send.run(inputFileName);
                }
            };
            final ScheduledFuture<?> beeperHandle =
                    send.scheduler.scheduleAtFixedRate(beeper, 0, 1, MINUTES);
            send.scheduler.schedule(new Runnable() {
                public void run() {
                    beeperHandle.cancel(true);
                }
            }, 60 * 60, SECONDS);
        }
    }
}