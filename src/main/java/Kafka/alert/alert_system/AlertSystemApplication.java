package Kafka.alert.alert_system;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AlertSystemApplication {

    // Flink가 보내는 다양한 종류의 경고 메시지를 유연하게 받기 위한 공통 POJO
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NotificationPayload implements Serializable {
        public String alertType;
        public String description;
        public String principal;
        public String clientIp;
        public String methodName;
        public String resourceType;
    }

    public static void main(String[] args) {
        // --- 1. Kafka Consumer 설정 ---
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP", "3.143.42.149:29092,3.143.42.149:39092,3.143.42.149:49092");
        String groupId = "email-notification-worker-group";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Kafka 클러스터 보안 설정
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin-secret\";");

        // --- 2. 4개의 토픽 구독 ---
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(
                "certified-2-time",
                "certified-notMove",
                "resource-level-false",
                "system-level-false"
        ));

        System.out.println("Email Notification Worker started. Subscribing to 4 topics...");

        try {
            // --- 3. 무한 루프를 돌며 메시지 처리 ---
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    String sourceTopic = record.topic();
                    System.out.printf("Received message from topic: %s%n", sourceTopic);

                    try {
                        // --- 4. 토픽 이름에 따라 이메일 주소 결정 (라우팅 로직) ---
                        String targetEmail;
                        switch (sourceTopic) {
                            case "certified-2-time":
                            case "certified-notMove":
                                targetEmail = "tgurd123@gmail.com"; // 보안팀
                                break;
                            case "resource-level-false":
                                targetEmail = "tgurd123@gmail.com"; // Kafka 관리자
                                break;
                            case "system-level-false":
                            default:
                                targetEmail = "tgsduser@gmail.com"; // 플랫폼팀
                                break;
                        }

                        // --- 5. 이메일 내용 생성 및 발송 ---
                        String subject = "[Kafka Security Alert] " + sourceTopic;
                        String body = formatEmailBody(record.value());

                        // 실제 이메일 발송
                        sendEmail(targetEmail, subject, body);

                    } catch (Exception e) {
                        System.err.println("Failed to process or send email for record: " + record.value());
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumer.close();
            System.out.println("Email Notification Worker stopped.");
        }
    }

    private static String formatEmailBody(String jsonPayload) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            NotificationPayload payload = mapper.readValue(jsonPayload, NotificationPayload.class);
            return "A new security alert has been triggered.\n\n" +
                    "Alert Type: " + (payload.alertType != null ? payload.alertType : "N/A") + "\n" +
                    "Description: " + (payload.description != null ? payload.description : "No description provided.") + "\n\n" +
                    "--- Details ---\n" +
                    "Principal: " + (payload.principal != null ? payload.principal : "N/A") + "\n" +
                    "Client IP: " + (payload.clientIp != null ? payload.clientIp : "N/A") + "\n" +
                    "Method Name: " + (payload.methodName != null ? payload.methodName : "N/A") + "\n" +
                    "Resource Type: " + (payload.resourceType != null ? payload.resourceType : "N/A") + "\n\n" +
                    "Raw Log:\n" + jsonPayload;
        } catch (Exception e) {
            return "Failed to parse alert details. Please check the raw log below:\n\n" + jsonPayload;
        }
    }


    // 여기 메일 만들면 고쳐주세요~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    private static void sendEmail(String to, String subject, String body) {
        final String smtpHost = "smtp.gmail.com";
        final int smtpPort = 587;
        final String username = "your-email@gmail.com";
        final String password = "your-app-password";

        // ---------------------------------------------

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.port", smtpPort);
        props.put("mail.smtp.ssl.trust", smtpHost);

        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(username));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            message.setSubject(subject);
            message.setText(body);

            Transport.send(message);

            System.out.printf("Successfully sent email to %s%n", to);

        } catch (MessagingException e) {
            System.err.printf("!!! FAILED to send email to %s%n", to);
            e.printStackTrace();
        }
    }
}
