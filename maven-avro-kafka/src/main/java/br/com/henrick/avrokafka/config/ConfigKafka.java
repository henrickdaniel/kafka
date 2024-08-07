package br.com.henrick.avrokafka.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ConfigKafka {

    private static final String CONFIG_FILE = "client.properties";

    public static Properties readConfig() throws IOException {
        // reads the client configuration from client.properties
        // and returns it as a Properties object
        if (!Files.exists(Paths.get(CONFIG_FILE))) {
            throw new IOException(CONFIG_FILE + " not found.");
        }

        final Properties config = new Properties();
        try (InputStream inputStream = new FileInputStream(CONFIG_FILE)) {
            config.load(inputStream);
        }

        return config;
    }

}
