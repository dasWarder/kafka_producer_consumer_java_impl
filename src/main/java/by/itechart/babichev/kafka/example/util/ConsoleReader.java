package by.itechart.babichev.kafka.example.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConsoleReader {

    private final BufferedReader reader;

    public ConsoleReader() {
        this.reader = new BufferedReader(new InputStreamReader(System.in));
    }

    public String readData() throws IOException {
        return reader.readLine();
    }

    public void closeReader() throws IOException {
        reader.close();
    }
}
