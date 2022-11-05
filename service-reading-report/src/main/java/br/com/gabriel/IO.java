package br.com.gabriel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public class IO {

    IO() {
    }

    public static void copyTo(Path source, File target) throws IOException {
        boolean created = target.getParentFile().mkdirs();
        System.out.println(created ? "Created new File" : "Directory already exists");
        Files.copy(source, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    public static void append(File target, String value) throws IOException {
        Files.write(target.toPath(), value.getBytes(), StandardOpenOption.APPEND);
    }
}
