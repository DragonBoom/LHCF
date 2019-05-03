package indi.dragonboom.crawler.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

public class FilesTest {

	@Test
	void go() throws IOException {
		Path p = Paths.get("E:\\crawler\\ff");
		Files.write(p, new String("ff").getBytes());
	}
}
