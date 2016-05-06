package vldb.evaluation.common;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by taegeonum on 4/25/16.
 */
public final class FileWordGenerator extends Generator {

  @Override
  public void close() throws Exception {
    sc.close();
  }

  @NamedParameter(short_name = "data_path", default_value="./dataset/bigtwitter.txt")
  public final static class FileDataPath implements Name<String> {}

  private final File inputFile;
  private Scanner sc;
  private int index = 0;
  private String[] buffer = null;

  @Inject
  private FileWordGenerator(
      @Parameter(FileDataPath.class) final String fileDataPath) {
    this.inputFile = new File(fileDataPath);
    if (!inputFile.isFile()) {
      throw new RuntimeException(inputFile + " is not file");
    }

    try {
      this.sc = new Scanner(inputFile, "UTF-8");
      this.buffer = sc.nextLine().split(" ");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return a word.
   * @return a word
   */
  @Override
  public String nextString() {
    if (index >= buffer.length) {
      if (sc.hasNextLine()) {
        index = 0;
        final String line = sc.nextLine();
        if (line.length() == 0) {
          return "noword";
        }
        buffer = line.split(" ");
      } else {
        return null;
      }
    }
    try {
      String word = buffer[index];
      word = word.toLowerCase();
      word = word.trim();
      index += 1;
      return word;
    } catch (final ArrayIndexOutOfBoundsException e) {
      e.printStackTrace();
      System.out.println("@@ error");
      return "noword";
    }
  }

  @Override
  public String lastString() {
    return null;
  }
}
