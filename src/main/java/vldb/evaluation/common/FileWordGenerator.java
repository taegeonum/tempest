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

  @NamedParameter(short_name = "data_path", default_value="./dataset/hashtag.txt")
  public final static class FileDataPath implements Name<String> {}

  private final File inputFile;
  private Scanner sc;
  private int index = 0;
  private String[] buffer = null;
  private final String filePath;

  @Inject
  private FileWordGenerator(
      @Parameter(FileDataPath.class) final String fileDataPath) {
    this.filePath = fileDataPath;
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
    if (sc.hasNextLine()) {
      final String str = sc.nextLine();
      if (str != null) {
        return str;
      } else {
        return "a";
      }
    } else {
      sc.close();
      try {
        sc = new Scanner(new File(filePath),  "UTF-8");
        return sc.nextLine();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public String lastString() {
    return null;
  }
}
