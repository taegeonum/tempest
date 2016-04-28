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
public final class WikiWordGenerator extends Generator {

  @NamedParameter(short_name = "data_path", default_value=".")
  public final static class WikidataPath implements Name<String> {}

  private final File inputFile;
  private Scanner sc;
  private int index = 0;
  private String[] buffer = null;

  @Inject
  private WikiWordGenerator(
      @Parameter(WikidataPath.class) final String wikiDataPath) {
    this.inputFile = new File(wikiDataPath);
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
        buffer = sc.nextLine().split(" ");
        index = 0;
      } else {
        return null;
      }
    }
    try {
      final String word = buffer[index];
      index += 1;
      return word;
    } catch (final ArrayIndexOutOfBoundsException e) {
      e.printStackTrace();
      return "noword";
    }
  }

  @Override
  public String lastString() {
    return null;
  }
}
