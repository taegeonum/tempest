package atc.evaluation.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
 
 
public class ProcessHelper {
  //private static Logger log = Logger.getLogger(ProcessHelper.class);
  private List<Process> processes;
 
 
  public ProcessHelper() {
    processes = new ArrayList< Process >();
 
  }
 
  public Process startNewJavaProcess(final String optionsAsString, final String mainClass,
                                     final String classPath,
                                     final String[] arguments)
      throws IOException {
 
    ProcessBuilder processBuilder = createProcess(optionsAsString, mainClass, classPath, arguments);
    Process process = processBuilder.start();
    processes.add(process);
    //log.debug("Process " + process.toString() + " has started");
    return process;
  }
 
 
  private ProcessBuilder createProcess(final String optionsAsString, final String mainClass,
                                       final String classPath, final String[] arguments) {
    String jvm = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    //log.debug("classpath: " + classpath);
    // String workingDirectory = System.getProperty("user.dir");
 
    String[] options = optionsAsString.split(" ");
    List < String > command = new ArrayList <String>();
    command.add(jvm);
    command.addAll(Arrays.asList(options));
    command.add(mainClass);
    command.addAll(Arrays.asList(arguments));
 
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    Map< String, String > environment = processBuilder.environment();
    environment.put("CLASSPATH", classPath);
    return processBuilder;
  }
 
 
  public void killProcess(final Process process) {
    process.destroy();
  }
 
  /**
   * Kill all processes.
   */
  public void shutdown() {
    //log.debug("Killing " + processes.size() + " processes.");    
    for (Process process : processes) {
      killProcess(process);
    }
  }
}