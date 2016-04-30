package vldb.evaluation;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.pafas.*;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TimescaleString;

import java.util.List;

/**
 * Created by taegeonum on 4/30/16.
 */
public final class DependencyGraphBuildCostEvaluation {

  public static DependencyGraph<Integer> getDependencyGraph(
      final String tsString,
      final Class<? extends DependencyGraph> dgClass) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, dgClass);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, GreedySelectionAlgorithm.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final DependencyGraph<Integer> dependencyGraph = injector.getInstance(DependencyGraph.class);
    final TimescaleParser tsParser = injector.getInstance(TimescaleParser.class);
    final List<Timescale> timescales = tsParser.timescales;
    final PeriodCalculator periodCalculator = injector.getInstance(PeriodCalculator.class);
    final long period = periodCalculator.getPeriod();
    return dependencyGraph;
  }

  public static void main(final String[] args) throws InjectionException {

    final String tsString20 = "(71,15)(72,21)(86,3)(95,10)(119,10)(168,30)(210,2)(226,15)(232,5)(259,23)(274,1)(340,6)(345,1)(507,30)(604,28)(659,6)(807,14)(839,10)(871,17)(1068,23)";
    final String tsString40 = "(50,7)(60,26)(67,28)(98,6)(110,7)(136,17)(140,20)(181,6)(306,14)(317,24)(351,20)(361,28)(378,30)(425,13)(438,17)(448,20)(483,10)(484,6)(486,7)(487,8)(493,10)(498,24)(574,8)(609,14)(611,20)(725,24)(726,14)(776,13)(779,24)(795,15)(800,24)(819,5)(832,14)(878,6)(933,8)(939,17)(988,8)(1010,26)(1176,17)(1196,20)";
    final String tsString60 = "(64,14)(70,6)(78,9)(79,27)(90,20)(110,28)(130,3)(169,3)(180,4)(210,4)(213,27)(283,24)(299,12)(300,14)(343,12)(405,7)(408,20)(439,1)(441,5)(443,3)(445,28)(454,11)(492,14)(592,6)(594,18)(633,30)(676,14)(699,14)(703,30)(730,7)(753,6)(780,18)(792,1)(806,4)(809,3)(814,16)(831,20)(891,18)(909,20)(917,4)(925,1)(989,18)(993,5)(1008,9)(1025,6)(1032,27)(1034,28)(1036,8)(1044,27)(1054,14)(1066,21)(1068,18)(1083,11)(1091,22)(1103,11)(1111,12)(1141,22)(1151,5)(1183,9)(1192,7)";
    final String tsString80 = "(54,2)(68,16)(72,16)(82,29)(89,29)(108,6)(133,30)(135,5)(141,24)(161,3)(170,24)(173,20)(187,16)(198,4)(275,17)(297,16)(306,12)(318,16)(346,1)(394,15)(411,6)(431,17)(467,20)(481,16)(498,5)(545,3)(551,29)(556,12)(561,30)(566,2)(621,29)(625,30)(626,2)(635,5)(638,1)(679,3)(701,6)(710,17)(719,16)(720,15)(726,5)(733,2)(737,10)(738,6)(745,17)(747,17)(784,15)(799,16)(812,16)(814,17)(827,20)(832,15)(842,30)(861,3)(879,4)(880,15)(884,17)(894,4)(898,15)(901,8)(938,12)(970,4)(979,4)(992,10)(994,12)(1006,8)(1013,1)(1043,30)(1046,17)(1050,10)(1065,2)(1077,2)(1078,16)(1089,1)(1116,20)(1124,8)(1148,30)(1154,16)(1181,16)(1183,5)";
    final String tsString100 = "(52,2)(77,14)(78,5)(79,13)(84,9)(89,13)(104,13)(113,28)(115,26)(122,27)(130,7)(132,26)(156,4)(157,13)(168,30)(192,13)(213,20)(214,28)(230,12)(235,6)(243,18)(249,16)(254,2)(296,13)(299,6)(311,6)(321,12)(336,21)(344,16)(349,1)(357,18)(366,16)(382,6)(388,20)(402,28)(444,7)(447,16)(467,26)(476,14)(487,6)(488,16)(489,16)(490,4)(504,30)(506,2)(507,20)(516,8)(521,30)(538,27)(548,15)(557,1)(567,20)(581,30)(582,3)(614,4)(638,7)(652,21)(671,30)(672,18)(684,15)(685,12)(733,9)(743,2)(745,21)(752,3)(760,30)(761,7)(763,21)(768,27)(772,18)(797,18)(818,3)(879,15)(894,12)(932,15)(938,30)(944,26)(948,2)(966,10)(975,16)(977,20)(993,5)(1007,24)(1009,9)(1012,12)(1022,4)(1040,28)(1045,28)(1050,13)(1064,27)(1093,20)(1094,20)(1101,24)(1116,1)(1127,24)(1132,7)(1164,15)(1191,13)(1193,27)(1200,16)";


    System.out.println("---------------------TS20: " +tsString20+"\n");
    System.out.println("--------------------------Type\tConstructionTime-------------------------------");

    long buildStartTime = System.currentTimeMillis();
    DependencyGraph<Integer> incrementParallelDG = getDependencyGraph(tsString20, IncrementalParallelDependencyGraphImpl.class);
    long buildEndTime = System.currentTimeMillis();
    System.out.println("IncParallelDG\t" + (buildEndTime-buildStartTime));

    System.out.println("----------------IncParallelDG step & time. StepSize=10------------------");
    for (int i = 1; i <= 100; i++) {
      buildStartTime = System.currentTimeMillis();
      incrementParallelDG.getFinalTimespans(10*i);
      buildEndTime = System.currentTimeMillis();
      System.out.println(i+"\t" + (buildEndTime-buildStartTime));
    }

    buildStartTime = System.currentTimeMillis();
    DependencyGraph<Integer> parallelStaticDG = getDependencyGraph(tsString20, StaticParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("---------------------StaticParallelDG\t" + (buildEndTime-buildStartTime) + "----------------");


    buildStartTime = System.currentTimeMillis();
    DependencyGraph<Integer> staticDG = getDependencyGraph(tsString20, StaticDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("--------------------StaticDG\t" + (buildEndTime-buildStartTime)+"---------------------");


    System.out.println("--------------------------TS40: " + tsString40+"-----------------------------\n");
    System.out.println("--------------------------Type\tConstructionTime-------------------------------");

    buildStartTime = System.currentTimeMillis();
    incrementParallelDG = getDependencyGraph(tsString40, IncrementalParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("IncParallelDG\t" + (buildEndTime-buildStartTime));

    System.out.println("----------------IncParallelDG step & time. StepSize=10------------------");
    for (int i = 1; i <= 100; i++) {
      buildStartTime = System.currentTimeMillis();
      incrementParallelDG.getFinalTimespans(10*i);
      buildEndTime = System.currentTimeMillis();
      System.out.println(i+"\t" + (buildEndTime-buildStartTime));
    }

    buildStartTime = System.currentTimeMillis();
    parallelStaticDG = getDependencyGraph(tsString40, StaticParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("---------------------StaticParallelDG\t" + (buildEndTime-buildStartTime) + "----------------");


    buildStartTime = System.currentTimeMillis();
    staticDG = getDependencyGraph(tsString40, StaticDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("--------------------StaticDG\t" + (buildEndTime-buildStartTime)+"---------------------");

    System.out.println("--------------------------TS60: " + tsString60+"-----------------------------\n");
    System.out.println("--------------------------Type\tConstructionTime-------------------------------");

    buildStartTime = System.currentTimeMillis();
    incrementParallelDG = getDependencyGraph(tsString60, IncrementalParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("IncParallelDG\t" + (buildEndTime-buildStartTime));

    System.out.println("----------------IncParallelDG step & time. StepSize=10------------------");
    for (int i = 1; i <= 100; i++) {
      buildStartTime = System.currentTimeMillis();
      incrementParallelDG.getFinalTimespans(10*i);
      buildEndTime = System.currentTimeMillis();
      System.out.println(i+"\t" + (buildEndTime-buildStartTime));
    }

    buildStartTime = System.currentTimeMillis();
    parallelStaticDG = getDependencyGraph(tsString60, StaticParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("---------------------StaticParallelDG\t" + (buildEndTime-buildStartTime) + "----------------");


    buildStartTime = System.currentTimeMillis();
    staticDG = getDependencyGraph(tsString60, StaticDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("--------------------StaticDG\t" + (buildEndTime-buildStartTime)+"---------------------");


    System.out.println("--------------------------TS100: " + tsString100+"-----------------------------\n");
    System.out.println("--------------------------Type\tConstructionTime-------------------------------");

    buildStartTime = System.currentTimeMillis();
    incrementParallelDG = getDependencyGraph(tsString100, IncrementalParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("IncParallelDG\t" + (buildEndTime-buildStartTime));

    System.out.println("----------------IncParallelDG step & time. StepSize=10------------------");
    for (int i = 1; i <= 100; i++) {
      buildStartTime = System.currentTimeMillis();
      incrementParallelDG.getFinalTimespans(10*i);
      buildEndTime = System.currentTimeMillis();
      System.out.println(i+"\t" + (buildEndTime-buildStartTime));
    }

    buildStartTime = System.currentTimeMillis();
    parallelStaticDG = getDependencyGraph(tsString100, StaticParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("---------------------StaticParallelDG\t" + (buildEndTime-buildStartTime) + "----------------");


    buildStartTime = System.currentTimeMillis();
    staticDG = getDependencyGraph(tsString100, StaticDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("--------------------StaticDG\t" + (buildEndTime-buildStartTime)+"---------------------");
  }
}
