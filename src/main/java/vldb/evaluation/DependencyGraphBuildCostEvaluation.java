package vldb.evaluation;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import vldb.evaluation.util.RandomSlidingWindowGenerator;
import vldb.operator.window.timescale.Timescale;
import vldb.operator.window.timescale.common.TimescaleParser;
import vldb.operator.window.timescale.pafas.*;
import vldb.operator.window.timescale.parameter.StartTime;
import vldb.operator.window.timescale.parameter.TimescaleString;

import java.util.*;

/**
 * Created by taegeonum on 4/30/16.
 */
public final class DependencyGraphBuildCostEvaluation {

  public static Injector getDependencyGraph(
      final String tsString,
      final Class<? extends DependencyGraph> dgClass,
      final Class<? extends PartialTimespans> ptClass) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TimescaleString.class, tsString);
    jcb.bindNamedParameter(StartTime.class, "0");
    jcb.bindImplementation(DependencyGraph.class, dgClass);
    jcb.bindImplementation(DependencyGraph.SelectionAlgorithm.class, DPSelectionAlgorithm.class);
    jcb.bindImplementation(PartialTimespans.class, ptClass);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector;
  }

  public static void main(final String[] args) throws InjectionException {
    final List<String> tss = new LinkedList<>();
    final List<String> windows = new LinkedList<>();

    /*
    tss.add("(250,86)(358,38)(391,95)(402,85)(414,96)(465,32)(573,58)(593,79)(623,17)(640,67)(647,10)(650,48)(674,37)(717,45)(828,30)(831,93)(903,14)(1017,28)(1099,38)(1172,15)");
    windows.add("20");

    tss.add("(216,90)(225,38)(270,70)(310,25)(318,5)(326,19)(363,43)(380,11)(430,77)(474,43)(500,96)(551,19)(564,80)(572,60)(603,62)(611,100)(629,73)(654,73)(746,21)(758,2)(769,75)(785,29)(792,9)(808,33)(871,29)(896,10)(914,18)(934,31)(960,100)(986,49)(993,5)(995,97)(1008,6)(1048,35)(1067,95)(1116,70)(1156,20)(1159,30)(1160,63)(1182,28)");
    windows.add("40");

    tss.add("(201,31)(209,36)(221,51)(225,51)(252,1)(258,28)(266,62)(306,3)(365,100)(376,95)(382,85)(390,86)(431,10)(454,36)(482,34)(509,39)(520,91)(524,78)(530,76)(550,20)(558,19)(564,87)(565,45)(570,5)(581,39)(589,7)(591,10)(603,29)(614,73)(617,40)(627,26)(647,84)(654,75)(685,5)(691,95)(698,82)(728,19)(741,68)(835,80)(846,10)(858,32)(911,41)(917,50)(947,68)(948,78)(978,19)(992,95)(1000,2)(1018,26)(1019,70)(1021,3)(1044,65)(1068,4)(1084,3)(1092,48)(1094,73)(1113,3)(1124,78)(1157,50)(1186,76)");
    windows.add("60");

    tss.add("(219,51)(234,83)(260,50)(270,99)(279,51)(290,26)(303,60)(320,58)(332,45)(343,93)(352,24)(360,75)(365,34)(391,95)(400,84)(422,2)(443,12)(450,15)(458,42)(488,2)(521,55)(529,70)(530,33)(542,9)(547,43)(548,91)(554,95)(610,21)(619,76)(634,99)(652,57)(705,12)(711,29)(720,66)(730,87)(740,24)(751,77)(758,50)(765,6)(767,36)(777,88)(812,13)(847,51)(866,51)(883,76)(887,81)(900,87)(907,81)(908,3)(909,86)(911,88)(922,4)(934,85)(957,93)(962,28)(968,55)(979,55)(981,8)(987,55)(1022,66)(1030,86)(1036,38)(1052,86)(1081,5)(1086,99)(1094,14)(1103,78)(1105,38)(1107,15)(1117,19)(1122,99)(1125,42)(1141,21)(1156,91)(1157,12)(1159,57)(1162,85)(1172,35)(1196,57)(1197,9)");
    windows.add("80");

    tss.add("(202,33)(225,42)(232,85)(238,44)(241,80)(246,94)(248,17)(255,15)(262,6)(276,60)(304,3)(305,44)(342,58)(357,4)(368,78)(384,39)(393,7)(405,4)(408,99)(417,18)(421,45)(424,16)(425,51)(427,85)(432,39)(437,100)(442,57)(458,36)(464,63)(465,75)(467,13)(469,89)(472,26)(482,34)(500,29)(543,55)(545,25)(555,14)(565,75)(568,77)(576,90)(585,96)(610,11)(620,65)(628,58)(639,87)(645,36)(653,68)(665,1)(679,17)(681,10)(684,11)(690,56)(696,40)(702,60)(735,11)(743,53)(760,44)(764,78)(771,15)(777,72)(791,18)(796,57)(813,80)(829,28)(837,32)(854,34)(863,77)(873,90)(881,87)(885,100)(894,91)(897,32)(903,18)(915,47)(917,21)(920,55)(926,36)(959,13)(962,85)(976,66)(981,22)(983,47)(985,35)(998,36)(1008,95)(1015,95)(1040,51)(1046,47)(1047,34)(1049,7)(1072,11)(1093,76)(1101,80)(1116,12)(1125,42)(1129,76)(1141,45)(1165,17)(1168,1)");
    windows.add("100");

    tss.add("(209,73)(279,77)(316,51)(317,4)(318,51)(322,24)(325,63)(334,42)(337,3)(340,23)(343,31)(345,6)(352,56)(354,9)(355,94)(356,8)(363,33)(373,77)(375,21)(392,63)(394,1)(395,88)(406,99)(412,33)(417,17)(431,33)(447,21)(454,48)(456,18)(457,93)(468,4)(473,62)(479,48)(490,72)(494,83)(497,34)(500,44)(508,3)(517,18)(518,2)(520,47)(521,54)(525,48)(532,48)(533,62)(536,42)(539,1)(544,66)(572,77)(576,6)(581,7)(596,69)(605,81)(635,33)(639,36)(665,18)(671,8)(673,66)(679,36)(683,73)(694,7)(700,6)(703,27)(713,44)(726,94)(747,94)(756,23)(766,6)(767,1)(771,22)(777,1)(780,2)(788,66)(794,47)(807,18)(809,94)(812,62)(816,17)(819,88)(824,73)(830,2)(833,63)(854,46)(858,23)(869,56)(881,62)(887,3)(896,62)(929,18)(943,77)(970,68)(984,28)(985,3)(988,53)(1004,66)(1016,83)(1034,93)(1035,62)(1042,54)(1046,56)(1047,11)(1066,6)(1068,8)(1069,81)(1071,14)(1079,99)(1087,69)(1103,94)(1106,66)(1112,42)(1115,36)(1122,56)(1126,88)(1148,73)(1157,24)(1158,88)(1170,81)(1175,69)(1190,9)(1191,51)");
    windows.add("120");

    tss.add("(201,34)(206,4)(219,18)(225,12)(227,18)(229,2)(230,30)(237,6)(252,17)(255,28)(264,73)(268,69)(270,96)(277,64)(300,57)(306,51)(310,95)(312,50)(313,74)(317,92)(319,24)(339,50)(342,92)(370,10)(376,12)(389,4)(396,36)(398,25)(403,3)(413,84)(414,89)(421,64)(460,74)(461,37)(462,37)(467,89)(472,37)(473,89)(480,72)(483,46)(484,51)(495,32)(503,73)(507,68)(510,7)(514,73)(515,9)(523,76)(528,3)(543,25)(545,36)(553,5)(554,72)(562,19)(564,76)(569,34)(596,16)(624,64)(635,75)(637,61)(638,20)(648,12)(652,68)(655,36)(660,72)(662,35)(682,12)(689,84)(690,100)(706,100)(707,64)(708,76)(718,1)(726,76)(740,57)(746,28)(774,3)(779,7)(783,95)(789,18)(792,32)(807,38)(819,69)(854,63)(859,23)(864,5)(867,61)(869,12)(872,73)(876,73)(883,64)(885,95)(891,100)(905,32)(906,5)(912,89)(917,38)(925,16)(929,20)(933,34)(950,6)(951,21)(952,57)(954,74)(975,42)(984,30)(989,70)(995,51)(999,90)(1009,84)(1012,57)(1015,75)(1017,95)(1018,25)(1020,36)(1042,12)(1046,70)(1048,30)(1050,89)(1051,24)(1053,4)(1063,24)(1069,64)(1070,18)(1076,20)(1078,46)(1080,37)(1081,10)(1094,17)(1097,100)(1116,69)(1124,96)(1127,84)(1141,90)(1144,89)(1159,1)(1160,37)(1164,72)(1171,36)(1176,76)");
    windows.add("140");

    tss.add("(209,55)(211,5)(216,18)(223,60)(228,3)(231,98)(232,89)(233,57)(239,62)(243,3)(257,66)(260,56)(261,52)(270,31)(280,18)(282,14)(290,65)(291,56)(304,66)(308,99)(312,49)(319,24)(322,38)(327,5)(332,54)(334,10)(351,13)(354,76)(355,90)(361,66)(366,84)(389,12)(406,96)(413,80)(416,34)(417,32)(421,72)(423,49)(425,64)(438,44)(447,57)(448,21)(451,12)(457,24)(461,30)(462,12)(464,45)(476,11)(482,85)(488,25)(494,70)(502,27)(509,1)(521,72)(523,25)(535,77)(543,36)(549,16)(551,70)(568,7)(572,36)(579,84)(584,22)(585,51)(587,43)(597,38)(601,77)(609,93)(612,32)(634,78)(635,2)(639,20)(640,70)(643,86)(646,7)(652,19)(653,50)(659,48)(662,89)(672,63)(676,86)(681,85)(684,15)(694,32)(697,17)(705,62)(707,22)(709,64)(712,39)(746,78)(747,91)(765,1)(768,50)(769,45)(783,20)(785,20)(791,50)(796,77)(810,76)(821,11)(827,80)(834,75)(844,76)(851,66)(866,12)(869,40)(878,5)(888,10)(891,48)(893,18)(901,72)(908,44)(916,16)(932,26)(937,44)(939,3)(956,48)(960,70)(965,33)(970,62)(980,56)(992,93)(995,2)(997,31)(999,48)(1002,15)(1003,77)(1006,63)(1012,22)(1039,88)(1043,51)(1046,84)(1048,50)(1049,43)(1063,22)(1066,13)(1077,6)(1078,51)(1085,3)(1088,57)(1094,15)(1095,60)(1096,78)(1097,21)(1105,1)(1118,33)(1120,18)(1122,32)(1123,51)(1124,14)(1125,54)(1128,76)(1149,31)(1156,18)(1158,4)(1163,19)(1169,14)(1177,55)(1180,80)(1190,62)");
    windows.add("160");

    tss.add("(209,91)(210,31)(224,6)(234,55)(236,32)(240,62)(243,21)(247,70)(248,55)(252,94)(254,31)(260,50)(261,94)(265,74)(271,14)(275,48)(276,4)(277,54)(278,42)(290,4)(311,62)(321,78)(331,37)(335,61)(341,10)(344,26)(347,21)(348,56)(350,31)(354,17)(357,35)(361,1)(363,84)(367,50)(381,10)(400,45)(402,50)(404,24)(406,77)(409,55)(418,24)(420,77)(423,88)(425,45)(437,35)(448,74)(451,90)(455,27)(456,3)(468,66)(469,25)(470,88)(471,62)(472,6)(476,8)(482,92)(493,15)(496,40)(499,74)(501,26)(515,33)(516,5)(521,35)(524,42)(528,8)(529,85)(531,60)(534,48)(535,16)(536,46)(543,4)(548,37)(549,54)(550,60)(553,50)(559,10)(565,24)(567,3)(569,40)(573,2)(581,6)(582,70)(589,10)(590,85)(597,31)(612,72)(615,16)(633,16)(635,66)(641,55)(646,27)(647,37)(648,2)(650,55)(656,75)(657,25)(659,54)(661,22)(674,10)(682,50)(688,63)(689,13)(698,66)(702,84)(714,21)(723,25)(726,37)(727,26)(728,85)(729,25)(732,32)(735,36)(745,63)(747,7)(754,42)(756,91)(760,42)(772,23)(781,69)(787,77)(788,92)(802,74)(814,23)(824,4)(826,30)(829,91)(837,2)(840,96)(851,65)(852,63)(862,9)(878,78)(881,34)(895,5)(903,2)(911,62)(919,6)(925,7)(929,75)(942,55)(943,18)(948,4)(957,16)(968,30)(969,25)(990,4)(994,91)(1000,72)(1004,14)(1005,74)(1007,50)(1008,45)(1018,92)(1020,69)(1044,55)(1051,3)(1053,65)(1063,30)(1064,16)(1069,11)(1072,77)(1085,18)(1086,54)(1089,96)(1092,7)(1099,85)(1105,91)(1116,92)(1118,35)(1122,12)(1127,8)(1130,3)(1132,99)(1136,72)(1137,51)(1143,61)(1147,26)(1158,68)(1179,44)(1200,75)");
    windows.add("180");

    tss.add("(204,60)(207,15)(209,18)(213,60)(217,52)(223,16)(226,10)(227,25)(239,5)(242,94)(244,90)(245,33)(247,14)(255,28)(259,50)(262,3)(263,83)(266,75)(275,85)(279,8)(289,68)(296,44)(297,42)(300,10)(303,37)(306,91)(310,97)(313,7)(315,74)(319,16)(321,26)(323,65)(331,6)(337,80)(338,70)(340,28)(341,80)(343,30)(353,9)(361,1)(362,37)(379,56)(382,35)(384,10)(393,13)(399,13)(403,66)(404,17)(422,35)(426,100)(435,34)(436,91)(438,33)(460,40)(463,65)(465,33)(471,36)(473,42)(484,18)(486,65)(489,90)(490,47)(498,78)(507,13)(509,97)(517,51)(519,74)(522,58)(525,29)(528,17)(529,47)(530,25)(533,70)(540,26)(541,84)(543,88)(546,87)(553,47)(556,40)(561,40)(578,8)(580,20)(582,16)(588,77)(597,12)(604,44)(605,94)(611,1)(612,47)(618,74)(621,5)(626,36)(637,4)(643,21)(651,1)(652,78)(657,25)(659,8)(665,29)(674,80)(689,40)(697,37)(704,36)(710,17)(729,14)(741,60)(748,87)(767,14)(774,75)(780,77)(786,51)(794,74)(804,5)(808,50)(813,56)(820,3)(821,80)(834,2)(837,99)(838,20)(844,47)(846,13)(849,12)(853,1)(855,78)(865,80)(866,77)(868,10)(875,29)(879,47)(882,94)(892,29)(906,88)(908,90)(909,33)(914,15)(920,35)(925,3)(932,3)(934,8)(938,68)(939,29)(940,18)(941,10)(943,37)(953,68)(957,25)(959,2)(961,29)(966,97)(969,87)(971,60)(979,56)(980,14)(983,74)(994,18)(998,33)(1000,63)(1001,48)(1004,70)(1015,88)(1019,26)(1021,29)(1024,87)(1031,45)(1043,50)(1044,42)(1054,25)(1061,25)(1066,1)(1070,39)(1073,70)(1078,11)(1085,18)(1086,10)(1091,87)(1098,91)(1105,2)(1107,74)(1110,30)(1112,5)(1121,40)(1131,97)(1132,47)(1134,88)(1137,37)(1144,21)(1145,33)(1148,11)(1153,91)(1154,51)(1155,24)(1157,87)(1169,68)(1178,34)(1180,75)(1184,20)(1191,2)(1196,75)(1197,42)");
    windows.add("200");
    */

/*
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, 200+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, 10000+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, 1+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, 100+"");
    //jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxPeriod.class, 10000+"");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final RandomSlidingWindowGenerator swg = injector.getInstance(RandomSlidingWindowGenerator.class);

    final List<Timescale> timescales = swg.generateSlidingWindows(10);
    Collections.sort(timescales);
    System.out.println("windlws: " + timescales);
    long buildStartTime = System.currentTimeMillis();
    Injector inj = getDependencyGraph(TimescaleParser.parseToString(timescales), IncrementalParallelDependencyGraphImpl.class,
        IncrementalPartialTimespans.class);
    final IncrementalParallelDependencyGraphImpl<Integer> dependencyGraph = inj.getInstance(IncrementalParallelDependencyGraphImpl.class);
    final PartialTimespans<Integer> partialTimespans = inj.getInstance(IncrementalPartialTimespans.class);
    long buildEndTime = System.currentTimeMillis();
    final Set<Timescale> timescaleSet = new HashSet<>(timescales);
    System.out.println((buildEndTime - buildStartTime) + "\t"
        + PeriodCalculator.calculatePeriodFromTimescales(timescaleSet));
    System.out.println("----------------- incremental build -------------------");
    long time = 0;
    for (int k = 1; k <= 1000; k++) {
      buildStartTime = System.currentTimeMillis();
      time = partialTimespans.getNextSliceTime(time);
      dependencyGraph.getFinalTimespans(time);
      buildEndTime = System.currentTimeMillis();
      System.out.println(k + "\t" + (buildEndTime - buildStartTime));
    }
    dependencyGraph.close();
*/
    // Experiment version

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinWindowSize.class, 200+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxWindowSize.class, 1200+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MinIntervalSize.class, 1+"");
    jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxIntervalSize.class, 100+"");
    //jcb.bindNamedParameter(RandomSlidingWindowGenerator.MaxPeriod.class, 10000+"");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final RandomSlidingWindowGenerator swg = injector.getInstance(RandomSlidingWindowGenerator.class);

    // numWindows
    for (int j = 0; j < 1; j++) {
      System.out.println("----------------- window " + (20*(j+1)) + "-------------------");
      for (int i = 0; i < 1; i++) {
        final List<Timescale> timescales = swg.generateSlidingWindows(20*(j+1));
        Collections.sort(timescales);
        //System.out.println("---------------------W=" + window);
        //System.out.println("--------------------------Type\tConstructionTime-------------------------------");

        long buildStartTime = System.currentTimeMillis();
        Injector inj = getDependencyGraph(TimescaleParser.parseToString(timescales), IncrementalParallelDependencyGraphImpl.class,
            IncrementalPartialTimespans.class);
        final IncrementalParallelDependencyGraphImpl<Integer> dependencyGraph = inj.getInstance(IncrementalParallelDependencyGraphImpl.class);
        final PartialTimespans<Integer> partialTimespans = inj.getInstance(IncrementalPartialTimespans.class);
        long buildEndTime = System.currentTimeMillis();
        final Set<Timescale> timescaleSet = new HashSet<>(timescales);
        System.out.println((buildEndTime - buildStartTime) + "\t"
            + PeriodCalculator.calculatePeriodFromTimescales(timescaleSet));
        System.out.println("----------------- incremental build -------------------");
        long time = 0;
        for (int k = 1; k <= 1000; k++) {
          buildStartTime = System.currentTimeMillis();
          time = partialTimespans.getNextSliceTime(time);
          dependencyGraph.getFinalTimespans(time);
          buildEndTime = System.currentTimeMillis();
          System.out.println(k + "\t" + (buildEndTime - buildStartTime));
        }
        dependencyGraph.close();
      }
    }

    /*
    final List<String> tss = new LinkedList<>();
    final List<String> periods = new LinkedList<>();
    final List<String> windows = new LinkedList<>();
    tss.add("(203,8)(250,25)(349,4)(353,50)(355,50)(374,200)(396,100)(400,10)(500,25)(612,5)(721,5)(753,100)(794,2)(813,8)(856,20)(879,625)(949,10)(1024,5)(1174,16)(1197,40)");
    periods.add("10000");
    windows.add("20");

    tss.add("(205,2)(218,2)(390,2)(392,200)(511,125)(558,5)(625,32)(646,200)(672,80)(840,1)(851,20)(915,250)(958,5)(1013,20)(1016,10)(1035,20)(1044,625)(1054,1000)(1137,5)(1187,160)");
    periods.add("20000");
    windows.add("20");

    tss.add("(294,5)(335,50)(475,16)(593,200)(627,5)(782,6)(819,300)(828,750)(841,625)(853,6)(856,10)(874,625)(948,1)(960,6)(989,120)(1067,80)(1096,30)(1127,15)(1183,750)(1199,1)");
    periods.add("30000");
    windows.add("20");

    tss.add("(248,100)(322,2)(465,50)(493,100)(509,125)(597,4)(617,10)(651,250)(660,160)(755,64)(828,4)(842,320)(903,625)(908,320)(936,400)(971,320)(1036,800)(1087,1)(1162,125)(1194,625)");
    periods.add("40000");
    windows.add("20");

    tss.add("(235,4)(387,16)(427,125)(456,1)(522,500)(525,250)(576,250)(588,5)(716,20)(719,5)(774,2)(779,1)(809,625)(865,16)(866,20)(899,20)(933,250)(981,25)(988,625)(1045,500)");
    periods.add("10000");
    windows.add("20");

    tss.add("(205,16)(269,2)(271,20)(282,25)(333,8)(356,200)(363,16)(379,50)(401,20)(440,8)(451,4)(568,80)(582,500)(585,50)(595,400)(596,2)(622,20)(689,10)(702,20)(709,10)(713,200)(759,400)(796,5)(809,20)(854,16)(856,4)(871,4)(879,250)(915,400)(916,200)(922,5)(931,400)(984,8)(986,625)(1076,400)(1121,400)(1139,4)(1153,20)(1161,25)(1182,5)");
    periods.add("10000");
    windows.add("40");

    tss.add("(249,5)(299,16)(336,80)(339,10)(359,10)(368,25)(399,100)(401,25)(409,8)(413,20)(423,4)(475,8)(501,25)(518,1)(558,80)(561,40)(587,500)(591,2)(613,100)(667,20)(677,500)(689,625)(694,500)(724,250)(730,200)(740,2)(755,8)(771,5)(778,1)(784,500)(787,5)(788,250)(833,100)(849,25)(852,2)(853,50)(867,400)(883,25)(885,25)(891,200)(892,200)(897,16)(903,40)(927,500)(963,5)(1004,8)(1007,5)(1025,1)(1034,400)(1036,40)(1057,50)(1065,625)(1076,25)(1088,25)(1098,400)(1099,500)(1124,625)(1183,100)(1188,625)(1190,5)");
    periods.add("10000");
    windows.add("60");

    tss.add("(215,1)(226,16)(248,10)(259,4)(273,16)(319,80)(331,250)(340,250)(341,40)(364,80)(391,4)(403,50)(423,40)(426,200)(428,10)(443,20)(449,2)(476,16)(494,40)(522,100)(528,16)(533,250)(550,50)(559,16)(569,5)(570,5)(576,50)(594,100)(595,16)(602,20)(610,500)(636,80)(656,50)(661,80)(725,16)(728,5)(742,100)(743,5)(757,5)(763,80)(775,100)(779,200)(780,2)(781,10)(784,100)(815,10)(817,80)(838,80)(845,20)(853,8)(873,400)(888,5)(901,200)(902,200)(904,4)(909,16)(910,25)(914,4)(923,40)(961,1)(962,1)(988,4)(1005,100)(1018,40)(1022,625)(1028,100)(1045,125)(1071,80)(1073,1)(1080,40)(1092,5)(1110,80)(1121,1000)(1146,10)(1154,16)(1157,4)(1171,40)(1172,50)(1184,40)(1195,2)");
    periods.add("10000");
    windows.add("80");

    for (int i = 0; i < tss.size(); i++) {
      final String p1 = tss.get(i);
      final String period = periods.get(i);
      final String window = windows.get(i);
      System.out.println("---------------------P="+ period + "\tW=" + window);
      System.out.println("--------------------------Type\tConstructionTime-------------------------------");

      long buildStartTime = System.currentTimeMillis();
      DependencyGraph<Integer> incrementParallelDG = getDependencyGraph(p1, IncrementalParallelDependencyGraphImpl.class,
          IncrementalPartialTimespans.class);
      long buildEndTime = System.currentTimeMillis();
      System.out.println("IncParallelDG\t" + (buildEndTime - buildStartTime));

      System.out.println("----------------IncParallelDG step & time. StepSize=10------------------");
      for (int j = 1; j <= 50; j++) {
        buildStartTime = System.currentTimeMillis();
        incrementParallelDG.getFinalTimespans(10 * j);
        buildEndTime = System.currentTimeMillis();
        System.out.println(j + "\t" + (buildEndTime - buildStartTime));
      }


      buildStartTime = System.currentTimeMillis();
      DependencyGraph<Integer> parallelStaticDG = getDependencyGraph(p1, StaticParallelDependencyGraphImpl.class,
          DefaultPartialTimespans.class);
      buildEndTime = System.currentTimeMillis();
      System.out.println("---------------------StaticParallelDG\t" + (buildEndTime - buildStartTime) + "----------------");


      buildStartTime = System.currentTimeMillis();
      DependencyGraph<Integer> staticDG = getDependencyGraph(p1, StaticDependencyGraphImpl.class,
          DefaultPartialTimespans.class);
      buildEndTime = System.currentTimeMillis();
      System.out.println("--------------------StaticDG\t" + (buildEndTime - buildStartTime) + "---------------------");
    }
    */

    /*
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


    System.out.println("--------------------------TS80: " + tsString80+"-----------------------------\n");
    System.out.println("--------------------------Type\tConstructionTime-------------------------------");

    buildStartTime = System.currentTimeMillis();
    incrementParallelDG = getDependencyGraph(tsString80, IncrementalParallelDependencyGraphImpl.class);
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
    parallelStaticDG = getDependencyGraph(tsString80, StaticParallelDependencyGraphImpl.class);
    buildEndTime = System.currentTimeMillis();
    System.out.println("---------------------StaticParallelDG\t" + (buildEndTime-buildStartTime) + "----------------");


    buildStartTime = System.currentTimeMillis();
    staticDG = getDependencyGraph(tsString80, StaticDependencyGraphImpl.class);
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
    */
  }
}
