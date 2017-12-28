package vldb.operator.window.timescale.pafas;

import org.apache.reef.tang.*;
import org.junit.Test;
import vldb.evaluation.Metrics;
import vldb.example.DefaultExtractor;
import vldb.operator.window.aggregator.impl.CountByKeyAggregator;
import vldb.operator.window.aggregator.impl.KeyExtractor;
import vldb.operator.window.timescale.TimeWindowOutputHandler;
import vldb.operator.window.timescale.TimescaleWindowOperator;
import vldb.operator.window.timescale.flatfit.FlatFitMWOConfiguration;
import vldb.operator.window.timescale.pafas.active.ActiveDPSelectionAlgorithm;
import vldb.operator.window.timescale.pafas.active.AdjustPartialDependencyGraph;
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.pafas.vldb2018.FlatFitCombinedDependencyGraph;
import vldb.operator.window.timescale.pafas.vldb2018.FlatFitCombinedMWOConfiguration;
import vldb.operator.window.timescale.pafas.vldb2018.FastFitDPSelectionAlgorithm;
import vldb.operator.window.timescale.parameter.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Created by taegeonum on 4/22/16.
 */
public final class PafasMWOTest {
  private static final Logger LOG = Logger.getLogger(PafasMWOTest.class.getName());

  @Test
  public void testPafasMWO() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(KeyExtractor.class, DefaultExtractor.class);
    jcb.bindNamedParameter(NumThreads.class, "4");
    jcb.bindNamedParameter(ReusingRatio.class, "0.0");
    jcb.bindNamedParameter(WindowGap.class, "25");
    jcb.bindNamedParameter(SharedFinalNum.class, "1000000");
    jcb.bindNamedParameter(OverlappingRatio.class, "0.0");

    final long currTime = 0;
    final List<Configuration> configurationList = new LinkedList<>();
    final List<String> operatorIds = new LinkedList<>();
    final String timescaleString3 =  "(4,2)(5,3)(6,4)(10,5)";
    final String timescaleString4 =  "(5,4)(8,3)(12,7)(16,6)";
    final String timescaleString11 = "(5,1)(10,1)(20,2)(30,2)(60,4)(90,4)(360,5)(600,5)(900,10)(1800,10)";
    final String timescaleString6 = "(5,2)(6,2)(10,2)";
    final String timescaleString = "(107,60)(170,1)(935,10)(1229,10)(1991,110)(2206,20)(2284,140)(2752,30)(2954,88)(2961,165)(2999,60)(3043,55)(3076,35)(3134,110)(3161,210)(3406,40)(3515,385)(3555,40)(3590,210)(3593,840)";
    final String timescaleString0 = "(107,60)(170,1)(179,11)(411,15)(656,15)(868,140)(886,15)(915,40)(935,10)(1229,10)(1396,20)(1430,20)(1828,60)(1991,110)(2032,22)(2150,30)(2206,20)(2262,120)(2284,140)(2344,40)(2534,140)(2752,30)(2812,40)(2843,35)(2954,88)(2961,165)(2999,60)(3027,42)(3043,55)(3076,35)(3110,70)(3134,110)(3148,35)(3161,210)(3166,30)(3289,55)(3303,35)(3317,110)(3404,35)(3406,40)(3409,35)(3450,165)(3515,385)(3530,120)(3543,35)(3555,40)(3558,40)(3566,660)(3590,210)(3593,840)";
    final String timescaleString_ = "(5,1)(14,2)";

    // 250
    final String timescaleString250 = "(31,4)(74,50)(79,5)(82,60)(124,1)(140,120)(146,1)(148,90)(154,10)(183,50)(205,5)(210,75)(243,5)(248,5)(279,20)(358,250)(371,150)(381,375)(384,10)(405,150)(433,90)(448,225)(465,15)(473,120)(476,36)(489,375)(493,360)(510,300)(512,120)(541,100)(614,60)(633,450)(644,100)(650,10)(659,150)(668,60)(701,250)(702,600)(710,125)(713,180)(727,45)(732,75)(740,100)(742,600)(759,25)(764,225)(771,120)(792,75)(800,30)(804,180)(817,10)(878,75)(895,450)(896,20)(937,500)(938,125)(941,90)(971,250)(982,100)(993,450)(1003,36)(1010,900)(1034,100)(1069,1000)(1072,45)(1086,75)(1106,75)(1117,120)(1131,20)(1132,10)(1133,200)(1148,1000)(1155,180)(1174,10)(1181,125)(1190,20)(1212,100)(1225,40)(1266,300)(1272,100)(1288,500)(1330,120)(1334,600)(1345,60)(1365,900)(1378,375)(1387,90)(1388,75)(1390,30)(1403,500)(1410,75)(1415,15)(1428,40)(1433,45)(1448,450)(1458,500)(1462,30)(1463,300)(1466,100)(1490,75)(1501,750)(1510,180)(1513,150)(1527,180)(1528,600)(1542,125)(1560,125)(1568,75)(1571,15)(1641,250)(1656,600)(1662,750)(1672,225)(1685,120)(1689,20)(1733,120)(1747,450)(1772,120)(1779,30)(1781,20)(1783,360)(1811,360)(1815,20)(1831,375)(1836,1500)(1869,360)(1872,25)(1885,450)(1916,50)(1919,1800)(1936,1125)(1938,750)(1943,45)(1952,150)(1959,40)(1965,1500)(1969,450)(1978,1800)(1982,40)(1987,900)(1995,45)(1997,375)(2023,125)(2024,1125)(2031,360)(2060,40)(2068,90)(2077,360)(2104,60)(2120,200)(2122,1125)(2164,125)(2170,250)(2173,300)(2183,90)(2228,125)(2235,500)(2256,100)(2287,25)(2299,120)(2313,900)(2322,450)(2325,100)(2358,250)(2377,50)(2400,75)(2402,1500)(2406,200)(2426,125)(2471,40)(2486,180)(2499,360)(2510,1000)(2527,750)(2565,60)(2573,90)(2575,150)(2582,60)(2589,500)(2608,125)(2613,100)(2642,450)(2646,600)(2661,750)(2664,1000)(2676,600)(2710,45)(2713,45)(2718,120)(2733,60)(2734,1500)(2737,250)(2764,900)(2818,30)(2845,40)(2851,120)(2856,30)(2901,1000)(2906,375)(2921,60)(2925,45)(2941,2250)(2943,90)(2946,75)(2948,1125)(2965,1800)(2971,360)(2991,45)(2995,600)(3039,45)(3058,375)(3071,1500)(3075,3000)(3083,180)(3091,1000)(3125,1000)(3132,225)(3190,375)(3193,375)(3200,150)(3216,50)(3238,900)(3239,1500)(3240,500)(3247,3000)(3248,50)(3252,2250)(3254,120)(3263,200)(3265,750)(3274,360)(3295,500)(3300,450)(3310,450)(3321,200)(3330,450)(3335,1500)(3359,1500)(3378,300)(3399,1500)(3414,250)(3419,375)(3420,150)(3428,1500)(3471,40)(3474,300)(3478,50)(3493,300)(3522,375)(3543,300)";
    // 300
    final String timescaleString300 = "(31,4)(74,50)(79,5)(82,60)(124,1)(128,20)(140,120)(146,1)(148,90)(154,10)(183,50)(205,5)(210,75)(243,5)(248,5)(279,20)(358,250)(371,150)(381,375)(384,10)(405,150)(431,50)(433,90)(448,225)(465,15)(466,40)(473,120)(476,36)(488,45)(489,375)(493,360)(510,300)(512,120)(517,500)(541,100)(601,360)(614,60)(626,20)(632,500)(633,450)(644,100)(650,10)(659,150)(661,225)(668,60)(678,125)(701,250)(702,600)(710,125)(713,180)(727,45)(732,75)(740,100)(742,600)(759,25)(764,225)(771,120)(777,12)(779,500)(792,75)(800,30)(804,180)(817,10)(821,360)(878,75)(895,450)(896,20)(905,72)(937,500)(938,125)(941,90)(971,250)(982,100)(993,450)(1003,36)(1010,900)(1034,100)(1051,600)(1069,1000)(1072,45)(1086,75)(1105,15)(1106,75)(1117,120)(1131,20)(1132,10)(1133,200)(1142,75)(1148,1000)(1155,180)(1174,10)(1179,120)(1181,125)(1190,20)(1212,100)(1218,24)(1225,40)(1251,900)(1266,300)(1272,100)(1288,500)(1330,120)(1334,600)(1345,60)(1365,900)(1378,375)(1381,20)(1387,90)(1388,75)(1390,30)(1403,500)(1410,75)(1415,15)(1419,75)(1428,40)(1433,45)(1448,450)(1458,500)(1462,30)(1463,300)(1466,100)(1490,75)(1501,750)(1510,180)(1513,150)(1527,180)(1528,600)(1542,125)(1560,125)(1568,75)(1571,15)(1633,15)(1634,45)(1641,250)(1656,600)(1662,750)(1672,225)(1685,120)(1689,20)(1716,20)(1721,1125)(1733,120)(1747,450)(1772,120)(1779,30)(1781,20)(1783,360)(1811,360)(1815,20)(1831,375)(1836,1500)(1869,360)(1872,25)(1885,450)(1904,750)(1916,50)(1919,1800)(1936,1125)(1938,750)(1943,45)(1952,150)(1956,25)(1959,40)(1965,1500)(1969,450)(1974,180)(1976,125)(1978,1800)(1982,40)(1987,900)(1995,45)(1997,375)(2023,125)(2024,1125)(2031,360)(2052,200)(2060,40)(2068,90)(2077,360)(2104,60)(2120,200)(2122,1125)(2138,375)(2164,125)(2170,250)(2173,300)(2183,90)(2196,360)(2211,1125)(2219,200)(2228,125)(2235,500)(2256,100)(2287,25)(2299,120)(2301,125)(2313,900)(2322,450)(2325,100)(2332,1000)(2358,250)(2377,50)(2400,75)(2402,1500)(2406,200)(2426,125)(2471,40)(2486,180)(2499,360)(2510,1000)(2527,750)(2565,60)(2573,90)(2575,150)(2582,60)(2589,500)(2608,125)(2613,100)(2642,450)(2646,600)(2661,750)(2664,1000)(2666,600)(2672,2250)(2676,600)(2710,45)(2713,45)(2718,120)(2733,60)(2734,1500)(2737,250)(2759,120)(2764,900)(2818,30)(2845,40)(2851,120)(2856,30)(2872,360)(2901,1000)(2906,375)(2921,60)(2925,45)(2941,2250)(2943,90)(2946,75)(2948,1125)(2965,1800)(2971,360)(2989,60)(2991,45)(2995,600)(3039,45)(3058,375)(3071,1500)(3075,3000)(3083,180)(3089,125)(3091,1000)(3125,1000)(3132,225)(3190,375)(3193,375)(3200,150)(3216,50)(3238,900)(3239,1500)(3240,500)(3247,3000)(3248,50)(3252,2250)(3254,120)(3263,200)(3265,750)(3274,360)(3295,500)(3300,450)(3305,375)(3310,450)(3318,90)(3321,200)(3330,450)(3332,1800)(3335,1500)(3359,1500)(3371,500)(3378,300)(3388,30)(3399,1500)(3414,250)(3419,375)(3420,150)(3428,1500)(3471,40)(3474,300)(3478,50)(3493,300)(3505,1800)(3516,900)(3522,375)(3543,300)";
    // PAFAS
    //final String timescaleString_ =  "(5,1)(10,1)(15,1)(20,1)(30,1)";

    // zipf-zipf 150
    //final String timescaleString_zipf150 = "(10,1)(11,4)(13,1)(14,2)(15,10)(16,1)(17,1)(18,10)(19,1)(20,1)(22,4)(26,1)(29,1)(30,20)(40,1)(41,1)(43,16)(47,1)(50,1)(51,4)(52,1)(63,1)(67,50)(73,1)(75,32)(78,1)(79,16)(80,1)(91,1)(95,5)(115,40)(122,1)(123,1)(126,10)(127,1)(128,1)(138,1)(148,50)(151,20)(153,2)(159,1)(169,110)(197,1)(215,5)(218,5)(233,44)(234,5)(235,4)(256,2)(280,55)(325,20)(364,1)(399,25)(401,20)(406,1)(412,10)(413,4)(420,4)(439,4)(443,8)(450,1)(451,5)(503,5)(508,22)(525,5)(542,25)(547,5)(565,50)(576,5)(582,20)(589,55)(605,25)(649,11)(650,10)(657,5)(679,16)(685,55)(740,10)(751,5)(767,5)(778,220)(788,10)(800,16)(848,440)(874,50)(877,5)(880,40)(891,10)(900,20)(919,80)(956,20)(958,20)(973,100)(1049,25)(1105,50)(1122,880)(1166,50)(1218,100)(1297,50)(1313,25)(1344,80)(1363,10)(1449,10)(1465,200)(1472,400)(1511,25)(1540,400)(1546,40)(1548,100)(1625,275)(1664,110)(1702,25)(1713,20)(1742,20)(1808,220)(1893,88)(1965,25)(1976,110)(1999,20)(2005,275)(2075,275)(2094,100)(2124,50)(2196,160)(2236,20)(2237,200)(2352,25)(2487,32)(2534,25)(2567,220)(2664,50)(2739,25)(2787,25)(2824,25)(2841,220)(2892,40)(2932,55)(2939,32)(2955,50)(2961,160)(3018,50)(3042,100)(3077,55)(3124,55)(3154,50)(3368,40)(3374,400)(3409,275)(3485,40)(3538,220)";
    // zipf-zipf 100
    //final String timescaleString = "(10,1)(11,4)(13,1)(14,2)(15,10)(16,1)(17,1)(20,1)(29,1)(30,20)(40,1)(50,1)(51,4)(52,1)(63,1)(67,50)(95,5)(122,1)(123,1)(151,20)(153,2)(159,1)(215,5)(218,5)(325,20)(364,1)(399,25)(401,20)(406,1)(412,10)(413,4)(420,4)(439,4)(443,8)(450,1)(451,5)(503,5)(508,22)(525,5)(542,25)(547,5)(565,50)(576,5)(582,20)(589,55)(657,5)(679,16)(685,55)(740,10)(778,220)(848,440)(877,5)(891,10)(900,20)(956,20)(973,100)(1049,25)(1105,50)(1166,50)(1218,100)(1297,50)(1313,25)(1344,80)(1363,10)(1472,400)(1540,400)(1546,40)(1548,100)(1625,275)(1713,20)(1742,20)(1808,220)(1893,88)(1965,25)(1976,110)(1999,20)(2005,275)(2094,100)(2124,50)(2196,160)(2236,20)(2237,200)(2487,32)(2534,25)(2567,220)(2664,50)(2787,25)(2824,25)(2892,40)(2932,55)(2939,32)(2955,50)(2961,160)(3042,100)(3077,55)(3124,55)(3154,50)(3368,40)(3409,275)(3485,40)";
    /*
    configurationList.add(EagerMWOConfiguration.CONF
        .set(EagerMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(EagerMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(EagerMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
        .set(EagerMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(EagerMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-Active");


    configurationList.add(InterNodeMWOConfiguration.CONF
        .set(InterNodeMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(InterNodeMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(InterNodeMWOConfiguration.SELECTION_ALGORITHM, InterNodeDPSelectionAlgorithm.class)
        .set(InterNodeMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(InterNodeMWOConfiguration.DEPENDENCY_GRAPH, InterNodeDependencyGraph.class)
        .set(InterNodeMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-inter");
*/

    configurationList.add(FlatFitCombinedMWOConfiguration.CONF
        .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, FastFitDPSelectionAlgorithm.class)
        .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, FlatFitCombinedDependencyGraph.class)
        .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-fit");


    configurationList.add(StaticActiveSingleMWOConfiguration.CONF
        .set(StaticActiveSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(StaticActiveSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticActiveSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
        .set(StaticActiveSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(StaticActiveSingleMWOConfiguration.DEPENDENCY_GRAPH, AdjustPartialDependencyGraph.class)
        .set(StaticActiveSingleMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-active");

/*
    configurationList.add(StaticSingleMWOConfiguration.CONF
        .set(StaticSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(StaticSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticSingleMWOConfiguration.SELECTION_ALGORITHM, OntheflyStaticSelectionAlgorithm.class)
        .set(StaticSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(StaticSingleMWOConfiguration.DEPENDENCY_GRAPH, BuildingFromPartialGraphForVldb2018.class)
        .set(StaticSingleMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST");
*/
    configurationList.add(FlatFitMWOConfiguration.CONF
        .set(FlatFitMWOConfiguration.START_TIME, currTime)
        .set(FlatFitMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(FlatFitMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .build());
    operatorIds.add("FlatFIT");


    /*
    configurationList.add(CuttyMWOConfiguration.CONF
        .set(CuttyMWOConfiguration.START_TIME, currTime)
        .set(CuttyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(CuttyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .build());
    operatorIds.add("Cutty");
*/

    /*
    // On-the-fly operator
    configurationList.add(OntheflyMWOConfiguration.CONF
        .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(OntheflyMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("OntheFly");

    configurationList.add(ActiveDynamicMWOConfiguration.CONF
        .set(ActiveDynamicMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(ActiveDynamicMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(ActiveDynamicMWOConfiguration.SELECTION_ALGORITHM, DynamicDPTradeOffSelectionAlgorithm.class)
        .set(ActiveDynamicMWOConfiguration.OUTPUT_LOOKUP_TABLE, DynamicDPOutputLookupTableImpl.class)
        .set(ActiveDynamicMWOConfiguration.DYNAMIC_DEPENDENCY, DynamicOptimizedDependencyGraphImpl.class)
        .set(ActiveDynamicMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("Dynamic-FAST");
*/

    /*
    // PAFAS-Greedy
    configurationList.add(StaticMWOConfiguration.CONF
        .set(StaticMWOConfiguration.INITIAL_TIMESCALES, "(4,2)(5,3)(6,4)(10,5)")
        .set(StaticMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticMWOConfiguration.SELECTION_ALGORITHM, DPSelectionAlgorithm.class)
        .set(StaticMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("PAFAS-DP");



    // On-the-fly operator
    configurationList.add(OntheflyMWOConfiguration.CONF
        .set(OntheflyMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(OntheflyMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(OntheflyMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("OntheFly");

    // TriOPs
    configurationList.add(TriOpsMWOConfiguration.CONF
        .set(TriOpsMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(TriOpsMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(TriOpsMWOConfiguration.START_TIME, currTime)
        .build());
    operatorIds.add("TriOps");
*/

  
    int i = 0;
    final List<TimescaleWindowOperator> mwos = new LinkedList<>();
    final List<Metrics> aggregationCounters = new LinkedList<>();
    for (final Configuration conf : configurationList) {
      final Injector injector = Tang.Factory.getTang().newInjector(Configurations.merge(jcb.build(), conf));
      injector.bindVolatileInstance(TimeWindowOutputHandler.class, new LoggingHandler<>(operatorIds.get(i)));
      System.out.println("Creating " + operatorIds.get(i));
      final TimescaleWindowOperator<String, Map<String, Long>> mwo = injector.getInstance(TimescaleWindowOperator.class);
      System.out.println("Finished creation " + operatorIds.get(i));
      mwos.add(mwo);
      final Metrics metrics = injector.getInstance(Metrics.class);
      aggregationCounters.add(metrics);
      i += 1;
    }

    final int numKey = 10;
    final int numInput = 20000;
    final Random random = new Random();
    final int tick = numInput / 2000;
    int tickTime = 1;
    long stored = 0;
    for (i = 0; i < numInput; i++) {
      final int key = 1;
      for (final TimescaleWindowOperator mwo : mwos) {
        if (i % tick == 0) {
          mwo.execute(new WindowTimeEvent(tickTime));
        }
        mwo.execute(Integer.toString(key));
      }

      if (i % tick == 0) {
        tickTime += 1;
      }
    }

    for (final TimescaleWindowOperator mwo : mwos) {
      mwo.close();
    }

    i = 0;
    for (final TimescaleWindowOperator mwo : mwos) {
      final Metrics aggregationCounter = aggregationCounters.get(i);
      final long partialCount = aggregationCounter.partialCount;
      final long finalCount = aggregationCounter.finalCount;
      final long storedAgg = aggregationCounter.storedFinal + aggregationCounter.storedPartial;
      final String id = operatorIds.get(i);
      System.out.println(id + " aggregation count: partial: " + partialCount + ", final: " + finalCount
          + ", total: " + (partialCount + finalCount) + ", stored: " + storedAgg);
      i += 1;
    }
  }
}
