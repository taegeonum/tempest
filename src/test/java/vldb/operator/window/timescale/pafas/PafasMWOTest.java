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
import vldb.operator.window.timescale.pafas.event.WindowTimeEvent;
import vldb.operator.window.timescale.pafas.vldb2018.dynamic.DynamicFastMWOConfiguration;
import vldb.operator.window.timescale.pafas.vldb2018.singlethread.MultiThreadFinalAggregator;
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
    jcb.bindNamedParameter(MultiThreadFinalAggregator.ParallelThreshold.class, "180");

    final long currTime = 0;
    final List<Configuration> configurationList = new LinkedList<>();
    final List<String> operatorIds = new LinkedList<>();
    //final String timescaleString =  "(4,2)(5,3)(6,4)(10,5)";
    //final String timescaleString =  "(5,1)(8,3)(12,7)(16,6)(21,8)";
    //final String timescaleString = "(5,1)(10,1)(20,2)(30,2)(60,4)(90,4)(360,5)(600,5)(900,10)(1800,10)";
    //final String timescaleString = "(5,1)(10,1)(20,2)(30,2)(60,4)(90,4)";
    final String timescaleString = "(5,1)(6,2)(10,2)";
    //final String timescaleString = "(107,60)(170,1)(935,10)(1229,10)(1991,110)(2206,20)(2284,140)(2752,30)(2954,88)(2961,165)(2999,60)(3043,55)(3076,35)(3134,110)(3161,210)(3406,40)(3515,385)(3555,40)(3590,210)(3593,840)";
    final String timescaleString0 = "(107,60)(170,1)(179,11)(411,15)(656,15)(868,140)(886,15)(915,40)(935,10)(1229,10)(1396,20)(1430,20)(1828,60)(1991,110)(2032,22)(2150,30)(2206,20)(2262,120)(2284,140)(2344,40)(2534,140)(2752,30)(2812,40)(2843,35)(2954,88)(2961,165)(2999,60)(3027,42)(3043,55)(3076,35)(3110,70)(3134,110)(3148,35)(3161,210)(3166,30)(3289,55)(3303,35)(3317,110)(3404,35)(3406,40)(3409,35)(3450,165)(3515,385)(3530,120)(3543,35)(3555,40)(3558,40)(3566,660)(3590,210)(3593,840)";
    //final String timescaleString = "(3,2)(4,1)(6,3)";

    // 250
    //final String timescaleString = "(31,4)(74,50)(79,5)(82,60)(124,1)(140,120)(146,1)(148,90)(154,10)(183,50)(205,5)(210,75)(243,5)(248,5)(279,20)(358,250)(371,150)(381,375)(384,10)(405,150)(433,90)(448,225)(465,15)(473,120)(476,36)(489,375)(493,360)(510,300)(512,120)(541,100)(614,60)(633,450)(644,100)(650,10)(659,150)(668,60)(701,250)(702,600)(710,125)(713,180)(727,45)(732,75)(740,100)(742,600)(759,25)(764,225)(771,120)(792,75)(800,30)(804,180)(817,10)(878,75)(895,450)(896,20)(937,500)(938,125)(941,90)(971,250)(982,100)(993,450)(1003,36)(1010,900)(1034,100)(1069,1000)(1072,45)(1086,75)(1106,75)(1117,120)(1131,20)(1132,10)(1133,200)(1148,1000)(1155,180)(1174,10)(1181,125)(1190,20)(1212,100)(1225,40)(1266,300)(1272,100)(1288,500)(1330,120)(1334,600)(1345,60)(1365,900)(1378,375)(1387,90)(1388,75)(1390,30)(1403,500)(1410,75)(1415,15)(1428,40)(1433,45)(1448,450)(1458,500)(1462,30)(1463,300)(1466,100)(1490,75)(1501,750)(1510,180)(1513,150)(1527,180)(1528,600)(1542,125)(1560,125)(1568,75)(1571,15)(1641,250)(1656,600)(1662,750)(1672,225)(1685,120)(1689,20)(1733,120)(1747,450)(1772,120)(1779,30)(1781,20)(1783,360)(1811,360)(1815,20)(1831,375)(1836,1500)(1869,360)(1872,25)(1885,450)(1916,50)(1919,1800)(1936,1125)(1938,750)(1943,45)(1952,150)(1959,40)(1965,1500)(1969,450)(1978,1800)(1982,40)(1987,900)(1995,45)(1997,375)(2023,125)(2024,1125)(2031,360)(2060,40)(2068,90)(2077,360)(2104,60)(2120,200)(2122,1125)(2164,125)(2170,250)(2173,300)(2183,90)(2228,125)(2235,500)(2256,100)(2287,25)(2299,120)(2313,900)(2322,450)(2325,100)(2358,250)(2377,50)(2400,75)(2402,1500)(2406,200)(2426,125)(2471,40)(2486,180)(2499,360)(2510,1000)(2527,750)(2565,60)(2573,90)(2575,150)(2582,60)(2589,500)(2608,125)(2613,100)(2642,450)(2646,600)(2661,750)(2664,1000)(2676,600)(2710,45)(2713,45)(2718,120)(2733,60)(2734,1500)(2737,250)(2764,900)(2818,30)(2845,40)(2851,120)(2856,30)(2901,1000)(2906,375)(2921,60)(2925,45)(2941,2250)(2943,90)(2946,75)(2948,1125)(2965,1800)(2971,360)(2991,45)(2995,600)(3039,45)(3058,375)(3071,1500)(3075,3000)(3083,180)(3091,1000)(3125,1000)(3132,225)(3190,375)(3193,375)(3200,150)(3216,50)(3238,900)(3239,1500)(3240,500)(3247,3000)(3248,50)(3252,2250)(3254,120)(3263,200)(3265,750)(3274,360)(3295,500)(3300,450)(3310,450)(3321,200)(3330,450)(3335,1500)(3359,1500)(3378,300)(3399,1500)(3414,250)(3419,375)(3420,150)(3428,1500)(3471,40)(3474,300)(3478,50)(3493,300)(3522,375)(3543,300)";
    // 300
    final String timescaleString300 = "(31,4)(74,50)(79,5)(82,60)(124,1)(128,20)(140,120)(146,1)(148,90)(154,10)(183,50)(205,5)(210,75)(243,5)(248,5)(279,20)(358,250)(371,150)(381,375)(384,10)(405,150)(431,50)(433,90)(448,225)(465,15)(466,40)(473,120)(476,36)(488,45)(489,375)(493,360)(510,300)(512,120)(517,500)(541,100)(601,360)(614,60)(626,20)(632,500)(633,450)(644,100)(650,10)(659,150)(661,225)(668,60)(678,125)(701,250)(702,600)(710,125)(713,180)(727,45)(732,75)(740,100)(742,600)(759,25)(764,225)(771,120)(777,12)(779,500)(792,75)(800,30)(804,180)(817,10)(821,360)(878,75)(895,450)(896,20)(905,72)(937,500)(938,125)(941,90)(971,250)(982,100)(993,450)(1003,36)(1010,900)(1034,100)(1051,600)(1069,1000)(1072,45)(1086,75)(1105,15)(1106,75)(1117,120)(1131,20)(1132,10)(1133,200)(1142,75)(1148,1000)(1155,180)(1174,10)(1179,120)(1181,125)(1190,20)(1212,100)(1218,24)(1225,40)(1251,900)(1266,300)(1272,100)(1288,500)(1330,120)(1334,600)(1345,60)(1365,900)(1378,375)(1381,20)(1387,90)(1388,75)(1390,30)(1403,500)(1410,75)(1415,15)(1419,75)(1428,40)(1433,45)(1448,450)(1458,500)(1462,30)(1463,300)(1466,100)(1490,75)(1501,750)(1510,180)(1513,150)(1527,180)(1528,600)(1542,125)(1560,125)(1568,75)(1571,15)(1633,15)(1634,45)(1641,250)(1656,600)(1662,750)(1672,225)(1685,120)(1689,20)(1716,20)(1721,1125)(1733,120)(1747,450)(1772,120)(1779,30)(1781,20)(1783,360)(1811,360)(1815,20)(1831,375)(1836,1500)(1869,360)(1872,25)(1885,450)(1904,750)(1916,50)(1919,1800)(1936,1125)(1938,750)(1943,45)(1952,150)(1956,25)(1959,40)(1965,1500)(1969,450)(1974,180)(1976,125)(1978,1800)(1982,40)(1987,900)(1995,45)(1997,375)(2023,125)(2024,1125)(2031,360)(2052,200)(2060,40)(2068,90)(2077,360)(2104,60)(2120,200)(2122,1125)(2138,375)(2164,125)(2170,250)(2173,300)(2183,90)(2196,360)(2211,1125)(2219,200)(2228,125)(2235,500)(2256,100)(2287,25)(2299,120)(2301,125)(2313,900)(2322,450)(2325,100)(2332,1000)(2358,250)(2377,50)(2400,75)(2402,1500)(2406,200)(2426,125)(2471,40)(2486,180)(2499,360)(2510,1000)(2527,750)(2565,60)(2573,90)(2575,150)(2582,60)(2589,500)(2608,125)(2613,100)(2642,450)(2646,600)(2661,750)(2664,1000)(2666,600)(2672,2250)(2676,600)(2710,45)(2713,45)(2718,120)(2733,60)(2734,1500)(2737,250)(2759,120)(2764,900)(2818,30)(2845,40)(2851,120)(2856,30)(2872,360)(2901,1000)(2906,375)(2921,60)(2925,45)(2941,2250)(2943,90)(2946,75)(2948,1125)(2965,1800)(2971,360)(2989,60)(2991,45)(2995,600)(3039,45)(3058,375)(3071,1500)(3075,3000)(3083,180)(3089,125)(3091,1000)(3125,1000)(3132,225)(3190,375)(3193,375)(3200,150)(3216,50)(3238,900)(3239,1500)(3240,500)(3247,3000)(3248,50)(3252,2250)(3254,120)(3263,200)(3265,750)(3274,360)(3295,500)(3300,450)(3305,375)(3310,450)(3318,90)(3321,200)(3330,450)(3332,1800)(3335,1500)(3359,1500)(3371,500)(3378,300)(3388,30)(3399,1500)(3414,250)(3419,375)(3420,150)(3428,1500)(3471,40)(3474,300)(3478,50)(3493,300)(3505,1800)(3516,900)(3522,375)(3543,300)";
    // PAFAS
    //final String timescaleString_ =  "(5,1)(10,1)(15,1)(20,1)(30,1)";

    // zipf-zipf 150
    //final String timescaleString = "(10,1)(11,4)(13,1)(14,2)(15,10)(16,1)(17,1)(18,10)(19,1)(20,1)(22,4)(26,1)(29,1)(30,20)(40,1)(41,1)(43,16)(47,1)(50,1)(51,4)(52,1)(63,1)(67,50)(73,1)(75,32)(78,1)(79,16)(80,1)(91,1)(95,5)(115,40)(122,1)(123,1)(126,10)(127,1)(128,1)(138,1)(148,50)(151,20)(153,2)(159,1)(169,110)(197,1)(215,5)(218,5)(233,44)(234,5)(235,4)(256,2)(280,55)(325,20)(364,1)(399,25)(401,20)(406,1)(412,10)(413,4)(420,4)(439,4)(443,8)(450,1)(451,5)(503,5)(508,22)(525,5)(542,25)(547,5)(565,50)(576,5)(582,20)(589,55)(605,25)(649,11)(650,10)(657,5)(679,16)(685,55)(740,10)(751,5)(767,5)(778,220)(788,10)(800,16)(848,440)(874,50)(877,5)(880,40)(891,10)(900,20)(919,80)(956,20)(958,20)(973,100)(1049,25)(1105,50)(1122,880)(1166,50)(1218,100)(1297,50)(1313,25)(1344,80)(1363,10)(1449,10)(1465,200)(1472,400)(1511,25)(1540,400)(1546,40)(1548,100)(1625,275)(1664,110)(1702,25)(1713,20)(1742,20)(1808,220)(1893,88)(1965,25)(1976,110)(1999,20)(2005,275)(2075,275)(2094,100)(2124,50)(2196,160)(2236,20)(2237,200)(2352,25)(2487,32)(2534,25)(2567,220)(2664,50)(2739,25)(2787,25)(2824,25)(2841,220)(2892,40)(2932,55)(2939,32)(2955,50)(2961,160)(3018,50)(3042,100)(3077,55)(3124,55)(3154,50)(3368,40)(3374,400)(3409,275)(3485,40)(3538,220)";
    // zipf-zipf 100
    //final String timescaleString = "(10,1)(11,4)(13,1)(14,2)(15,10)(16,1)(17,1)(20,1)(29,1)(30,20)(40,1)(50,1)(51,4)(52,1)(63,1)(67,50)(95,5)(122,1)(123,1)(151,20)(153,2)(159,1)(215,5)(218,5)(325,20)(364,1)(399,25)(401,20)(406,1)(412,10)(413,4)(420,4)(439,4)(443,8)(450,1)(451,5)(503,5)(508,22)(525,5)(542,25)(547,5)(565,50)(576,5)(582,20)(589,55)(657,5)(679,16)(685,55)(740,10)(778,220)(848,440)(877,5)(891,10)(900,20)(956,20)(973,100)(1049,25)(1105,50)(1166,50)(1218,100)(1297,50)(1313,25)(1344,80)(1363,10)(1472,400)(1540,400)(1546,40)(1548,100)(1625,275)(1713,20)(1742,20)(1808,220)(1893,88)(1965,25)(1976,110)(1999,20)(2005,275)(2094,100)(2124,50)(2196,160)(2236,20)(2237,200)(2487,32)(2534,25)(2567,220)(2664,50)(2787,25)(2824,25)(2892,40)(2932,55)(2939,32)(2955,50)(2961,160)(3042,100)(3077,55)(3124,55)(3154,50)(3368,40)(3409,275)(3485,40)";

    //final String timescaleString = "(5,1)";
    // 800
    //final String timescaleString = "(50,10)(51,25)(52,8)(53,50)(54,25)(55,10)(56,40)(57,50)(58,15)(59,45)(60,20)(61,60)(62,30)(63,30)(65,32)(66,25)(67,25)(68,1)(69,20)(71,50)(73,48)(75,2)(76,30)(78,75)(79,36)(80,30)(81,75)(83,75)(84,24)(85,45)(86,15)(89,45)(91,60)(92,40)(93,80)(94,75)(95,5)(96,5)(97,75)(98,50)(99,10)(104,100)(105,10)(107,1)(108,15)(109,25)(113,15)(114,75)(116,20)(117,20)(118,75)(119,75)(122,5)(123,30)(124,2)(126,80)(127,120)(129,6)(130,5)(132,5)(133,5)(134,15)(135,40)(136,90)(137,75)(138,5)(140,60)(142,45)(144,60)(148,45)(150,15)(151,100)(152,80)(153,40)(155,4)(165,45)(166,75)(171,150)(172,45)(173,40)(179,25)(180,50)(181,5)(182,9)(185,150)(192,90)(194,45)(197,40)(199,40)(201,15)(204,5)(207,72)(208,160)(210,9)(212,1)(214,10)(215,75)(219,9)(220,50)(225,60)(228,15)(235,40)(236,160)(238,10)(239,100)(241,90)(242,45)(244,240)(246,150)(247,225)(249,100)(252,80)(253,96)(255,75)(262,180)(264,80)(271,40)(272,45)(273,80)(277,90)(281,50)(282,200)(283,50)(287,5)(295,240)(296,45)(297,20)(300,20)(302,100)(304,240)(306,75)(309,80)(310,240)(314,60)(318,240)(319,75)(320,300)(321,180)(323,45)(324,15)(325,120)(329,120)(332,240)(335,120)(336,100)(339,200)(341,100)(347,80)(352,45)(353,180)(358,150)(360,300)(361,45)(362,180)(367,90)(375,80)(376,90)(379,5)(380,100)(389,80)(394,200)(397,180)(398,40)(401,400)(405,120)(407,360)(408,150)(409,225)(412,20)(417,360)(419,75)(424,150)(426,288)(430,50)(431,60)(436,90)(440,240)(441,240)(442,25)(445,400)(456,40)(466,25)(470,360)(472,16)(477,10)(480,120)(483,120)(488,10)(489,10)(491,450)(494,180)(496,160)(504,50)(508,150)(509,15)(512,15)(516,300)(517,480)(520,25)(524,180)(527,240)(533,480)(537,450)(546,180)(548,25)(555,150)(556,80)(563,288)(564,225)(568,15)(572,90)(574,300)(576,25)(582,50)(583,400)(584,150)(589,40)(594,240)(598,200)(603,240)(608,180)(609,600)(613,80)(614,30)(615,225)(616,180)(619,75)(621,240)(624,90)(640,20)(647,450)(648,80)(655,5)(667,90)(674,100)(694,50)(706,50)(709,200)(712,160)(714,225)(715,600)(717,50)(720,150)(729,225)(730,50)(737,600)(745,150)(746,60)(748,50)(750,180)(752,480)(755,150)(756,180)(761,300)(768,300)(775,100)(781,75)(783,20)(788,480)(791,180)(801,450)(818,40)(821,90)(826,160)(829,200)(831,450)(836,25)(843,60)(854,50)(868,15)(875,100)(878,150)(880,150)(884,450)(887,300)(889,100)(895,18)(897,25)(898,800)(899,400)(912,160)(918,360)(923,450)(924,150)(929,225)(939,300)(940,180)(943,300)(944,10)(947,180)(950,100)(954,150)(956,15)(963,800)(970,20)(973,100)(978,20)(979,200)(982,600)(985,15)(986,75)(989,900)(993,900)(998,600)(1003,180)(1006,40)(1009,75)(1013,20)(1022,30)(1025,120)(1034,600)(1036,480)(1040,25)(1055,480)(1061,180)(1063,180)(1069,360)(1084,25)(1086,450)(1087,300)(1094,900)(1100,45)(1105,800)(1111,150)(1117,25)(1123,10)(1129,400)(1132,45)(1133,150)(1142,45)(1155,100)(1166,200)(1171,30)(1172,300)(1175,150)(1178,360)(1185,480)(1194,25)(1206,50)(1207,45)(1211,25)(1216,90)(1224,20)(1230,720)(1231,75)(1233,360)(1236,15)(1239,400)(1243,10)(1245,300)(1250,75)(1255,100)(1263,300)(1264,200)(1269,400)(1270,600)(1280,50)(1281,300)(1284,360)(1287,200)(1294,1200)(1300,120)(1301,360)(1302,45)(1306,40)(1316,720)(1317,600)(1318,90)(1326,720)(1338,720)(1340,480)(1345,225)(1350,160)(1351,450)(1364,150)(1372,200)(1390,30)(1394,120)(1399,160)(1409,150)(1411,100)(1426,480)(1427,450)(1432,450)(1437,90)(1438,480)(1443,240)(1446,1440)(1469,225)(1471,1200)(1474,200)(1485,450)(1493,180)(1495,25)(1500,30)(1503,100)(1508,25)(1511,180)(1523,200)(1527,20)(1530,180)(1533,30)(1534,100)(1539,60)(1541,15)(1546,45)(1551,120)(1553,40)(1569,360)(1571,75)(1580,100)(1588,360)(1591,200)(1593,400)(1595,160)(1598,600)(1602,450)(1605,20)(1615,450)(1625,50)(1641,90)(1642,360)(1656,100)(1658,400)(1680,1200)(1698,30)(1700,1200)(1706,1200)(1710,20)(1714,90)(1715,20)(1727,400)(1747,720)(1752,1440)(1759,40)(1763,45)(1771,240)(1791,45)(1792,40)(1801,200)(1802,180)(1807,90)(1814,300)(1817,225)(1824,1440)(1831,1440)(1835,1200)(1837,150)(1843,25)(1845,1200)(1847,50)(1851,40)(1859,240)(1877,900)(1878,160)(1906,600)(1915,15)(1916,100)(1918,50)(1919,40)(1923,480)(1925,60)(1936,90)(1956,240)(1958,600)(1962,240)(1989,90)(1996,800)(2008,900)(2015,1440)(2018,45)(2020,360)(2021,20)(2023,60)(2031,180)(2034,200)(2035,600)(2049,100)(2053,40)(2057,20)(2065,120)(2099,150)(2117,300)(2124,1440)(2131,240)(2137,720)(2141,1800)(2154,450)(2159,50)(2166,600)(2178,400)(2179,450)(2189,400)(2201,480)(2210,360)(2231,30)(2264,200)(2271,160)(2277,90)(2283,45)(2296,900)(2315,25)(2316,360)(2317,60)(2322,225)(2344,80)(2348,1440)(2349,40)(2352,240)(2355,400)(2357,150)(2367,600)(2389,240)(2396,800)(2398,1200)(2399,900)(2407,288)(2414,25)(2415,100)(2426,480)(2434,90)(2437,200)(2443,150)(2445,2400)(2454,30)(2461,400)(2470,400)(2471,480)(2477,800)(2482,1200)(2491,80)(2503,240)(2511,240)(2516,200)(2517,900)(2518,720)(2519,40)(2534,360)(2536,180)(2549,120)(2550,180)(2555,50)(2584,40)(2592,80)(2609,1800)(2614,2400)(2621,720)(2622,180)(2639,50)(2641,30)(2670,400)(2686,150)(2687,80)(2703,900)(2710,480)(2733,30)(2745,400)(2760,45)(2781,800)(2788,400)(2808,2400)(2814,45)(2815,50)(2825,160)(2847,240)(2849,1800)(2866,1440)(2869,600)(2884,360)(2888,80)(2891,50)(2898,200)(2912,150)(2913,50)(2916,600)(2919,180)(2939,1800)(2943,1200)(2944,400)(2992,150)(2994,240)(3014,1200)(3016,40)(3026,160)(3030,720)(3060,160)(3061,240)(3068,900)(3083,30)(3089,120)(3095,900)(3137,480)(3153,75)(3164,450)(3169,120)(3170,180)(3173,450)(3180,360)(3182,60)(3183,225)(3194,48)(3200,900)(3210,1200)(3225,225)(3235,150)(3241,45)(3242,60)(3247,45)(3253,1800)(3266,200)(3289,180)(3295,200)(3300,100)(3322,60)(3325,400)(3357,1440)(3389,160)(3393,450)(3394,225)(3418,360)(3425,900)(3511,40)(3518,800)(3553,200)(3560,360)(3596,240)(3603,45)(3617,225)(3624,450)(3626,75)(3627,800)(3634,3600)(3696,90)(3697,120)(3708,360)(3720,720)(3726,160)(3747,240)(3765,300)(3779,400)(3806,90)(3812,1800)(3815,60)(3847,120)(3850,50)(3853,800)(3866,160)(3880,600)(3881,1800)(3908,450)(3946,75)(3948,225)(3953,800)(3962,150)(3978,1440)(3995,50)(3997,40)(4011,150)(4029,1440)(4032,240)(4034,900)(4036,900)(4041,120)(4056,2400)(4067,75)(4091,150)(4102,100)(4103,2400)(4105,1200)(4118,40)(4124,50)(4128,200)(4135,3600)(4160,100)(4165,100)(4167,360)(4178,1440)(4186,225)(4218,360)(4219,800)(4234,800)(4237,300)(4243,3600)(4268,160)(4284,150)(4289,100)(4342,800)(4363,60)(4380,1200)(4410,75)(4423,180)(4428,225)(4431,600)(4432,450)(4493,720)(4508,75)(4517,240)(4521,300)(4529,600)(4532,240)(4544,400)(4553,400)(4559,150)(4565,400)(4569,160)(4571,600)(4589,60)(4602,2400)(4604,225)(4620,1200)(4622,1800)(4626,50)(4680,90)(4681,3600)(4688,450)(4699,225)(4702,1200)(4711,720)(4735,225)(4741,600)(4751,800)(4755,600)(4779,450)(4784,50)(4827,1440)(4849,3600)(4886,720)(4889,800)(4920,1440)(4926,80)(4935,80)(4936,225)(4959,80)(4971,50)(4973,300)(4988,180)(5009,2400)(5021,240)(5024,60)(5025,1200)(5027,2400)(5034,150)(5068,400)(5113,225)(5120,480)(5133,800)(5152,150)(5165,1440)(5172,200)(5193,480)(5212,400)(5234,800)(5235,60)(5252,60)(5290,100)(5301,450)(5327,480)(5331,240)(5364,400)(5375,720)(5411,3600)(5413,600)(5421,160)(5436,200)(5438,300)(5441,200)(5456,1200)(5486,360)(5511,120)(5526,720)(5540,720)(5543,75)(5555,400)(5564,80)(5578,160)(5585,225)(5596,160)(5600,225)(5602,120)(5608,1200)(5627,450)(5633,600)(5644,720)(5648,180)(5652,800)(5690,225)(5691,480)(5698,450)(5784,400)(5800,120)(5817,60)(5819,900)(5833,800)(5920,600)(5923,80)(5933,450)(5934,720)(5936,100)(5955,225)(5971,100)";
    //final String timescaleString = "(2,1)(3,1)(4,1)(5,1)(6,1)(7,1)(8,1)(9,1)(10,1)(11,1)(12,1)(13,1)(14,1)(15,1)(16,1)(17,1)(18,1)(19,1)(20,1)";

    //final String timescaleString = "(2,1)(3,1)(4,1)(5,1)";
    //final String timescaleString = "(2,1)(3,1)(4,1)(5,1)(6,1)(7,1)(8,1)(9,1)(10,1)(11,1)(12,1)(13,1)(14,1)(15,1)(16,1)(17,1)(18,1)(19,1)(20,1)(21,1)(22,1)(23,1)(24,1)(25,1)(26,1)(27,1)(28,1)(29,1)(30,1)(31,1)(32,1)(33,1)(34,1)(35,1)(36,1)(37,1)(38,1)(39,1)(40,1)(41,1)(42,1)(43,1)(44,1)(45,1)(46,1)(47,1)(48,1)(49,1)(50,1)(51,1)(52,1)(53,1)(54,1)(55,1)(56,1)(57,1)(58,1)(59,1)(60,1)(61,1)(62,1)(63,1)(64,1)(65,1)(66,1)(67,1)(68,1)(69,1)(70,1)(71,1)(72,1)(73,1)(74,1)(75,1)(76,1)(77,1)(78,1)(79,1)(80,1)(81,1)(82,1)(83,1)(84,1)(85,1)(86,1)(87,1)(88,1)(89,1)(90,1)(91,1)(92,1)(93,1)(94,1)(95,1)(96,1)(97,1)(98,1)(99,1)(100,1)(101,1)(102,1)(103,1)(104,1)(105,1)(106,1)(107,1)(108,1)(109,1)(110,1)(111,1)(112,1)(113,1)(114,1)(115,1)(116,1)(117,1)(118,1)(119,1)(120,1)(121,1)(122,1)(123,1)(124,1)(125,1)(126,1)(127,1)(128,1)(129,1)(130,1)(131,1)(132,1)(133,1)(134,1)(135,1)(136,1)(137,1)(138,1)(139,1)(140,1)(141,1)(142,1)(143,1)(144,1)(145,1)(146,1)(147,1)(148,1)(149,1)(150,1)(151,1)(152,1)(153,1)(154,1)(155,1)(156,1)(157,1)(158,1)(159,1)(160,1)(161,1)(162,1)(163,1)(164,1)(165,1)(166,1)(167,1)(168,1)(169,1)(170,1)(171,1)(172,1)(173,1)(174,1)(175,1)(176,1)(177,1)(178,1)(179,1)(180,1)(181,1)(182,1)(183,1)(184,1)(185,1)(186,1)(187,1)(188,1)(189,1)(190,1)(191,1)(192,1)(193,1)(194,1)(195,1)(196,1)(197,1)(198,1)(199,1)(200,1)";

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


    configurationList.add(FlatFitCombinedMWOConfiguration.CONF
        .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, FastFitDPSelectionAlgorithm.class)
        .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, FlatFitCombinedDependencyGraph.class)
        .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, SingleThreadFinalAggregator.class)
        .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
        .set(FlatFitCombinedMWOConfiguration.IS_PARALLEL, "false")
        .build());
    operatorIds.add("FAST-fit");
*/

    configurationList.add(DynamicFastMWOConfiguration.CONF
        .set(DynamicFastMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(DynamicFastMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(DynamicFastMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-Dy");
/*
    configurationList.add(FlatFitCombinedMWOConfiguration.CONF
        .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, SimpleTreeHeightDPSelectionAlgorithm.class)
        .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, SimpleTreeHeightDependencyGraph.class)
        .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, MultiThreadImprovedFinalAggregator.class)
        .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
        .set(FlatFitCombinedMWOConfiguration.IS_PARALLEL, "true")
        .build());
    operatorIds.add("FAST-height");

    configurationList.add(StaticActiveSingleMWOConfiguration.CONF
        .set(StaticActiveSingleMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(StaticActiveSingleMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(StaticActiveSingleMWOConfiguration.SELECTION_ALGORITHM, ActiveDPSelectionAlgorithm.class)
        .set(StaticActiveSingleMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(StaticActiveSingleMWOConfiguration.DEPENDENCY_GRAPH, AdjustPartialDependencyGraph.class)
        .set(StaticActiveSingleMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-active");

    configurationList.add(FlatFitCombinedMWOConfiguration.CONF
        .set(FlatFitCombinedMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .set(FlatFitCombinedMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(FlatFitCombinedMWOConfiguration.SELECTION_ALGORITHM, FastFitDPSelectionAlgorithm.class)
        .set(FlatFitCombinedMWOConfiguration.OUTPUT_LOOKUP_TABLE, DPOutputLookupTableImpl.class)
        .set(FlatFitCombinedMWOConfiguration.DEPENDENCY_GRAPH, FastCuttyCombinedDependencyGraph.class)
        .set(FlatFitCombinedMWOConfiguration.FINAL_AGGREGATOR, SingleThreadFinalAggregator.class)
        .set(FlatFitCombinedMWOConfiguration.START_TIME, "0")
        .build());
    operatorIds.add("FAST-Cutty");

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
    operatorIds.add("CuttyP");

    configurationList.add(CuttyParallelMWOConfiguration.CONF
        .set(CuttyParallelMWOConfiguration.START_TIME, currTime)
        .set(CuttyParallelMWOConfiguration.CA_AGGREGATOR, CountByKeyAggregator.class)
        .set(CuttyParallelMWOConfiguration.INITIAL_TIMESCALES, timescaleString)
        .build());

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

    final int numKey = 100;
    final int numInput = 100000;
    final Random random = new Random();
    final int tick = numInput / 10000;
    int tickTime = 1;
    long stored = 0;
    for (i = 0; i < numInput; i++) {
      final int key = i % numKey;
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
