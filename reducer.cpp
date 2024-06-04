#include "../include/MyPolygon.h"
#include "../include/Point.h"
#include "../include/util.h"
#include "../index/RTree.h"
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <geos/geom/Geometry.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/io/WKTReader.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

using namespace std;

using namespace std::chrono;


RTree<MyPolygon *, double, 2, double> rtree;

int main() {
    //ifstream inputFile(argv[1]);
    auto startTime = high_resolution_clock::now();

    auto startInitialTime1 = high_resolution_clock::now();

    //获取此reducer的id
    const char *taskId = getenv("mapreduce_task_id");
    string taskIdStr = taskId ? taskId : "Unknown Task ID";

    string line;
    map<int, int> pointCountMap;
    geos::io::WKTReader wktReader;
    //map<int, shared_ptr<geos::geom::Polygon>> polygonMap;
    vector<MyPolygon *> polygons;
    vector<Point *> points;

    map<string, int> keyValueCountMap; //映射每个key及其对应的value个数
    int totalKeysProcessed = 0;        //总key数
    int totalValuesProcessed = 0;      //总value数

    //cerr << "initial complete" << endl;
 
    //microseconds totalWKTReadTime(0);
    microseconds totalRTreeBuildTime(0);
    microseconds totalRTreeSearchTime(0);
    microseconds totalContainsTime(0);
    microseconds totalIOTime(0);
    //microseconds totalTransTime(0);
    microseconds forLoopDuration(0);
    microseconds totalInitialTime(0);
    microseconds totalStep1Time(0);
    microseconds totalStep2Time(0);
    microseconds totalStep3Time(0);    
    microseconds totalStep4Time(0);    


    string currentKey;

    auto endInitialTime1 = high_resolution_clock::now();
    totalInitialTime += duration_cast<microseconds>(endInitialTime1 - startInitialTime1);


    auto startTime4 = high_resolution_clock::now();

    while (true) {


        //cerr << "come in while" << endl;

        auto startTime1 = high_resolution_clock::now();

        auto ioStart1 = high_resolution_clock::now();
        
        if (!getline(cin, line)) {
        break;
        }

        //分离key与value
        stringstream ss(line);
        string key, value;
        getline(ss, key, '\t');
        getline(ss, value);

        auto ioEnd1 = high_resolution_clock::now();
        totalIOTime += duration_cast<microseconds>(ioEnd1 - ioStart1);

        //cerr << "while split1 complete" << endl;

        //当key值发生变化时进行点与多边形的判断
        if (!currentKey.empty() && key != currentKey) {

            for (const auto &point : points) {

                auto rTreeSearchStart1 = high_resolution_clock::now();

                //rtree过滤
                rtree.Search(
                    (double *)point, (double *)point,
                    [&pointCountMap, &point,
                     &totalContainsTime](MyPolygon *polygon) -> bool {

                        auto containsStart1 = high_resolution_clock::now();
                        //IDEAL判断
                        if (polygon->contain(*point, nullptr)) {
                            pointCountMap[polygon->id]++;
                        }
                        auto containsEnd1 = high_resolution_clock::now();
                        totalContainsTime += duration_cast<microseconds>(containsEnd1 - containsStart1);

                        return true;
                    });
                auto rTreeSearchEnd1 = high_resolution_clock::now();
                totalRTreeSearchTime += duration_cast<microseconds>(rTreeSearchEnd1 - rTreeSearchStart1);
            }

            auto forLoopStart1 = high_resolution_clock::now();

            points.clear();
            polygons.clear();


            //输出结果
            for (const auto &entry : pointCountMap) {
                cout << "Polygon ID: " << entry.first << ", Points count: " << entry.second << endl;
            }

            //更新处理过的key数
            totalKeysProcessed++;

            //清空rtree
            rtree.RemoveAll();
            pointCountMap.clear();

            auto forLoopEnd1 = high_resolution_clock::now();
            forLoopDuration += duration_cast<microseconds>(forLoopEnd1 - forLoopStart1);

        }


        auto endTime1 = high_resolution_clock::now();
        totalStep1Time += duration_cast<microseconds>(endTime1 - startTime1);


        auto startTime2 = high_resolution_clock::now();

        auto ioStart2 = high_resolution_clock::now();


        //key值没有变化时正常建立rtree
        currentKey = key;

        //更新该key对应的value数
        keyValueCountMap[key]++;
        //更新处理的总value数
        totalValuesProcessed++;

        //分离value中的不同部分
        stringstream valueStream(value);
        string idStr, wkt;
        getline(valueStream, idStr, ',');
        getline(valueStream, wkt);

        //cerr << "Processing line: " << line << endl;

        int id = stoi(idStr);
        string prefix = wkt.substr(0, 3);

        bool isPol = (prefix == "POL");
        bool isPoi = (prefix == "POI");

        auto ioEnd2 = high_resolution_clock::now();
        totalIOTime += duration_cast<microseconds>(ioEnd2 - ioStart2);


        //cerr << "while split2 complete" << endl;

        if (isPol) {

            //cerr << "come in polygon" << endl;

            auto ioStart3 = high_resolution_clock::now();

            wkt = wkt.substr(7);
            MyPolygon *p = new MyPolygon();
            size_t offset = 0;
            p = p->read_polygon(wkt.c_str(), offset);
            p->id = id;

            auto ioEnd3 = high_resolution_clock::now();
            totalIOTime += duration_cast<microseconds>(ioEnd3 - ioStart3);

            auto rTreeBuildStart = high_resolution_clock::now();

            rtree.Insert(p->getMBB()->low, p->getMBB()->high, p);
            p->rasterization(30);
            polygons.push_back(p);

            auto rTreeBuildEnd = high_resolution_clock::now();
            totalRTreeBuildTime += duration_cast<microseconds>(rTreeBuildEnd - rTreeBuildStart);

        } 

  
        if (isPoi) {

            //cerr << "come in point" << endl;

            auto ioStart4 = high_resolution_clock::now();

            wkt = wkt.substr(5);

            Point *source = new Point();
            size_t offset = 0;
            source->x = read_double(wkt.c_str(), offset);
            source->y = read_double(wkt.c_str(), offset);
            points.push_back(source);

            auto ioEnd4 = high_resolution_clock::now();
            totalIOTime += duration_cast<microseconds>(ioEnd4 - ioStart4);

        }

        auto endTime2 = high_resolution_clock::now();
        totalStep2Time += duration_cast<microseconds>(endTime2 - startTime2);
    }


    auto endTime4 = high_resolution_clock::now();
    totalStep4Time += duration_cast<microseconds>(endTime4 - startTime4);

    auto startTime3 = high_resolution_clock::now();



    //最后一个key需要单独处理
    if (!points.empty()) {

        for (const auto &point : points) {

            auto rTreeSearchStart2 = high_resolution_clock::now();

            //rtree过滤
            rtree.Search((double *)point, (double *)point,
                         [&pointCountMap, &point,
                          &totalContainsTime](MyPolygon *polygon) -> bool {

                             auto containsStart2 = high_resolution_clock::now();
                             //IDEAL判断
                             if (polygon->contain(*point, nullptr)) {
                                 pointCountMap[polygon->id]++;
                             }
                             auto containsEnd2 = high_resolution_clock::now();
                             totalContainsTime += duration_cast<microseconds>(containsEnd2 - containsStart2);

                             return true;
                         });
            auto rTreeSearchEnd2 = high_resolution_clock::now();
            totalRTreeSearchTime += duration_cast<microseconds>(rTreeSearchEnd2 - rTreeSearchStart2);
        }
        

        auto forLoopStart2 = high_resolution_clock::now();
        
        points.clear();
        polygons.clear();

        //输出结果
        for (const auto &entry : pointCountMap) {
            cout << "Polygon ID: " << entry.first << ", Points count: " << entry.second << endl;
        }

        //最后一个key时更新key数
        totalKeysProcessed++;

        auto forLoopEnd2 = high_resolution_clock::now(); 
        forLoopDuration += duration_cast<microseconds>(forLoopEnd2 - forLoopStart2);
    }


    auto endTime3 = high_resolution_clock::now();
    totalStep3Time += duration_cast<microseconds>(endTime3 - startTime3);


    //输出reducer的id
    cerr << "Reducer id: " << taskIdStr << endl;

    //输出key的个数以及对应的value个数
    cerr << "Reducer Keys Processed Number: " << totalKeysProcessed << endl;
    cerr << "Reducer Values processed Number: " << totalValuesProcessed << endl;

    auto forLoopStart3 = high_resolution_clock::now();

    for (const auto &entry : keyValueCountMap) {
        cerr << "Key id: " << entry.first << ", Values Processed: " << entry.second << endl;
    }

    auto forLoopEnd3 = high_resolution_clock::now(); 
    forLoopDuration += duration_cast<microseconds>(forLoopEnd3 - forLoopStart3);

    auto endTime = high_resolution_clock::now();

    cerr << "Reducer R-Tree Build Time: " << totalRTreeBuildTime.count() / 1000000.0 << "s" << endl;
    //总时间-精炼时间=使用rtree进行mbr的过滤时间
    cerr << "Reducer R-Tree Search Time: " << (totalRTreeSearchTime.count() - totalContainsTime.count()) / 1000000.0 << "s" << endl;
    cerr << "Reducer Contains Check Time: " << totalContainsTime.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer IO Read Time: " << totalIOTime.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer Total Reducer Time: " << duration_cast<microseconds>(endTime - startTime).count() / 1000000.0 << "s" << endl;
    cerr << "Reducer Forloop Cout Time: " << forLoopDuration.count() / 1000000.0 << "s" << endl; 
    cerr << "Reducer Initial Time: " << totalInitialTime.count() / 1000000.0 << "s" << endl; 

    cerr << "Reducer Step1 Time: " << totalStep1Time.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer Step2 Time: " << totalStep2Time.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer Step3 Time: " << totalStep3Time.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer Step4 Time: " << totalStep4Time.count() / 1000000.0 << "s" << endl;

    // 先转换为自epoch以来的时间，而后再转化为系统时间
    auto startTimeSystem = system_clock::time_point(duration_cast<system_clock::duration>(startTime.time_since_epoch()));
    auto endTimeSystem = system_clock::time_point(duration_cast<system_clock::duration>(endTime.time_since_epoch()));
    auto startTimeT = system_clock::to_time_t(startTimeSystem);
    auto endTimeT = system_clock::to_time_t(endTimeSystem);

    cerr << "Reducer Start time: " << put_time(localtime(&startTimeT), "%Y-%m-%d %H:%M:%S") << endl;
    cerr << "Reducer End time: " << put_time(localtime(&endTimeT), "%Y-%m-%d %H:%M:%S") << endl;

    return 0;
}
