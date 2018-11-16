package mapreduce

import (
    "os"
    "encoding/json"
    "log"
    "sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFileName string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
    //
    // get all the intermediate files for reduceTask
    // loop through all the files
    //     read the file, and loop throught it to form a dictionary of string-[]string
    //         loop through the dictionary string-[]string
    //            output: =reduce(key, vaules)
    //            write it to the file, as JSON encoded keyValue
	//

    mapList := make([]int, nMap)
    outFile, err := os.OpenFile(outFileName, os.O_RDWR|os.O_CREATE, 0755)
    if err != nil {
        log.Println(err)
        os.Exit(1)
    }
    enc := json.NewEncoder(outFile)
    defer outFile.Close()
    keyMap := make(map[string][]string)

    for m, _ := range mapList {
        fileName := reduceName(jobName, m, reduceTask)

        file, err := os.Open(fileName)
        if err != nil {
            log.Println(err)
            os.Exit(1)
        }

        dec := json.NewDecoder(file)
        var kv KeyValue

        for ;; { 
            err = dec.Decode(&kv)
            if err != nil {
                break;
            }
            keyMap[kv.Key] = append(keyMap[kv.Key],kv.Value)
        }


        file.Close()
    }   

    var keys []string
    for k := range keyMap {
        keys = append(keys, k)
    }

    sort.Strings(keys)

        for _, k := range keys {
            result := reduceF(k, keyMap[k])
            enc.Encode(&KeyValue{k, result})
        }
}
