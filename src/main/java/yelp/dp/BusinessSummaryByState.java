package yelp.dp;

import java.io.IOException;
import java.util.Set;
import java.util.Map.Entry;
import java.util.HashMap;

// json 형식을 사용하기 위해 gson 라이브러리 이용
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

// 하둡 라이브러리
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BusinessSummaryByState {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            
            String _key = json.get("state").getAsString(); // json 파일에서 기준키에 해당하는 value 값을 mapper의 output key로 반환
            
            JsonObject _value = new JsonObject();
            JsonObject _valueCategories = new JsonObject(); // 각 카테고리를 담을 json

            _value.addProperty("count", 1); // 전체 개수

            // 카테고리 별 계산
            if(!json.get("categories").isJsonNull()) { // 가게에 카테고리가 있는 경우만 실행
                String[] categories = json.get("categories").getAsString().split(", "); // ', '를 기준으로 문자열을 분할함
                for (String category : categories) {
                    _valueCategories.addProperty(category, 1);
                }
            }
            _value.add("categories", _valueCategories); // value에 카테고리 별 계산을 한 결과를 넣음

            context.write(new Text(_key), new Text(_value.toString()));
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ', ...)등을 그대로 유지

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int countSummation = 0; // 현재 주의 가게의 합
            HashMap<String, Integer> categoriesHashMap = new HashMap<String, Integer>();

            for (Text value : values) {
                JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // value값으로 json 생성
                
                countSummation += json.get("count").getAsInt(); // 전체 개수 합

                Set<Entry<String, JsonElement>> categoriesEntries = json.get("categories").getAsJsonObject().entrySet(); // json의 categories를 각 key와 value값을 집합으로 정의
                for(Entry<String, JsonElement> categoriesEntry: categoriesEntries) {
                    categoriesHashMap.put(categoriesEntry.getKey(), categoriesHashMap.getOrDefault(categoriesEntry.getKey(), 0) + 1); // 각 카테고리에 대한 값이 있으면 해당 값에 +1을 하고, 없으면 1을 저장함
                }
            }

            JsonObject resultJson = new JsonObject(); // 결과 json 생성
            resultJson.addProperty("state", key.toString());
            resultJson.addProperty("count", countSummation);
            resultJson.add("categories", gson.toJsonTree(categoriesHashMap));
            context.write(NullWritable.get(), new Text(gson.toJson(resultJson)));
        }
    }

    public static void main(String[] args) throws Exception {
        // usage : hadoop jar ... [path: file path in local] \
        //                        [path: working directory in hdfs]

        System.out.println("###############################");
        System.out.println("  Summary of business by State ");
        System.out.println("###############################");

        Configuration conf = new Configuration(); // 설정 정의

        FileSystem hdfs = FileSystem.get(conf); // hdfs에 접근할 수 있도록 새로운 객체 생성

        // 로컬에 있는 파일을 hdfs 내로 복사
        System.out.print("Copy local files to hdfs...    ");
        if(!hdfs.exists(new Path(args[1], "input"))) { hdfs.mkdirs(new Path(args[1], "input")); } // hdfs 내의 input 폴더가 없다면 생성
        hdfs.copyFromLocalFile(false, true, new Path(args[0]), new Path(args[1], "input")); // 파일을 hdfs로 복사 및 덮어쓰기
        System.out.println("Success!");
        
        // hdfs 내에 output 폴더가 존재한다면 삭제
        if (hdfs.exists(new Path(args[1], "output"))) {
            System.out.println("Delete output folder in hdfs.");
            hdfs.delete(new Path(args[1], "output"), true);
        }

        // 잡 생성 및 설정
        Job job = Job.getInstance(conf, "Summary of business by State");
        job.setJarByClass(BusinessSummaryByState.class); // Job 클래스 설정
        job.setMapperClass(Map.class); // Mapper 클래스 설정
        job.setReducerClass(Reduce.class); // Reducer 클래스 설정

        job.setMapOutputKeyClass(Text.class); // Mapper의 ouput key 자료형 설정
        job.setMapOutputValueClass(Text.class); // Mapper의 output value 자료형 설정
        job.setOutputKeyClass(NullWritable.class); // Reducer의 output key 자료형 설정
        job.setOutputValueClass(Text.class); // Reducer의 output value 자료형 설정

        FileInputFormat.addInputPath(job, new Path(new Path(args[1], "input"), new Path(args[0]).getName())); // input file 설정
        FileOutputFormat.setOutputPath(job, new Path(args[1], "output")); // output file 설정

        // mapreduce 작업이 끝날 떄까지 대기 후 작업 실시
        if(job.waitForCompletion(true)) {
            hdfs.delete(new Path(args[1], "input"), true); // hdfs 내의 입력 데이터 삭제

            // 결과 파일을 로컬로 복사
            System.out.println("Copy output folder to local working directory.");
            hdfs.copyToLocalFile(false, new Path(args[1], "output"), new Path(System.getProperty("user.dir"))); // hdfs 내의 원본 ouput 폴더는 유지
            System.exit(0); // 정상 종료
        }
        System.exit(1); // 비정상 종료
    }
}
