package com.junyoung.yelpdp;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

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

public class MergeJson {
    public static class JsonMapper extends Mapper<Object, Text, Text, Text> {
        String mergeKey = "";
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            mergeKey = conf.get("mergeKey"); // conf에서 병합키를 가져옴
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            String _key = json.get(mergeKey).getAsString(); // json 파일에서 기준키에 해당하는 value 값을 mapper의 output key로 반환
            context.write(new Text(_key), value);
        }
    }

    public static class JsonReducer extends Reducer<Text, Text, NullWritable, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ', ...)등을 그대로 유지

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JsonObject mergedJson = new JsonObject(); // 빈 json 생성

            // 같은 key 값으로 들어온 values의 각 요소(value)의 json key와 value를 mergedJson으로 복사
            for (Text value : values) {
                JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // value값으로 json 생성
                Set<Map.Entry<String, JsonElement>> entries = json.entrySet(); // json의 각 key와 value값을 집합으로 정의

                // 생성된 집합의 각 요소(key, value) 값을 mergedJson에 추가
                for(Map.Entry<String, JsonElement> entry: entries) {
                    mergedJson.add(entry.getKey(), json.get(entry.getKey()));
                }
            }
            context.write(NullWritable.get(), new Text(gson.toJson(mergedJson))); // NullWritable은 결과 파일을 조정하기 위함
        }
    }

    public static void main(String[] args) throws Exception {
        // usage : hadoop jar ... [path: file path in local] \
        //                        [path: file path in local] \
        //                        [key: keyword to merge two files] \
        //                        [path: working directory in hdfs]

        System.out.println("########################");
        System.out.println("  Merge Two Json Files  ");
        System.out.println("########################");

        Configuration conf = new Configuration(); // 설정 정의
        conf.set("mergeKey", args[2]); // 프로그램 인자 중 병합키를 conf에 저장
        conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");

        FileSystem hdfs = FileSystem.get(conf); // hdfs에 접근할 수 있도록 새로운 객체 생성

        // 로컬에 있는 파일을 hdfs 내로 복사
        System.out.print("Copy local files to hdfs...    ");
        Path[] localFiles = {new Path(args[0]), new Path(args[1])}; // 복사할 파일들
        if(!hdfs.exists(new Path(args[3], "input"))) { hdfs.mkdirs(new Path(args[3], "input")); } // hdfs 내의 input 폴더가 없다면 생성
        hdfs.copyFromLocalFile(false, true, localFiles, new Path(args[3], "input")); // 파일들을 hdfs로 복사 및 덮어쓰기
        System.out.println("Success!");
        
        // hdfs 내에 output 폴더가 존재한다면 삭제
        if (hdfs.exists(new Path(args[3], "output"))) {
            System.out.println("Delete output folder in hdfs.");
            hdfs.delete(new Path(args[3], "output"), true);
        }

        // 잡 생성 및 설정
        Job job = Job.getInstance(conf, "Merge Json");
        job.setJarByClass(MergeJson.class); // Job 클래스 설정
        job.setMapperClass(JsonMapper.class); // Mapper 클래스 설정
        job.setReducerClass(JsonReducer.class); // Reducer 클래스 설정

        job.setMapOutputKeyClass(Text.class); // Mapper의 ouput key 자료형 설정
        job.setMapOutputValueClass(Text.class); // Mapper의 output value 자료형 설정
        job.setOutputKeyClass(NullWritable.class); // Reducer의 output key 자료형 설정
        job.setOutputValueClass(Text.class); // Reducer의 output value 자료형 설정

        FileInputFormat.addInputPath(job, new Path(new Path(args[3], "input"), new Path(args[0]).getName())); // 첫번째 input file 설정
        FileInputFormat.addInputPath(job, new Path(new Path(args[3], "input"), new Path(args[1]).getName())); // 두번째 input file 설정
        FileOutputFormat.setOutputPath(job, new Path(args[3], "output")); // output file 설정

        // mapreduce 작업이 끝날 떄까지 대기 후 작업 실시
        if(job.waitForCompletion(true)) {
            hdfs.delete(new Path(args[3], "input"), true); // hdfs 내의 입력 데이터 삭제

            // 결과 파일을 로컬로 복사
            System.out.println("Copy output folder to local working directory.");
            hdfs.copyToLocalFile(false, new Path(args[3], "output"), new Path(System.getProperty("user.dir"))); // hdfs 내의 원본 ouput 폴더는 유지
            System.exit(0); // 정상 종료
        }
        System.exit(1); // 비정상 종료
    }
}