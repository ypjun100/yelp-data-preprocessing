package yelp.dp.ExtractUSData;

import java.io.IOException;

// json 형식을 사용하기 위해 gson 라이브러리 이용
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

// 해당 클래스를 실행하기 전 input 비즈니스 데이터셋이 미국 비즈니스 데이터셋이어야 함
public class ReviewDataset {
    public static class BusinessMapper extends Mapper<Object, Text, Text, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            
            String _key = json.get("business_id").getAsString(); // json 파일에서 business_id 값을 mapper의 output key로 저장

            context.write(new Text(_key), new Text("us_business"));
        }
    }

    public static class ReviewMapper extends Mapper<Object, Text, Text, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            
            String _key = json.get("business_id").getAsString(); // json 파일에서 business_id 값을 mapper의 output key로 저장
            
            context.write(new Text(_key), new Text(value.toString()));
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ', ...)등을 그대로 유지

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Boolean isTargetState = false;
            for (Text value : values) {
                if (value.toString().equals("us_business")) { // 만약 미국 비즈니스 데이터가 존재한다면 실행
                    isTargetState = true;
                    break;
                }
            }
            
            if (isTargetState) {
                for (Text value : values) {
                    if (value.toString().equals("us_business")) { // 만약 미국 비즈니스 데이터가 존재한다면 실행
                        continue;
                    }
                    context.write(NullWritable.get(), value);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // usage : hadoop jar ... [path: us_business.json file path in local] \
        //                        [path: review.json file path in local] \
        //                        [path: working directory in hdfs]

        System.out.println("########################");
        System.out.println(" Extract US Review Data ");
        System.out.println("########################");

        Configuration conf = new Configuration(); // 설정 정의

        FileSystem hdfs = FileSystem.get(conf); // hdfs에 접근할 수 있도록 새로운 객체 생성

        // 로컬에 있는 파일을 hdfs 내로 복사
        System.out.print("Copy local files to hdfs...    ");
        Path[] localFiles = {new Path(args[0]), new Path(args[1])}; // 복사할 파일들
        if(!hdfs.exists(new Path(args[2], "input"))) { hdfs.mkdirs(new Path(args[2], "input")); } // hdfs 내의 input 폴더가 없다면 생성
        hdfs.copyFromLocalFile(false, true, localFiles, new Path(args[2], "input")); // 파일들을 hdfs로 복사 및 덮어쓰기
        System.out.println("Success!");
        
        // hdfs 내에 output 폴더가 존재한다면 삭제
        if (hdfs.exists(new Path(args[2], "output"))) {
            System.out.println("Delete output folder in hdfs.");
            hdfs.delete(new Path(args[2], "output"), true);
        }

        // 잡 생성 및 설정
        Job job = Job.getInstance(conf, "Extract US Review Data");
        job.setJarByClass(ReviewDataset.class); // Job 클래스 설정
        job.setReducerClass(Reduce.class); // Reducer 클래스 설정

        job.setMapOutputKeyClass(Text.class); // Mapper의 ouput key 자료형 설정
        job.setMapOutputValueClass(Text.class); // Mapper의 output value 자료형 설정
        job.setOutputKeyClass(NullWritable.class); // Reducer의 output key 자료형 설정
        job.setOutputValueClass(Text.class); // Reducer의 output value 자료형 설정

        MultipleInputs.addInputPath(job, new Path(new Path(args[2], "input"), new Path(args[0]).getName()), TextInputFormat.class, ReviewDataset.BusinessMapper.class); // 비즈니스 데이터셋
        MultipleInputs.addInputPath(job, new Path(new Path(args[2], "input"), new Path(args[1]).getName()), TextInputFormat.class, ReviewDataset.ReviewMapper.class); // 리뷰 데이터셋
        FileOutputFormat.setOutputPath(job, new Path(args[2], "output")); // output file 설정

        // mapreduce 작업이 끝날 떄까지 대기 후 작업 실시
        if(job.waitForCompletion(true)) {
            hdfs.delete(new Path(args[2], "input"), true); // hdfs 내의 입력 데이터 삭제

            // 결과 파일을 로컬로 복사
            System.out.println("Copy output folder to local working directory.");
            hdfs.copyToLocalFile(false, new Path(args[2], "output"), new Path(System.getProperty("user.dir"))); // hdfs 내의 원본 ouput 폴더는 유지
            System.exit(0); // 정상 종료
        }
        System.exit(1); // 비정상 종료
    }
}