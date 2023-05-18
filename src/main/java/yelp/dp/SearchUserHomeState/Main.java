package yelp.dp.SearchUserHomeState;

// 하둡 라이브러리
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        // usage : hadoop jar ... [path: business.json file path in local] \
        //                        [path: review.json file path in local] \
        //                        [path: user.json file path in local] \
        //                        [path: working directory in hdfs]

        System.out.println("###############################");
        System.out.println("   Search User's Home State    ");
        System.out.println("###############################");

        Configuration conf = new Configuration(); // 설정 정의

        FileSystem hdfs = FileSystem.get(conf); // hdfs에 접근할 수 있도록 새로운 객체 생성

        // 로컬에 있는 파일을 hdfs 내로 복사
        System.out.print("Copy local files to hdfs...    ");
        if(!hdfs.exists(new Path(args[3], "input"))) { hdfs.mkdirs(new Path(args[3], "input")); } // hdfs 내의 input 폴더가 없다면 생성
        Path[] localFiles = {new Path(args[0]), new Path(args[1]), new Path(args[2])}; // 복사할 파일들
        hdfs.copyFromLocalFile(false, true, localFiles, new Path(args[3], "input")); // 파일들을 hdfs로 복사 및 덮어쓰기
        System.out.println("Success!");
        
        // hdfs 내에 이미 visited_users_output 폴더가 존재한다면 삭제
        if (hdfs.exists(new Path(args[3], "visited_users_output"))) {
            System.out.println("Delete visited_users_output folder in hdfs.");
            hdfs.delete(new Path(args[3], "visited_users_output"), true);
        }
        // hdfs 내에 이미 output 폴더가 존재한다면 삭제
        if (hdfs.exists(new Path(args[3], "output"))) {
            System.out.println("Delete output folder in hdfs.");
            hdfs.delete(new Path(args[3], "output"), true);
        }

        // 첫 번째 잡 생성 및 설정
        // 각 가게에 다녀간 손님 리스트
        Job job1 = Job.getInstance(conf, "Search User's Home State - 1");
        job1.setJarByClass(VisitedUsersByBusiness.class); // Job 클래스 설정
        job1.setReducerClass(VisitedUsersByBusiness.Reduce.class); // Reducer 클래스 설정

        job1.setMapOutputKeyClass(Text.class); // Mapper의 ouput key 자료형 설정
        job1.setMapOutputValueClass(Text.class); // Mapper의 output value 자료형 설정
        job1.setOutputKeyClass(NullWritable.class); // Reducer의 output key 자료형 설정
        job1.setOutputValueClass(Text.class); // Reducer의 output value 자료형 설정

        MultipleInputs.addInputPath(job1, new Path(new Path(args[3], "input"), new Path(args[0]).getName()), TextInputFormat.class, VisitedUsersByBusiness.BusinessMapper.class); // 비즈니스 데이터셋
        MultipleInputs.addInputPath(job1, new Path(new Path(args[3], "input"), new Path(args[1]).getName()), TextInputFormat.class, VisitedUsersByBusiness.ReviewMapper.class); // 리뷰 데이터셋
        FileOutputFormat.setOutputPath(job1, new Path(args[3], "visited_users_output")); // output file 설정
        if(job1.waitForCompletion(true) == false) { // job1을 돌리는 중 문제가 발생하면 프로그램 중단
            System.exit(1); // 비정상 종료
        }

        System.out.println("First job is finished! Starting Second Job...");


        // 두 번째 잡 생성 및 설정
        // 유저 데이터셋에 유저가 가장 많이 방문한 가게의 주(state)를 추가
        Job job2 = Job.getInstance(conf, "Search User's Home State - 2");
        job2.setJarByClass(UsersHomeState.class); // Job 클래스 설정
        job2.setReducerClass(UsersHomeState.Reduce.class); // Reducer 클래스 설정

        job2.setMapOutputKeyClass(Text.class); // Mapper의 ouput key 자료형 설정
        job2.setMapOutputValueClass(Text.class); // Mapper의 output value 자료형 설정
        job2.setOutputKeyClass(NullWritable.class); // Reducer의 output key 자료형 설정
        job2.setOutputValueClass(Text.class); // Reducer의 output value 자료형 설정

        MultipleInputs.addInputPath(job2, new Path(new Path(args[3], "input"), new Path(args[2]).getName()), TextInputFormat.class, UsersHomeState.UserMapper.class); // 유저 데이터셋
        MultipleInputs.addInputPath(job2, new Path(new Path(args[3], "visited_users_output"), "part-r-00000"), TextInputFormat.class, UsersHomeState.Job1Mapper.class); // Job1 데이터셋
        FileOutputFormat.setOutputPath(job2, new Path(args[3], "output")); // 최종 output file 설정

        // mapreduce 작업이 끝날 떄까지 대기 후 작업 실시
        if(job2.waitForCompletion(true)) {
            hdfs.delete(new Path(args[3], "input"), true); // hdfs 내의 입력 데이터 삭제

            // 결과 파일을 로컬로 복사
            System.out.println("Copy output folder to local working directory.");
            hdfs.copyToLocalFile(false, new Path(args[3], "output"), new Path(System.getProperty("user.dir"))); // hdfs 내의 원본 ouput 폴더는 유지
            System.exit(0); // 정상 종료
        }
        System.exit(1); // 비정상 종료
    }
}
