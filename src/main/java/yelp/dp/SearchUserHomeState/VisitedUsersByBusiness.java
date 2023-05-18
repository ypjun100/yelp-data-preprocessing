package yelp.dp.SearchUserHomeState;

import java.io.IOException;

// json 형식을 사용하기 위해 gson 라이브러리 이용
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

// 하둡 라이브러리
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class VisitedUsersByBusiness {
    public static class BusinessMapper extends Mapper<Object, Text, Text, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        // business 데이터셋에서 state를 가져옴
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            
            String _key = json.get("business_id").getAsString(); // json 파일에서 business_id 값을 mapper의 output key로 저장
            String _value = json.get("state").getAsString(); // json 파일에서 business_id 값을 mapper의 output value로 저장

            context.write(new Text(_key), new Text("business, " + _value));
        }
    }

    public static class ReviewMapper extends Mapper<Object, Text, Text, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        // 리뷰 데이터셋에서 리뷰를 작성한 user_id를 가져옴
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            
            String _key = json.get("business_id").getAsString(); // json 파일에서 business_id 값을 mapper의 output key로 저장
            String _value = json.get("user_id").getAsString(); // json 파일에서 business_id 값을 mapper의 output value로 저장

            context.write(new Text(_key), new Text("review, " + _value));
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ', ...)등을 그대로 유지

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JsonObject json = new JsonObject();
            String visited_users = "";

            json.addProperty("business_id", key.toString());
            json.addProperty("state", "");
            
            for (Text value : values) {
                String[] identityAndValue = value.toString().split(", ");
                if(identityAndValue[0].equals("business")) {
                    json.addProperty("state", identityAndValue[1]);
                } else if(identityAndValue[0].equals("review")) {
                    if(visited_users.equals("")) { // 방문한 유저 데이터가 비어있다면
                        visited_users = identityAndValue[1]; // 방문한 유저에 추가
                    } else { // 아니면
                        visited_users += ", " + identityAndValue[1]; // 방문한 유저들 뒤에 추가
                    }
                } else {
                    System.out.println("Else case ocurred! " + identityAndValue[1]);
                }
            }

            json.addProperty("visited_users", visited_users); // 방문 유저들 저장

            context.write(NullWritable.get(), new Text(gson.toJson(json)));
        }
    }
}
