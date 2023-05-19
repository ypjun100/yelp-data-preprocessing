package yelp.dp.SearchUserHomeState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

// json 형식을 사용하기 위해 gson 라이브러리 이용
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

// 하둡 라이브러리
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UsersHomeState {
    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        // business 데이터셋에서 state를 가져옴
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            
            String _key = json.get("user_id").getAsString(); // json 파일에서 business_id 값을 mapper의 output key로 저장
            json.remove("friends"); // user 데이터에서 friends는 삭제함

            context.write(new Text(_key), new Text("user, " + json.toString()));
        }
    }

    public static class Job1Mapper extends Mapper<Object, Text, Text, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ' 등)을 그대로 유지

        // business 데이터셋에서 state를 가져옴
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject json = gson.fromJson(value.toString(), JsonObject.class); // input의 line으로 json 정의
            
            String businessState = json.get("state").toString().replaceAll("\"", ""); // 가게가 위치한 주(state)에 특수기호를 제외하고 저장
            String[] visistedUsers = json.get("visited_users").toString().split(", "); // 해당 가게에 방문한 손님들 배열
            for (String visitedUser : visistedUsers) { // 해당 가게를 방문한 유저들에 대해 해당 가게가 위치한 주(state)를 value 값으로 전달
                context.write(new Text(visitedUser), new Text("job1, " + businessState));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // html의 주요 특수문자(<, >, ', ...)등을 그대로 유지

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JsonObject json = new JsonObject();
            HashMap<String, Integer> userReviewStates = new HashMap<String, Integer>(); // 유저가 작성한 리뷰의 가게 주(state)

            for (Text value : values) {
                String[] identityAndValue = value.toString().split(", ", 2); // 2개의 요소로 split
                if(identityAndValue[0].equals("user")) {
                    Set<Entry<String, JsonElement>> userJson = gson.fromJson(identityAndValue[1], JsonObject.class).entrySet();
                    for (Entry<String, JsonElement> userJsonElement : userJson) {
                        json.add(userJsonElement.getKey(), userJsonElement.getValue());
                    }
                } else if(identityAndValue[0].equals("job1")) {
                    // 유저가 현재 주(state)의 가게들에 대한 리뷰를 작성개수 카운팅
                    userReviewStates.put(identityAndValue[1], userReviewStates.getOrDefault(identityAndValue[1], 0) + 1);
                }
            }

            if(json.has("user_id") && userReviewStates.size() > 0) { // 유저 정보와 주(state) 정보가 모두 들어온 경우에만 실행
                // 유저가 가장 많이 리뷰를 작성한 주(state)를 찾음
                String maximumReviewState = ""; // 주가 저장될 문자열 변수
                Integer maximumReviewStateCount = 0; // 리뷰의 수가 저장될 변수
                for (Entry<String, Integer> userReviewState : userReviewStates.entrySet()) {
                    if(userReviewState.getValue() > maximumReviewStateCount) {
                        maximumReviewState = userReviewState.getKey();
                        maximumReviewStateCount = userReviewState.getValue();
                    }
                }

                json.addProperty("home_state", maximumReviewState); // 가장 리뷰를 많이 작성한 주(state)를 사용자가 살고 있는 주로 정의하고 저장

                context.write(NullWritable.get(), new Text(gson.toJson(json)));
            }
        }
    }
}
