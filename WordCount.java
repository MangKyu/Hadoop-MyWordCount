import java.awt.*;
import java.util.*;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyWordCount {
    public static class MyMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        // Mapper를 상속받음 <입력의 KEY, 입력의 VALUE, 출력의 KEY, 출력의 VALUE>

        private final static IntWritable one = new IntWritable(1);
        // static final 상수로 정의하여 공유. 1을 값으로 설정. 어떤 단어가 한 번 나올 때 (단어, 1)로 설정하기 위함

        private Text word = new Text();
        // 단어를 담는 용도. 이 위치에서 생성하는 게 성능 향상에 도움이 됨.
        // Map에 생성되면 Map이 호출될 때 마다(한 줄이 입력으로 올 때 마다)매번 객체생성. --> 미리 생성해둬서 성능 향상시킴

        @Override
        public void map(Object key, Text value, Context context)
            // Text value : 한줄 단위로 들어오는 map 함수의 입력
            // Context : MyMapper에서 처리한 결과를 Hadoop으로 전달하기 위한 연결고리

                throws IOException, InterruptedException {
            String line = value.toString();
            // Hadoop에서 사용하는 Text 자료구조를 Java의 String으로 바꾸어 처리

            StringTokenizer st = new StringTokenizer(line);
            // StringTokenizer(String, 구분자)로 한 줄을 분리함
            // 구분자가 없는 경우에는 모든 공백을 기준으로 입력을 분리
            // 공백을 기준으로 분리된 토큰을이 st 변수안에 저장되어 있음
            // ex) line = hello hadoop world 라고 하면, st에는[hello, hadoop, world]가 들어가있고
            // st.nextToken()할때마다 토큰을 st에서 하나씩 꺼냄

            // 토큰을 모아둔 st에 남은 토큰들이 있으면 반복문 실행
            while(st.hasMoreTokens()) {
                // 토큰들을 소문자로 변경하여 단어를 set 해준다.
                word.set(st.nextToken().toLowerCase());
                context.write(word, one);
                // word.set : 하나씩 담음
                // nextToken : 다음 토큰 가져옴
                // toLowerCase : 대소문자 구분 안하기 위해 모두 소문자로 변경

                // context.write(word, one) : 단어 기록 ex) 'hello'가 1번 나왔다 --> ('hello', 1)
            }
        }
    }


    public static class MyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Reducer를 상속받음 <입력의 KEY, 입력의 VALUE, 출력의 KEY, 출력의 VALUE>

        // Key로 들어온 입력들의 개수를 합하기 위한 변수
        private IntWritable sumWritable = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)
            // Key: 단어
            // Iterable<IntWritable>: 특정 단어에 대한 values들을 모아둔 반복자
            // Context: 단어의 개수를 계산한 sumWritable을 Hadoop에 전달하기 위한 연결고리

                throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            // 반복문 돌면서 IntWritable 하나씩 가져온다.

            sumWritable.set(sum);
            // Java의 자료혀에 저장된 단어의 총 개수를 Hadoop 자료형인 sumWritable에 저장

            // (단어, 총 개수)를 context에 저장하여 Hadoop으로 전달
            context.write(key, sumWritable);
        }
    }


    // Main method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Configuration : 환경설정을 위한 클래스

        Job job = Job.getInstance(conf, "WordCount");
        // Job : MapReduce의 Job

        // 작성한 WordCountClass를 job으로, MyMapper를 Mapper로, MyReducer를 Reducer로 설정
        job.setJarByClass(MyWordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Map 함수와 reduce 함수의 출력의 key, value를 설정
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 입력파일이 분할되어 분할된 블록을 읽어들이는 방식
        job.setInputFormatClass(TextInputFormat.class);

        // HDFS에 결과를 저장하기 위한 방식
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // 입력폴더
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // 출력폴더

        job.waitForCompletion(true);
    }
}
