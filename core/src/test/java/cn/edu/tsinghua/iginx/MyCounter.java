package cn.edu.tsinghua.iginx;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MyCounter {

  public static void main(String[] args) throws IOException {
    String path = "/Users/cauchy-ny/Downloads/benchmark-3.log";
    FileInputStream inputStream = new FileInputStream(path);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

    int time = 300000;
    int offset = time;
    int count = 0;

    String str;
    List<Integer> list = new ArrayList<>();
    while ((str = reader.readLine()) != null) {
      String[] arr = str.split(",");
      if (arr.length != 3) {
        continue;
      }
      int curTime = Integer.parseInt(arr[2]);
      if (curTime < time) {
        count++;
      } else {
        list.add(count);
        count = 1;
        time += offset;
      }
    }
    list.add(count);

    list.forEach(System.out::println);
  }
}
