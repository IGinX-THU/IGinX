package cn.edu.tsinghua.iginx.utils;

import java.util.Random;

public class AlgorithmUtils {

    private final static Random random = new Random(0);

    public static int[] washCard(int n) {
        int[] nums = new int[n];
        for (int i = 0; i < n; i++) {
            nums[i] = i;
        }
        for (int i = n - 1; i >= 0; i--) {
            int p = random.nextInt(i + 1);
            swap(nums, i, p);
        }
        return nums;
    }

    private static void swap(int[] nums, int i, int j) {
        int temp = nums[i];
        nums[i] = nums[j];
        nums[j] = temp;
    }

}
