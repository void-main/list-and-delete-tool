package com.ksyun.ks3;

import com.ksyun.ks3.lib.SDKWrapper;
import com.ksyun.ks3.lib.SDKWrapper.ListTester;

public class ListApp {
    // 改为自己的REGION
    private static final String ENDPOINT = "kss.ksyun.com";
    
    // 使用自己的AK，SK
    private static final String AK = "";
    private static final String SK = "";
    
    // 填写要list的bucket
    private static final String BUCKETNAME = "";
    
    private static ListTester tester = new ListTester() {
        /**
         * 实现这个方法，进行自己的业务逻辑判断
         */
        public boolean shouldDelete(String key) {
            return key.startsWith("tmp1");
        }
    };
    
    public static void main(String[] args) {
        // 两个参数，1个是保存key的数据文件，第2个是记录循环过程的文件
        if (args.length < 2) {
            System.out.println("Usage: datafilepath listmarkerstore");
            return;
        }
        
        SDKWrapper.initList(ENDPOINT, AK, SK, args[0], args[1]);
        
        // 调用这个方法，会删除之前保存的数据文件
        SDKWrapper.resetList();
        
        // 开始列举文件，如果列举过程中发生异常，会返回false
        // 自动间隔10s中从上次出错的位置开始重试
        while(!SDKWrapper.listAndSaveObjects(BUCKETNAME, tester)) {
            System.out.println("LIST OBJECTS ERROR...");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        System.out.println("List objects done.");
    }

}
