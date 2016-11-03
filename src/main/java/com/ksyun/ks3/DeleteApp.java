package com.ksyun.ks3;

import com.ksyun.ks3.lib.SDKWrapper;


public class DeleteApp {
    // 改为自己的REGION
    private static final String ENDPOINT = "kss.ksyun.com";
    
    // 使用自己的AK，SK
    private static final String AK = "";
    private static final String SK = "";
    
    // 填写要list的bucket
    private static final String BUCKETNAME = "";

    public static void main(String[] args) {
        // 两个参数，1个是保存key的数据文件，第2个是记录删除失败的文件
        if (args.length < 2) {
            System.out.println("Usage: datafilepath errorfilepath");
            return;
        }
        
        SDKWrapper.initDelete(ENDPOINT, AK, SK, args[0], args[1]);
        
        // 调用这个方法会清空记录删除失败的文件
        SDKWrapper.resetErrorData();
        
        // 开始删除文件，删除过程中会打印日志，展示删除到哪个文件
        // 如果某个文件删除失败，会把key保存到errorfilepath那个中，这样可以后续再次运行
        SDKWrapper.deleteObjects(BUCKETNAME);
        
        System.out.println("Delete objects done.");
    }

}
