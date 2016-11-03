package com.ksyun.ks3.lib;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;

import com.ksyun.ks3.dto.Ks3ObjectSummary;
import com.ksyun.ks3.dto.ObjectListing;
import com.ksyun.ks3.exception.Ks3ClientException;
import com.ksyun.ks3.http.HttpClientConfig;
import com.ksyun.ks3.service.Ks3;
import com.ksyun.ks3.service.Ks3Client;
import com.ksyun.ks3.service.Ks3ClientConfig;
import com.ksyun.ks3.service.Ks3ClientConfig.PROTOCOL;
import com.ksyun.ks3.service.request.ListObjectsRequest;

public class SDKWrapper {
    private static Ks3 sClient;
    
    private static String sDataFilePath;
    private static String sMarkerStoreFile;
    private static String sDeleteErrorFile;
    
    public interface ListTester {
        public boolean shouldDelete(String key);
    }
    
    public static void initList(String endpoint, String ak, String sk, String dataFilePath, String listMarkerStoreFile) {
        SDKWrapper.initSDK(endpoint, ak, sk);
        sDataFilePath = dataFilePath;
        sMarkerStoreFile = listMarkerStoreFile;
    }
    
    public static void initDelete(String endpoint, String ak, String sk, String dataFilePath, String deleteErrorFile) {
        SDKWrapper.initSDK(endpoint, ak, sk);
        sDataFilePath = dataFilePath;
        sDeleteErrorFile = deleteErrorFile;
    }
    
    public static void initSDK(String endpoint, String ak, String sk) {
        Ks3ClientConfig config = new Ks3ClientConfig();
        config.setEndpoint(endpoint);
        config.setProtocol(PROTOCOL.http);
        config.setPathStyleAccess(true);

        HttpClientConfig hconfig = new HttpClientConfig();
        config.setHttpClientConfig(hconfig);
        
        sClient = new Ks3Client(ak, sk, config);
    }
    
    public static void resetList() {
        File f = new File(sDataFilePath);
        if (f.exists()) {
            f.delete();
        }
    }
    
    public static void resetErrorData() {
        File f = new File(sDeleteErrorFile);
        if (f.exists()) {
            f.delete();
        }
    }
    
    public static boolean listAndSaveObjects(String bucketName, ListTester tester) {
        ObjectListing list = null;
        FileOutputStream outStream = null;
        OutputStreamWriter os = null;
        BufferedWriter bw = null;
        String lastMarker = readFromFile(sMarkerStoreFile);
        
        int cnt = 0;
        
        try {
            outStream = new FileOutputStream(sDataFilePath, true);
            os = new OutputStreamWriter(outStream);
            bw = new BufferedWriter(os);
            
            // 初始化一个请求
            ListObjectsRequest request = new ListObjectsRequest(bucketName);
            if (lastMarker != null) {
                request.setMarker(lastMarker);
            }
            
            do{
                try {
                    list = sClient.listObjects(request);
                    if (list == null) {
                        return true;
                    }
                } catch (Ks3ClientException ec) {
                    ec.printStackTrace();
                    if (lastMarker != null) {
                        saveToFile(sMarkerStoreFile, lastMarker);
                    }
                    return false;
                }
                
                List<Ks3ObjectSummary> objects = list.getObjectSummaries();
                for (Ks3ObjectSummary ks3ObjectSummary : objects) {
                    String key = ks3ObjectSummary.getKey();
                    
                    if (tester.shouldDelete(key)) {
                        cnt ++;
                        bw.write(key);
                        bw.newLine();
                    }
                }
                
                //isTruncated为true时表示之后还有object，所以应该继续循环
                if(list.isTruncated()) {
                    lastMarker = objects.get(objects.size() - 1).getKey();
                    request.setMarker(lastMarker);
                }
                
                // 持久化marker
                if (lastMarker != null) {
                    saveToFile(sMarkerStoreFile, lastMarker);
                }
            } while(list.isTruncated());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("CNT: " + cnt);
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outStream != null) {
                try {
                    outStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        
        return true;
    }
    
    public static void deleteObjects(String bucketName) {
        File f = new File(sDataFilePath);
        if (!f.exists()) {
            System.out.println("Object data file not exists");
            return;
        }
        
        int cnt = 0;
        int errCnt = 0;
        
        FileInputStream fis = null;
        InputStreamReader is = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(f);
            is = new InputStreamReader(fis);
            br = new BufferedReader(is);

            String line = br.readLine();
            while (line != null) {
                cnt ++;
                try {
                    sClient.deleteObject(bucketName, line);
                    System.out.println("deleted file: '" + line + "' [" + cnt + "]");
                } catch (Ks3ClientException e) {
                    errCnt ++;
                    appendToFile(sDeleteErrorFile, line);
                    System.out.println("delete file '" + line + "' failed, write to error data file [" + cnt + "]");
                }
                
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Success: " + (cnt - errCnt));
            System.out.println("Error: " + errCnt);
            
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * 帮助方法
     */
    private static String readFromFile(String fileName) {
        File f = new File(fileName);
        if (!f.exists()) {
            return null;
        }
        
        FileInputStream fis = null;
        InputStreamReader is = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(f);
            is = new InputStreamReader(fis);
            br = new BufferedReader(is);
            return br.readLine();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private static void saveToFile(String fileName, String data) {
        FileOutputStream fos = null;
        OutputStreamWriter os = null;
        BufferedWriter bw = null;
        try {
            fos = new FileOutputStream(fileName);
            os = new OutputStreamWriter(fos);
            bw = new BufferedWriter(os);
            bw.write(data);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private static void appendToFile(String fileName, String data) {
        FileOutputStream fos = null;
        OutputStreamWriter os = null;
        BufferedWriter bw = null;
        try {
            fos = new FileOutputStream(fileName, true);
            os = new OutputStreamWriter(fos);
            bw = new BufferedWriter(os);
            bw.write(data);
            bw.newLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
