package io.airbyte.integrations.destination.databend;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;


import com.alibaba.fastjson.JSONObject;

public class DatabendUtils {
    /**
     * 
     * @param code
     * @return health or not
     */
    public static boolean isSuccessStatusCode(int code) {
        return code < 300 && code >= 200;
      }
      /**
   * Returns a new thread pool configured with the default settings.
   *
   * @param threadNamePrefix prefix of the thread name
   * @param parallel the number of concurrency
   * @return A new thread pool configured with the default settings.
   */
  public static ThreadPoolExecutor createDefaultExecutorService(
    final String threadNamePrefix, final int parallel) {
  ThreadFactory threadFactory =
      new ThreadFactory() {
        private int threadCount = 1;

        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r);
          thread.setName(threadNamePrefix + threadCount++);
          return thread;
        }
      };
  return (ThreadPoolExecutor) Executors.newFixedThreadPool(parallel, threadFactory);
    }
    /**
     * Returns a list of headers from a json formatted json string 
     */
    public static Map<String, String> getHeaders(String json) {
        JSONObject jsonNode = JSONObject.parseObject(json);
        Set<String> keys = jsonNode.keySet();
        HashMap<String, String> headers = new HashMap<String, String>();
        for (String key : keys) {
            headers.put(key, jsonNode.getString(key));
        }
        return headers;
    }
    
}