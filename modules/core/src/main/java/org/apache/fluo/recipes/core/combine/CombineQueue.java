package org.apache.fluo.recipes.core.combine;

import java.util.Map;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.observer.ObserverProvider.Registry;

/**
 * @since 1.1.0
 */
public class CombineQueue<K,V> {
  
 public void add(TransactionBase tx, Map<K, V> updates) {
    
  }
  
  public void registerObserver(Registry obsRegistry, Combiner<K,V> combiner, ValueObserver<K, V> updateObserver){
    
  }
  
  public static interface TypesArgument {
    public BucketArgument types(String keyType, String valueType);
  }
  
  public static interface BucketArgument {
    Options buckets(int numBuckets);
  }
  
  public static interface Options {
    public Options bufferSize(long bufferSize);
    public Options bucketsPerTablet(int bucketsPerTablet);
    public void finish(FluoConfiguration fluoConfig);
  }
  
  public static TypesArgument configure(String id){
    return null;
  }
  
  
  public static <K2,V2> CombineQueue<K2,V2> getInstance(String id, SimpleConfiguration appConfig) {
    return null;
  }
  
 
}
