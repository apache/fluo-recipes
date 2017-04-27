package org.apache.fluo.recipes.core.combine;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.observer.ObserverProvider.Registry;
import org.apache.fluo.recipes.core.map.CollisionFreeMap.Options;

/**
 * @since 1.1.0
 */
public class CombineQueue<K,V> {
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
  
  
  public static void main(String[] args) {
    CombineQueue.configure(null).types(null, null).buckets(13).bucketsPerTablet(5).finish(null);
  }
}
