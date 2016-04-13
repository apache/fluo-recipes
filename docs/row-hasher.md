# Row hash prefix recipe

## Background

Transactions are implemented in Fluo using conditional mutations.  Conditional
mutations require server side processing on tservers.  If data is not spread
evenly, it can cause some tservers to execute more conditional mutations than
others.  These tservers doing more work can become a bottleneck.  Most real
world data is not uniform and can cause this problem.

Before the Fluo [Webindex example][1] started using this recipe it suffered
from this problem.  The example was using reverse dns encoded URLs for row keys
like `p:com.cnn/story1.html`.  This made certain portions of the table more
popular, which in turn made some tservers do much more work.  This uneven
distribution of work lead to lower throughput and uneven performance.  Using
this recipe made those problems go away.

## Solution

This recipe provides code to help add a hash of the row as a prefix of the row.
Using this recipe rows are structured like the following.

```
<prefix>:<fixed len row hash>:<user row>
```

The recipe also provides code to help generate split points and configure
balancing of the prefix.

## Example Use

```java
import io.fluo.api.data.Bytes;
import io.fluo.recipes.common.Pirtos;
import io.fluo.recipes.data.RowHasher;

public class RowHasherExample {

  private static final RowHasher PAGE_ROW_HASHER = new RowHasher("p");

  //Provide one place to obtain row hasher.  
  public static RowHasher getPageRowHasher(){
    return PAGE_ROW_HASHER;
  }

  public static void main(String[] args) {
    RowHasher pageRowHasher = getPageRowHasher();

    String revUrl = "org.wikipedia/accumulo";

    // Add a hash prefix to the row. Use this hashedRow in your transaction
    Bytes hashedRow = pageRowHasher.addHash(revUrl);
    System.out.println("hashedRow      : " + hashedRow);

    // Remove the prefix. This can be used by transactions dealing with the hashed row.
    Bytes orig = pageRowHasher.removeHash(hashedRow);
    System.out.println("orig           : " + orig);


    // Generate table optimizations for the recipe. This can be called when setting up an
    // application that uses a hashed row.
    int numTablets = 20;
    Pirtos tableOptimizations = pageRowHasher.getTableOptimizations(numTablets);
    System.out.println("Balance config : " + tableOptimizations.getTabletGroupingRegex());
    System.out.println("Splits         : ");
    tableOptimizations.getSplits().forEach(System.out::println);
    System.out.println();
  }
}

```

The example program above prints the following.

```
hashedRow      : p:1yl0:org.wikipedia/accumulo
orig           : org.wikipedia/accumulo
Balance config : (\Qp:\E).*
Splits         : 
p:1sst
p:3llm
p:5eef
p:7778
p:9001
p:assu
p:clln
p:eeeg
p:g779
p:i002
p:jssv
p:lllo
p:neeh
p:p77a
p:r003
p:sssw
p:ullp
p:weei
p:y77b
p:~
```

The split points are used to create tablets in the Accumulo table used by Fluo.
Data and computation will spread very evenly across these tablets.  The
Balancing config will spread the tablets evenly across the tablet servers,
which will spread the computation evenly. See the [table optimizations][2]
documentation for information on how to apply the optimizations.
 
[1]: https://github.com/fluo-io/webindex
[2]: /docs/table-optimization.md
