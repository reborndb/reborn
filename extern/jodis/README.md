#Jodis - Java client for reborn
Jodis is a java client for reborn based on [Jedis](https://github.com/xetorthio/jedis) and [Curator](http://curator.apache.org/).

##Features
- Use a round robin policy to balance load to multiple reborn proxies.
- Detect proxy online and offline automatically.

##How to use
Add this to your pom.xml. We deploy jodis to https://oss.sonatype.org.
```xml
<dependency>
  <groupId>com.reborndb.jodis</groupId>
  <artifactId>jodis</artifactId>
  <version>0.1.2</version>
</dependency>
```
To use it
```java
JedisResourcePool jedisPool = new RoundRobinJedisPool("zkserver:2181", 30000, "/zk/reborn/db_xxx/proxy", new JedisPoolConfig());
try (Jedis jedis = jedisPool.getResource()) {
    jedis.set("foo", "bar");
    String value = jedis.get("foo");
}
```
Note: JDK7 is required to build and use jodis. If you want to use jodis with JDK6, you can copy the source files to your project, replace ThreadLocalRandom in BoundedExponentialBackoffRetryUntilElapsed and JDK7 specified grammar(maybe, not sure) , and then compile with JDK6.
