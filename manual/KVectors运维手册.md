% KVectors运维手册
% 王福强
% 2025-10-23

## KVectors的安装与部署

KVectors是Scala/Java程序，最简洁的使用方式就是`java -jar kvectors.jar`，因为程序编译打包之后，已经打包成了一个可执行的fat-jar/one-jar格式。

但是，假如客户公司有docker容器化的需求， 也支持以docker image的方式安装和部署，下面是一个可供参考的Dockerfile定义：

```dockerfile
# 多阶段构建 Dockerfile
# 阶段1: 构建阶段
FROM ghcr.io/graalvm/graalvm-community:25-ol9 AS builder

# 设置工作目录
WORKDIR /kvectors

# 安装Maven
RUN microdnf install -y maven

# 复制Maven配置文件
COPY pom.xml .

# 下载依赖（利用Docker缓存层）
RUN mvn dependency:go-offline -B

# 复制源代码
COPY src ./src

# 构建应用程序，跳过测试以加快构建速度
RUN mvn clean package -DskipTests

# 阶段2: 运行阶段 - 使用GraalVM完整镜像
FROM ghcr.io/graalvm/graalvm-community:25-ol9

# 创建非root用户
RUN groupadd -r kvectors && useradd -r -g kvectors kvectors

# 设置工作目录
WORKDIR /kvectors

# 从构建阶段复制JAR文件
COPY --from=builder /kvectors/target/*.jar kvectors.jar

# 更改文件所有者
RUN chown -R kvectors:kvectors /kvectors

# 切换到非root用户
USER kvectors

# 暴露端口
EXPOSE 1980

# 设置JVM参数
ENV JAVA_OPTS="--enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector"

# 设置JAR参数（传递给应用程序的参数）
ENV JAR_OPTS=""

# 启动应用程序
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar $JAR_OPTS kvectors.jar $@", "--"]
```


这里主要有三个需要重点关注的点：

1. 最好使用graalvm，因为它可以给scala程序带来30%的性能提升；（当然，即使使用OpenJDK，对于程序正常运行也没有任何影响，但推荐使用graalvm）
2. 以上Dockerfile定义采用多阶段构建的定义，构建最简洁的docker image
3. 开放了JAVA_OPTS和JAR_OPTS，允许用户自定义java虚拟机参数以及程序运行参数。


建议部署环境：

1. Java：推荐使用 Java25 LTS版本。
2. JDK： GraalVM
3. OS（操作系统）： MacOS 或者 Linux (原则上来说，Windows 也可以，但没有进行测试)
4. Memory(内存)： 无严格要求，当然最好不小于2G，一般根据系统负载进行增减，内存越多，KVectors可以发挥空间越大，尤其是可以获得更低时延(Latency)。
5. CPU（中央处理器）：建议支持SIMD指令的CPU型号， 不支持也没关系，但没有SIMD的性能加成。
6. Disk（磁盘）：建议SSD（HDD也支持），存储容量根据应用情况选择就可以了， 比如100万1024维向量，存储空间需至少4G，供参考以此类推。
7. GPU（显卡/图形处理器）：无强制需求，KVectors不依赖GPU资源。


## KVectors的配置与参数

KVectors的配置和参数相对较少，用户大部分情况下使用默认值就可以了，对于java虚拟机运行期的参数，可以根据通用文档进行配置。 

对于KVectors应用方面的参数配置，默认配置如下：

```properties
web.admin.enabled=false

# api endpoints config
api.server.host=localhost
api.server.port=1980
api.server.access.token=...
```

唯一需要配置的参数，原则上只有api.server.access.token， 我们可以在程序启动的时候，通过`-Dapi.server.access.token=xxx`的方式进行配置：

- 如果选择直接的命令行启动，需要在`-jar`与`jar`参数之间指定，比如： `java -jar -Dapi.server.access.token=xxx kvectors.jar`
- 如果选择Docker启动，直接以JAR_OPTS的环境变量形式提供即可， 比如： `docker run ... -e JAR_OPTS="-Dapi.server.access.token=xxx" ...`

生产环境部署，除了要明确指定`api.server.access.token`参数（不建议使用默认值，容易造成安全漏洞）， 
还有一个参数是必须（或者强烈建议）的，那就是`-Dprofile=production`。 

`-Dprofile=production`参数的目的与`api.server.access.token`类似，都是基于安全考虑， 因为有些开发和测试期间为了便利而开放的访问token，如果在生产环境不屏蔽它们的话，也同样容易造成安全漏洞。

这是KVectors在应用一级的配置重点。


再深入一级，就是对KVectors中的各个Vector Collection进行配置，这个需要根据具体的Vector Collection实现类型决定，**比如** AnnIndexKVectorCollection， 它的构造参数是这样的：

```scala
class AnnIndexKVectorCollection(val name: String, val repositoryDir: File, maxNumOfIndexArchives: Int = 11)
```

只是在构造的时候指定一下Vector Collection的name就可以了， repositoryDir会自动配置（虽然这里没有给默认值）。

至于maxNumOfIndexArchives， 更多是指定要保留多少索引的存档。 因为AnnIndexKVectorCollection支持定期全量构建索引，所以，如果不定期清理，会占用不必要的磁盘空间，所以，通过这个参数，程序会自动清理不再需要的索引存档。

大部分情况下， AnnIndexKVectorCollection可以满足90%的客户场景需求。


## KVectors的监控与集成

监控无非三个关键途径和指标：

1. logs
2. metrics
3. traces

KVectors采用slf4j和logback打印日志，会定期rollover，客户公司可以根据需要，通过自己的log agent本地刮取日志上传到自己的日志分析平台。

KVectors集成了dropwizard metrics和open telemetry两种metrics注册与统计方式，客户公司可以根据需要选择使用。

使用open telemetry，只要在kvectors.jar启动运行的时候，指定open telemetry自带的javaagent就可以了， 比如：

```
java -javaagent:./opentelemetry-javaagent.jar \
   -Dotel.service.name=my-hybrid-app \
   -Dotel.metrics.exporter=prometheus \ 
   ...
```

至于traces， open telemetry同样有支持。

假如客户公司有需要，后续版本可以再添加更多监控基础设施的支持。

## KVectors的错误与诊断

这也更多依赖前面提到的logs/metrics/traces三件套的支持。

大部分情况下，KVectors经过测试发布之后，不会有太大的错误和bugs存在。

客户公司如果自身实在搞不定，我们也提供在线或者线下支持。


## KVectors备份与恢复

KVectors的备份和恢复目前采用最简单的文件系统备份和恢复策略。

所有的KVectors的数据状态默认都存放在`${HOME}/.kvectors`下面

在`${HOME}/.kvectors`下面，又按照Vector Collection进行排布，每个Vector Collection有自己的数据目录：

- 要全量备份，就直接备份`${HOME}/.kvectors`整个目录（比如用rsync或者拷贝到冷热备份设备）
- 要局部备份，就直接备份指定的Vector Collection的数据目录就可以了。

KVectors面向单节点的设计初衷决定了，它可以最简单的方式完成数据的备份与恢复。

如果愿意，也可以备份到云OSS对象服务。

> TIP
> 
> 可以通过配置文件或者启动参数的形式更改kvectors默认的数据目录（dataDir）地址， 比如：
> 
> `java -jar -Ddata.dir="{{SOME_WHERE_YOU_LIKE}}" kvectors.jar`
> 
> 不过，如果更改了默认的数据目录位置，备份和恢复的时候也需要同步使用这个数据目录。





