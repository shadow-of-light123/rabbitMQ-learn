# Maven 构建踩坑记录（本机特定）

## 1. 企业网络拦截 repo.maven.apache.org
`curl` 报 `schannel: failed to receive handshake, SSL/TLS connection failed`。
**解法**：`~/.m2/settings.xml` 加阿里云镜像 + `.mvn/wrapper/maven-wrapper.properties` 的 distributionUrl 也指向阿里云。

## 2. Trae IDE 锁住 .m2 导致构建失败
报错形如：
```
Could not transfer artifact ...:maven-resources-plugin-3.5.0.jar
  ...jar.123456.tmp -> ...jar
```
**根因**：不是网络问题。jar 其实下载成功且 sha1 正确，但 Trae 的 Java 语言服务器
（`redhat.java` 扩展）在索引 `.m2` 目录，锁住了文件，导致 Maven 无法把 `.tmp` 重命名。

更深一层：`_remote.repositories` 里记录的仓库 ID（`central`）与当前仓库 ID
（`aliyun-public-xxx`）不匹配，Maven 因此**每次构建都重新下载重写该文件**，
而文件被锁着 → 必然失败。

**解法**（已验证有效）：
```bash
cd ~/.m2/repository
find . -name "_remote.repositories" -delete
find . -name "*.tmp" -delete
```
删除标记文件后，Maven 把本地副本当作"已安装"直接信任，不再重写，绕开了文件锁。

## 3. 本机没有独立 Maven
必须用工程自带的 `./mvnw`（Maven Wrapper）。
