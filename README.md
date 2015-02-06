Succinct
========

This repository maintains the java implementation of Succinct. The master branch is in version 0.6.0-SNAPSHOT.

- [Project Homepage](http://succinct.cs.berkeley.edu)

## Building applications with Succinct

### Dependency Information

#### Apache Maven

First add the following repository to your pom.xml:

```xml
<repositories>
    <repository>
        <id>succinct-java-mvn-repo</id>
        <url>https://raw.github.com/anuragkh/succinct-java/mvn-repo/</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
</repositories>
```

Then add the following dependency information:

```xml
<dependency>
    <groupId>edu.berkeley.cs.succinct</groupId>
    <artifactId>succinct</artifactId>
    <version>0.1.0</version>
</dependency>
```
