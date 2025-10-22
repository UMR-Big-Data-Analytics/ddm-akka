# ddm-akka
Akka example and homework code for the "Distributed Data Management" lecture.

## Requirements
- Java Version >= 21
- Maven Compiler Version >= 3.9.11

## Getting started
1. Clone repo
  ```
  git clone https://github.com/UMR-Big-Data-Analytics/ddm-akka.git
  ```

2. Get the Akka libraries

The license of Akka requires the user to obtain an access token (free for development) to checkout the libraries. 
For this, create an account for [Akka](https://account.akka.io/) and get your token from [here](https://account.akka.io/token).
Then, we need to add this token to the `~/.m2/settings.xml` file (Linux/Mac), `c:\Users\<username>\.m2`. If this file does not exist, create it.
Add the following content to this file, replacing `#TOKEN` with your actual token:
  ```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                      http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <activeProfiles>
        <activeProfile>akka</activeProfile>
    </activeProfiles>
    <profiles>
        <profile>
            <id>akka</id>
            <repositories>
                <repository>
                    <id>akka-secure</id>
                    <name>Akka Secure</name>
                    <url>https://repo.akka.io/#TOKEN/secure</url>
                </repository>
            </repositories>
        </profile>
    </profiles>
</settings>
  ```

Do not add the repository to the `pom.xml` file, as this would expose your token to everyone who has access to the `pom.xml` file.
        
3. Decompress test data
  ```
  cd ddm-akka/data
  unzip TPCH.zip
  ```

4. Build project with maven
  ```
  cd ..
  mvn package
  ```

5. Read the program documentation
  ```
  java -jar target/ddm-akka.jar
  ```

6. First run
  ```
  java -jar target/ddm-akka.jar master -es true
  ```
The flag `-es true` is used only for demo-ing purposes. Once a solution is implemented, skip this flag and leave it to `false`. If the flag is set to true, the program is terminated by pressing `enter` on the console, but a working solution should end itself once it is done.

7. Distributed run (locally on one machine)
  ```
  // Run a master
  java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar master -w 0
  // Run a worker (repeat command in different terminals for multiple workers)
  java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar worker -w 1
  ```

`-Xms` and `-Xmx` are options for the Java Virtual Machine [to configure initial and maximum heap size](https://www.ibm.com/docs/en/sdk-java-technology/8?topic=options-xms). To ensure that your program runs on the Pi cluster, make it no greater than two gigabytes (`-Xmx=2048m` or `-Xmx=2g`).

8. Distributed run (on multiple machines)
  ```
  // Run a master
  java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar master -w 0 -h <your-ip-address>
  // Run a worker (repeat command on different computers for multiple workers)
  java -Xms2048m -Xmx2048m -jar target/ddm-akka.jar worker -w 1 -mh <master-host-ip> -h <your-ip-address>
  ```

**Note that you need to substitute `<your-ip-address>` and `<master-host-ip>` with the local node's and the master's IP address, respectively.** You can use websites like [whatismyipaddress.com](https://whatismyipaddress.com/) or command-line utilities like `hostname -I` and `ifconfig` to get these IP addresses.

`-Xms` and `-Xmx` are options for the Java Virtual Machine [to configure initial and maximum heap size](https://www.ibm.com/docs/en/sdk-java-technology/8?topic=options-xms). To ensure that your program runs on the Pi cluster, make it no greater than two gigabytes (`-Xmx=2048m` or `-Xmx=2g`).

## Hints

1. Run `java -jar target/ddm-akka.jar` without arguments to have a help text printed to your console. It will describe all parameters in detail.
2. Use `java -Xms2048m -Xmx2048m` to restrict your program run to two gigabyte of memory. This ensures that it runs on a Pi cluster.
3. Use `LargeMessageProxy` to send large messages, but pay attention to the extra memory this proxy pattern requires for sending messages.
4. Use `MemoryUstils` to measure byte sizes and memory usage, but keep in mind that memory measurements in Java are very expensive.
