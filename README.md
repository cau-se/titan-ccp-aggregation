# Titan CCP - Aggregation

The [Titan Control Center Prototype](https://ieeexplore.ieee.org/abstract/document/8822045)
(CCP) is a scalable monitoring infrastructure for [Industrial DevOps](https://industrial-devops.org/).
It allows to monitor, analyze and visualize the electrical power consumption of
devices and machines in production environments such as factories.

This repository contains the **Aggregation** microservice of the Titan CCP.

## Build and Run

We use Gradle as a build tool. In order to build the executeables run 
`./gradlew build` on Linux/macOS or `./gradlew.bat build` on Windows. This will
create the file `build/distributions/titanccp-aggregation.tar` which contains
start scripts for Linux/macOS and Windows.

This repository also contains a Dockerfile. Run
`docker build -t titan-ccp-aggregation .` to create a container from it (after
building it with Gradle).