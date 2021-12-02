# Titan Control Center - Aggregation

The [Titan Control Center](https://doi.org/10.1016/j.simpa.2020.100050)
is a scalable monitoring infrastructure for [Industrial DevOps](https://industrial-devops.org/).
It allows to monitor, analyze and visualize the electrical power consumption of
devices and machines in industrial production environments.

This repository contains the **Aggregation** microservice of the Titan Control Center.

## Configuration

This microservice can be configured via environment variables or a Java `application.properties`
file. Most variables are self explaining.

* `EMIT_PERIOD_MS` determines the frequency aggregation results are generated with.
It has to be set at least as large as the period sensors are generating
measurements with. If a sensor is generating no measurement withing an emit
period, the corresponding aggregation result would not contain a value from the
corresponding sensor and, thus, would be lower than it should be normally.
* `GRACE_PERIOD_MS` dertermines for long late arriving measurements are considered
and, thus, for how long they would cause corrective aggregation results.


## Build and Run

We use Gradle as a build tool. In order to build the executeables run 
`./gradlew build` on Linux/macOS or `./gradlew.bat build` on Windows. This will
create the file `build/distributions/titanccp-aggregation.tar` which contains
start scripts for Linux/macOS and Windows.

This repository also contains a Dockerfile. Run
`docker build -t titan-ccp-aggregation .` to create a container from it (after
building it with Gradle).

## Reference

Please cite the Titan Control Center as follows:

> S. Henning, W. Hasselbring, *The Titan Control Center for Industrial DevOps analytics research*, Software Impacts 7 (2021), DOI: [10.1016/j.simpa.2020.100050](https://doi.org/10.1016/j.simpa.2020.100050).

BibTeX:

```bibtex
@article{Henning2021,
    title = {The Titan Control Center for Industrial DevOps analytics research},
    journal = {Software Impacts},
    volume = {7},
    pages = {100050},
    year = {2021},
    doi = {10.1016/j.simpa.2020.100050},
    author = {Sören Henning and Wilhelm Hasselbring},
}
```
