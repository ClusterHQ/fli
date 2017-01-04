# FLI

[Fli](https://clusterhq.com/fli/introduction), the CLI to [FlockerHub](https://clusterhq.com/flockerhub/introduction), is like Git for data.
Just like you can use Git to copy, branch, push and pull code, you can use Fli to copy, branch, push and pull your data volumes.

Fli lets you take incremental snapshots of any database or data volume that runs on Linux, and push those snapshots to a hosted volume repository called FlockerHub where they can be accessed by any person or machine to whom you’ve granted access.  
Fli can also be used without FlockerHub to manage data volumes locally.

![Fli logo](https://clusterhq.com/assets/images/logos/fli-portrait-dark.png "Fli")

Fli uses ZFS for taking and managing snapshots.
Because the snapshots are incremental, each additional push or pull to the FlockerHub sends only part of the data, speeding up transfer times and reducing bandwidth consumption.

Additionally, just like you can use Git to organize your code into repos and branches, Fli gives you the ability to organize your data into ``volumesets`` and ``branches``.
You can use Fli to manage these snapshots locally without using FlockerHub at all, or push them to FlockerHub to share with others.

# Installation

It's easy to get started using Fli because Fli is distributed on the [DockerHub](https://hub.docker.com/) as a container image called ``clusterhq/fli`` so you just need to run ```docker pull clusterhq/fli```. You can also download the Go binary directly by running ```curl -L "https://download.clusterhq.com/releases/fli/<version>" > /usr/local/bin/fli
chmod +x /usr/local/bin/fli<```

Using Fli requires a few dependencies so make sure you have these on your system before getting started.

- A supported Linux distribution– (Ubuntu 16.04 Xenial Xerus, RHEL 7.2, CentOS 7.2, or Fedora 24.)
- [Docker Engine](https://get.docker.com) if you are using the Fli container.
- [ZFS kernel modules and ZFS utilities](https://fli-docs.clusterhq.com/en/latest/GettingStarted.html#zfs-kernel-modules-and-zfs-utilities)

[View the complete documentation for Fli](https://fli-docs.clusterhq.com/en/latest/) or read on for some more background on the project.

## Build Fli from source

Fli can also be built from source. Instructions to build from source can be found [here](https://fli-docs.clusterhq.com/en/latest/Contributing.html#how-to-build-from-source).

# Why use Fli?

With increased popularity of microservices and container-based architectures, greater and greater emphasis is being placed on automated testing to ensure that separately created and updated microservices all work together when deployed to production.
Manual QA is no longer sufficient, there are simply too many variables to test.
As a result, a new set of requirements are emerging to address data management needs across the entire DevOps lifecycle.
Even before getting to production, container data management (CDM) needs to span a number of use cases including:

* Making realistic test data available to the entire development team to help improve developer productivity
* Providing realistic production data to staging and QA environments for accurate pre-prod testing
* Integrating test data into Continuous Integration / Delivery (CI/CD) environments to accelerate application development

Solving these problems is why we created Fli.

Fli, a Git-like CLI, lets Developers and Ops capture the state of a data volume running on a Linux server (usually this means a database, but Fli also works for snapshotting files, plugins, binaries and other test fixtures), and move it to a cloud based repository similar to GitHub where it can then be shared and then pulled by any other authorized user or machine.

At the present, Fli works best for testing, but these generic capabilities to snapshot and move data will one day enable:

* Disaster Recovery (DR) and backup resiliency for production stateful services to ensure required uptime availability
* Enabling movement of workloads across a multi-cloud environment for cost, performance and security reasons

# Demo

[![Using Fli to push and pull volumes to FlockerHub](http://img.youtube.com/vi/z2MKmu4Xhn4/0.jpg)](https://www.youtube.com/watch?v=z2MKmu4Xhn4)

# Get involved

* Submit a issue or PR on GitHub.  We'd love your help.
* Join our Slack community at http://slack.clusterhq.com/
* You can also join us on the ``#clusterhq`` channel on the ``irc.freenode.net``
* Ask a question on the ``flockerhub-fli-users`` [Google group](https://groups.google.com/forum/#!forum/flockerhub-fli-users)
* Send us an email at support@clusterhq.com
* Tweet us at @ClusterHQ
