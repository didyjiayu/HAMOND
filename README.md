# Hamond

[Hamond](https://gitlab.com/yujia1986/Hamond "Hamond") is an implementation to speedup [DIAMOND](http://ab.inf.uni-tuebingen.de/software/diamond/ "DIAMOND") in parallel by using [Apache Hadoop](https://hadoop.apache.org/ "Hadoop"). It has some advantages.

  - It has high failure tolerance, availability and scalability
  - It's faster than single PC DIAMOND in dealing with large size genomes
  - It accepts all DIAMOND alignment options
  - It can run either on an in-house Hadoop cluster or on [Amazon Web Service](https://aws.amazon.com/ "AWS")

DIAMOND is an alignment tool:

> DIAMOND is a BLAST-compatible local aligner for mapping protein and translated DNA query sequences against a protein reference database (BLASTP and BLASTX alignment mode). The speedup over BLAST is up to 20,000 on short reads at a typical sensitivity of 90-99% relative to BLAST depending on the data and settings.

Hamond is the abbreviation of Hadoop and DIAMOND. :-P

### Version
1.0

### Download release
[Hamond-1.0.jar](https://gitlab.com/yujia/Hamond/uploads/c25e16295093a9b515be82c54190c25d/Hamond-1.0-SNAPSHOT.jar "release")

### System requirements

  - In-house cluster
    - Linux system
    - Hadoop-2.4.0 or higher
    - Java 1.7 environment or higher
    
  - Amazon Web Service
    - [Elastic Map Reduce](https://aws.amazon.com/elasticmapreduce/ "EMR") service
    - An [S3](https://aws.amazon.com/s3/ "S3") bucket

### Execution

  - Execute on an in-house cluster
    
    User first has to download the released Hamond jar package and DIAMOND binary file to the master node of the cluster. Hamond can only accecpt one query file at once, so if user has multiple query files, concatenate them into one file like this:

    ```sh
    $ cat ~/*.faa >> ~/query.faa
    ```
    
    or
    
    ```sh
    $ cat ~/1.faa ~/2.faa ~/3.faa >> ~/query.faa
    ```
    
    The query and reference genome files should also be in the master node. To run Hamond, execute like this:
    
    ```sh
    $ hadoop jar ~/Hamond-x.x.jar diamondmapreduce.DiamondMapReduce ~/diamond ~/query.faa ~/reference.faa ~/Hamond.output blastp --sensitive -e 0.00001 -k 1000
    ```
    
    The first argument after the Hamond jar package is the main class name. User then has to give the paths of DIAMOND binary file, query and reference genome files and the output file in order. User has to specify the alignment type (blastp or blastx) as the fifth argument. After that, Hamond can accept all the [DIAMOND alignment options](https://github.com/bbuchfink/diamond#scoring--reporting-options "options").
    
  - Execute on Amazon Web Service
  
    Amazon provide Elastic Map Reduce service for user to create a Hadoop cluster with a few clicks and little expense. Hamond is fully compatible with it.

    User can register on [*EMR*](https://aws.amazon.com/elasticmapreduce/). User first has to go to S3 service to create a storage bucket and upload the Hamond jar package, DIAMOND binary file, query and reference files into it. The in EMR service, user can create a Hadoop cluster (make sure the Hadoop version is higher than 2.4.0) with steps. In steps, user should choose *Custom JAR* and locate the Hamond jar package in S3 bucket. Arguments should be like this:
    
    ```code
    diamondmapreduce.DiamondMapReduce s3://$yourBucket/diamond s3://yourBucket/query.faa s3://$yourBucket/reference.faa s3://$yourBucket/Hamond.output blastp --sensitive -e 0.00001 -k 1000
    ```
    
    It is similar to execution on in-house cluster, after the alignment type user can give any DIAMOND alignment options.
    
    User can find the output file in the S3 bucket. It is possible to add more than one Hamond step during creating the cluster. Make sure to click on *Auto-terminate cluster after the last step is completed*. This option can save the elapsed time hence the expense of the usage.

### License

Hamond is an open source application with Apache License Version 2.0.