# MoleclarTrajectoryAnalysis
Mtech_Sem1_Project_ProgrammingPractices

A. Prerequisites
1. You must have Scala v2.11.12 Spark v2.4.4
2. You must have sbt installed

B. Installation steps
1. Change your directory to Project Directory “MTA”
2. Compile Project using command “sbt compile”
3. Build JAR of Project using command “sbt package”
4. You can find the JAR file in “target” directory inside the project directory
5. Use this JAR to run modules

C. How to run Modules
1. Reading
Please use a different JAR for this module
path of JAR : /ppr.gp2/C3/read.jar
Run the following command:
spark-submit –class api.pdb_df –master yarn –deploy-mode cluster –num-executors
<num> –executor-cores <num> –driver-memory <num> –executor-memory 20g –
conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path> <HDFS
CRD files directory path> <OUTPUT dir>

2 Auto Image
Run the following command:
spark-submit –class api.autoImageMaster –master yarn –deploy-mode cluster –numexecutors <num> –executor-cores <num> –driver-memory <num> –executor-memory
20g –conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path>
<HDFS CRD files directory path> <OUTPUT dir>

3 PDB Generation
It gets generated in Auto Image Module itself
  
4 Masking
Run the following command:
spark-submit –class api.maskMasterf –master yarn –deploy-mode cluster –num-executors
<num> –executor-cores <num> –driver-memory <num> –executor-memory 20g –
conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path> <HDFS
CRD files directory path> <OUTPUT dir>

5 Average Structure
Run the following command:
spark-submit –class api.avgMaster –master yarn –deploy-mode cluster –num-executors
<num> –executor-cores <num> –driver-memory <num> –executor-memory 20g –
conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path> <HDFS
CRD files directory path> <OUTPUT dir>

6 Distance
Run the following command:
spark-submit –class api.distanceMaster –master yarn –deploy-mode cluster –numexecutors <num> –executor-cores <num> –driver-memory <num> –executor-memory
20g –conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path>
<HDFS CRD files directory path> <OUTPUT dir>
  
 7  Angle
Run the following command:
spark-submit –class api.angleMaster –master yarn –deploy-mode cluster –num-executors
<num> –executor-cores <num> –driver-memory <num> –executor-memory 20g –
conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path> <HDFS
CRD files directory path> <OUTPUT dir> <atom1 index> <atom2 index> <atom3
index>

8 RMSD
Run the following command:
spark-submit –class api.RmsdMasterAtom –master yarn –deploy-mode cluster –
num-executors <num> –executor-cores <num> –driver-memory <num> –executormemory 20g –conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO
path> <HDFS CRD files directory path> <OUTPUT dir> 
  
9 Residue-Wise RMSD
Run the following command:
spark-submit –class api.RmsdMasterRes –master yarn –deploy-mode cluster –numexecutors <num> –executor-cores <num> –driver-memory <num> –executor-memory
20g –conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path>
<HDFS CRD files directory path> <OUTPUT dir>

10 Dihedral
Run the following command:
spark-submit –class api.dihedralMaster –master yarn –deploy-mode cluster –numexecutors <num> –executor-cores <num> –driver-memory <num> –executor-memory
20g –conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path>
<HDFS CRD files directory path> <OUTPUT dir>

11 Hydrogen Bond
Run the following command:
spark-submit –class api.HBondMaster –master yarn –deploy-mode cluster –numexecutors <num> –executor-cores <num> –driver-memory <num> –executor-memory
20g –conf spark.driver.maxResultSize=20g <JAR path> <HDFS TOPO path>
<HDFS CRD files directory path> <OUTPUT dir>
  
  
