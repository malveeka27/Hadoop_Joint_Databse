tuj'Problem Statement
Write a mapper and reducer program to join two databases?
Consider Input data
joinA.txt
able,991
about,11
burger,15
actor,22
about,13
joinB.txt
Jan-01 able,5
Feb-02 about,3
Mar-03 about,8
Apr-04 able,13
Feb-22 actor,3
Feb-23 burger,5
Mar-08 burger,2
Dec-15 able,100

Finally, the output should look like : join_output.txt
able,Dec-15,100,991
able,Apr-04,13,991
able,Jan-01,5,991
about,Mar-03,8,13
about,Feb-02,3,13
about,Mar-03,8,11
about,Feb-02,3,11
actor,Feb-22,3,22
burger,Mar-08,2,15
burger,Feb-23,5,15
What we did
Write the code for Mapper function, Reducer fuction and Driver function.
Combine it in one file 

*Start hdfs*
vboxuser@vboxuser-VirtualBox:~$ hdfs namenode -format
vboxuser@vboxuser-VirtualBox:$ cd/hadoop-3.2.1/sbin
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ ./start-dfs.sh
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ /start-yarn.sh
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ jps

vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ export HADOOP_CLASSPATH=$(hadoop classpath)
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ echo $HADOOP_CLASSPATH

*Created directory to store the input files*
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$  hdfs dfs -mkdir /test
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ hdfs dfs -mkdir /test/myinput

*Added the input file from the local computer to the directory*
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$  hdfs dfs -put /home/vboxuser/Desktop/new/code/joinA.txt /test/myinput
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$  hdfs dfs -put /home/vboxuser/Desktop/new/code/joinB.txt /test/myinput

*Compiled the java file*
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ javac -classpath ${HADOOP_CLASSPATH} -d '/home/vboxuser/Desktop/new/code' 'home/vboxuser/Desktop/new/Join.java

*Created the jar file*
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ jar -cvf yo.jar -C '/home/vboxuser/Desktop/new/code'/ .

*Run the jar file after putting in the input to get the output
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$  hadoop jar yo.jar Join /test/myinput/joinA.txt test/myinput/joinB.txt /test/myoutput

*Checked the output produce*
vboxuser@vboxuser-VirtualBox:~/hadoop-3.2.1/sbin$ hdfs dfs -cat /test/myoutput/part-r-00000


