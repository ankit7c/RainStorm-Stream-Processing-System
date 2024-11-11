# CS 425 MP3 (Hybrid Distributed File System)

## Description 
Implementation of Hybrid Distributed File System for CS425 MP3.

## Installation Instructions

1) Run the run.bat file from your local machine  
    Edit hosts, ips, ports1,names, VM_USER parameters  
   & edit   "git config user.name 'user netid' && " ^  
   "git config user.email 'User email id' && " ^  
   before you run it  


2) Now ssh into all the 10 machines.   


3) Go to the repository folder on  the machine you want to select as introducer


3) open application.properties using:
```
nano application.properties
```



4) Edit the properties file to and set isIntroducer=true.


5) Add the files you want to use in the "Hybrid-Distributed-File-System/input/" folder.


5) On each machine go to repository folder and Run the code  using:

```
java -jar mp1-1.jar
```

6) Enter the command "join" to join the node.


7) Once Nodes joins the network, you can start running the create , get and various other commands.


8) sample command for creating a file on Hybrid Distributed File System
```
create input/test_file_1MB.txt test_file_1MB.txt
```
   





