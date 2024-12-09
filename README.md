# CS 425 MP4 (Rain Storm)

## Description 
Implementation of Rain-Storm for CS425 MP4.

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

5) Update the properties file with details for your executable. Use below fields and set totExecutables correctly.
```
totExecutables=Total no of executables
exeDir=Directory where Java classes are stored
class1=Class Name
methodName1=Method Name
stateful1= is operation stateful or stateless
saveMethodName1=Method name to save object/state
loadMethodName1=Method name to load object/state
savePath1=save file path
saveFileName1=file name of object
```


6) Add the files you want to process in the "Rain-Storm/input/" folder.


7) On each machine go to repository folder and Run the code  using:

```
java -jar mp1-1.jar
```

8) Enter the command "join" to join the ring/network.


9) Once Nodes joins the network, sample command for processing a file on Rain Storm
```
rainstorm OP1_class_name OP2_class_name File_name hydfs_dest_filename num_tasks Pattern1_for_OP1 Pattern1_for_OP2
```
   





