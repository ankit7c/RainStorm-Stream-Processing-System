# CS 425 MP4 - Rainstorm

## Team: GA6
Saurabh Darekar (sdare1) & Ankit Chavan (auc3)

---

## **Overview**
Rainstorm is a distributed stream processing system designed to efficiently process large datasets. It offers dynamic task allocation, fault tolerance, and logging mechanisms, making it an effective alternative to Apache Spark. This project was implemented as part of CS 425 MP4.

## **System Design**

### **1. Streaming Process**
- **Initialization:** A designated leader reads the Rainstorm command and assigns tasks to available members based on the membership list.
- **Task Allocation:** The leader assigns specific roles, such as reading, filtering, and aggregation.
- **Processing and Output:** Once assigned, members execute their tasks, and the final output is continuously displayed on the leader’s screen.

### **2. Logging and Batching**
- Each worker task sends logs to HyDFS.
- Aggregators store current states and processed data in HyDFS.
- Data and logs are appended in batches to minimize load.

### **3. Executing Custom Code**
- Uses dynamic Java classes for executing user-defined operations.
- Processes key-value pairs using custom implementations.

### **4. Fault Tolerance**
- Relies on the MP2 Failure Detector to identify failures.
- Logs, data, and worker states are maintained in MP3 HyDFS.
- Upon failure detection, tasks are reassigned, and data recovery is triggered.
- Ensures "exactly-once" processing by handling duplicate records efficiently.

---

## **Performance Evaluation**

### **Datasets Used**
- **Traffic Signs Dataset** (City of Champaign)
- **Street Lights Dataset** (City of Champaign)

### **Results**
#### **1. Traffic Signs Dataset**
- **Scenario 1 (Simple Operator):**
    - Extracted "OBJECTID" and "Sign_Type" based on a specific pattern.
    - Execution time: Spark (~11–12 sec) vs. Rainstorm (~8–9 sec).
- **Scenario 2 (Complex Operator):**
    - Filtered rows where "Sign Post" matches a pattern and counted values in the "category" column.
    - Execution time: Spark (~9–11 sec) vs. Rainstorm (~6–8 sec).

#### **2. Street Lights Dataset**
- **Scenario 1 (Simple Operator):**
    - Extracted "OBJECTID" and "Pole_Material" based on a specific pattern.
    - Execution time: Spark (~14–16 sec) vs. Rainstorm (~11–13 sec).
- **Scenario 2 (Complex Operator):**
    - Filtered rows where "Luminaire_Type" matches a pattern and counted values in "Pole_Material" column.
    - Execution time: Spark (~12–14 sec) vs. Rainstorm (~9–11 sec).

**Observations:** Rainstorm consistently outperformed Apache Spark, likely due to its lower initialization overhead and optimized execution model.

---

## **Installation Instructions**

1. Run `run.bat` from your local machine after configuring:
    - `hosts, ips, ports1, names, VM_USER` parameters.
    - Update Git user details before execution.

2. SSH into all 10 machines.
3. Navigate to the repository on the machine designated as the introducer.
4. Open `application.properties`:
   ```sh
   nano application.properties
   ```
5. Set `isIntroducer=true` and update execution details:
   ```sh
   totExecutables=<Total number>
   exeDir=<Directory containing Java classes>
   class1=<Class Name>
   methodName1=<Method Name>
   stateful1=<true/false>
   saveMethodName1=<Method for saving state>
   loadMethodName1=<Method for loading state>
   savePath1=<File path>
   saveFileName1=<File name>
   ```
6. Place input files in `Rain-Storm/input/`.
7. On each machine, navigate to the repository and execute:
   ```sh
   java -jar mp1-1.jar
   ```
8. Enter `join` to connect to the network.
9. Process a file using:
   ```sh
   rainstorm OP1_class_name OP2_class_name File_name hydfs_dest_filename num_tasks Pattern1_for_OP1 Pattern1_for_OP2
   ```

---

## **Conclusion**
Rainstorm provides an efficient, fault-tolerant, and scalable alternative to Spark for distributed stream processing. Through performance evaluation, we demonstrated its effectiveness in handling real-world datasets with lower execution time.

