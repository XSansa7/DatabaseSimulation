import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

public class DataManager {
    
    // keep track of the types (read-only or W/R) for transactions
    Map<Integer,Integer> transactionTypeMap;
    // keep track of the status for transactions: 1 means all sites that it has accessed are never down; vice versa
    Map<Integer, Integer> transactionStatusMap;
    //keep track of a version of Recent_Committed_Database when a specified transaction begins
    Map<Integer,int[][]> snapshotAtBeginOfRO; 
    //only for read-only transactions, which keeps track of which sites they have accessed
    Map<Integer,ArrayList<Integer>> transactionAccessedHistory; 
    //keep track of a transaction-coming sequence
    List<Integer> Begin_Time;  
    
    int[][] Current_Uncommitted_Database;
    int[][] Recent_Committed_Database;
    
    // keep track of which variable is available for access, 0 means the variable is unavailable:
    // 1 means the variable is available for read and write, 2 means the variable is available for
    // read and write for non-replicated variables, and only available for write for replicated variables
    int[][] WhetherVariableIsUpOnSite; 
                                       
     //keep track of lock information per site per variable
    List<LockInfo>[][] lockTables;  


    /** 
     * Input: none
     * Output:none
     * SideEffect: Create the constructor for DataManager,initialize
     * the values for class variables
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    DataManager(){
        lockTables = (ArrayList<LockInfo>[][]) new ArrayList[10][20];
        for (int i = 0; i < 10; i++) {
            lockTables[i] = (ArrayList<LockInfo>[]) new ArrayList[20];
            for (int j = 0; j < 20; j++) {
                lockTables[i][j] = new ArrayList<LockInfo>();
            }
        }
        
        Current_Uncommitted_Database=new int[10][20];
        Recent_Committed_Database=new int[10][20];
        WhetherVariableIsUpOnSite=new int[10][20];
        Begin_Time=new ArrayList<>();
        
        
        snapshotAtBeginOfRO=new HashMap<Integer,int[][]>();
        transactionTypeMap = new HashMap<Integer, Integer>();
        transactionStatusMap = new HashMap<Integer, Integer>();
        transactionAccessedHistory=new HashMap<Integer,ArrayList<Integer>>();
        
        for (int i=0;i<10;i++){
            
            for (int j=0;j<20;j++){
                
                Current_Uncommitted_Database[i][j]=10*(j+1);//"each variable xi is initialized to the value 10i"
                Recent_Committed_Database[i][j]=10*(j+1);
                WhetherVariableIsUpOnSite[i][j]=1; //initially, all variables at all sites are available
            }
            
        }
        
    }
    
    /** 
     * Input: an integer represent transaction number
     * Output:an integer represent commit result; 1 means successfully commit; 2 means pending; 0 means to be abort
     * SideEffect: for W/R transaction, release their locks, and update Recent_Committed_Database if it can commit
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    int Commit(int transaction){
        int type=transactionTypeMap.get(transaction);
        if (type==0){ // read-only variables, relying on snopshot
            //if WhetherVariableIsUpOnSite[][]
            ArrayList<Integer> history=transactionAccessedHistory.get(transaction);
            int whetherHasSiteFail=0;
            for (int i=0;i<history.size();i++){
                if (WhetherVariableIsUpOnSite[history.get(i)-1][0]==0){
                    whetherHasSiteFail=1;
                    break;
                }
            }
            if (whetherHasSiteFail==1){
                
                return 2;
            }
            
            
            return 1;
            
        }
        else{
            
            if (transactionStatusMap.get(transaction)!=1){
                
                return 0;
            }
            
            else{
                
                for (int i=0;i<10;i++){
                    for (int j=0;j<20;j++){
                        
                        if (lockTables[i][j].size()==1 && lockTables[i][j].get(0).getTransactionNum()==transaction &&lockTables[i][j].get(0).getLockType()==2)
                        {//means this transaction has written variable j+1 on site i+1
                            for (int k=0;k<10;k++){
                                if (WhetherVariableIsUpOnSite[k][j]>0){
                                    Recent_Committed_Database[k][j]=Current_Uncommitted_Database[k][j];
                                    WhetherVariableIsUpOnSite[k][j]=1;
                                }
                            }
                            
                            lockTables[i][j].clear();
                            
                        }
                        else{
                            
                            for (int k=0;k<lockTables[i][j].size();k++){
                                if (lockTables[i][j].get(k).getTransactionNum()==transaction){
                                    lockTables[i][j].remove(k);
                                    break;
                                }
                            }
                            
                        }
                    }
                }
                return 1;
            }
        }
        
    }

    /** 
     * Input: an integer represent transaction number
     * Output:None
     * SideEffect: for W/R transaction, release their locks and undo effects by this transaction on Concurrent_Uncommitted_Database
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    void Abort(int transaction){        
        int type=transactionTypeMap.get(transaction);
        if (type==0){ // read-only variables, relying on snopshot
            
        }
        else{
            
            for (int i=0;i<10;i++){
                for (int j=0;j<20;j++){
                    
                    if (lockTables[i][j].size()==1 && lockTables[i][j].get(0).getTransactionNum()==transaction &&lockTables[i][j].get(0).getLockType()==2)
                    {//means this transaction has written variable j+1 on site i+1
                        for (int k=0;k<10;k++){
                            
                            //undo all writes of this transaction
                            Current_Uncommitted_Database[k][j]=Recent_Committed_Database[k][j];
                            
                        }
                        
                        lockTables[i][j].clear();
                        
                    }
                    else{
                        for (int k=0;k<lockTables[i][j].size();k++){
                            if (lockTables[i][j].get(k).getTransactionNum()==transaction){
                                lockTables[i][j].remove(k);
                                break;
                            }
                        }
                        
                    }
                }
            }            
        }        
    }
    
    
    /** 
     * Input: an integer represents site number
     * Output:None
     * SideEffect: for W/R transaction, release their locks, update the transactionStatusMap and WhetherVariableIsUpOnSite
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    void fail(int site){
        for (int j=0;j<20;j++){
            
            WhetherVariableIsUpOnSite[site-1][j]=0;//this values means "cannot read or write"
            
            for (int k=0;k<lockTables[site-1][j].size();k++){
                int transaction=lockTables[site-1][j].get(k).getTransactionNum();
                
                transactionStatusMap.put(transaction,0);
            }
            
        }
        
    }
    
    /**
     * Input: an integer represents site number
     * Output:None
     * SideEffect: update WhetherVariableIsUpOnSite
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    void recover(int site){
        for (int j=0;j<20;j++){
            if ((j+1)%2==0){//even variables are replicated variables
                
                WhetherVariableIsUpOnSite[site-1][j]=2;//this value means available for write, but not available for read
                
            }
            else{//odd variables are non-replicated variables and thus can be recovered immediately
                WhetherVariableIsUpOnSite[site-1][j]=1;
            }
        }
        
    }
    
    /**
     * Input: an integer represents transaction number, and and integer represents its type (type=1 means W/R; type=0 means read-only)
     * Output: None
     * SideEffect: register this transaction in DataManager and record its type, coming order; if the transaction is read-only,
     * save a copy of Recent_Committed_Database for it
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    void Start(int transaction,int type){        
        Begin_Time.add(transaction);
        
        
        transactionTypeMap.put(transaction, type);
        transactionStatusMap.put(transaction, 1);
        if (type==0){
            transactionAccessedHistory.put(transaction,new ArrayList<>());
            
        }
        
        int [][]temp=new int[20][2];//20 variables, 2: 1 for value and 1 for site status
        
        for (int i=0;i<19;i=i+2){//these indexes are for odd-variables
            
            temp[i][0]=Recent_Committed_Database[(i+1)%10][i];
            temp[i][1]=WhetherVariableIsUpOnSite[(i+1)%10][i];
            
        }
        for (int i=1;i<20;i=i+2){//these indexes are for even-variables
            for (int j=0;j<10;j++){
                if (WhetherVariableIsUpOnSite[j][i]==1){
                    temp[i][0]=Recent_Committed_Database[j][i];
                    break;//as soon as finding one on site, stop loop
                }
            }
            temp[i][1]=1;//for replicated variables, there is always at least one site on
        }
        
        snapshotAtBeginOfRO.put(transaction,temp);
        
    }


    /** 
     * Input: an integer represents variable number; an integer represents value number; an integer represents transaction number
     * Output: true if write successfully and vice versa
     * SideEffect: obtain write locks of the variable, writes value to the variable
     * Author: Xiansha Jin
     * Date: Dec 5, 2016
     */
    boolean Write(int variable, int value, int transaction) {
        if (variable%2 != 0) {//means odd
            int targetSite = 1 + variable%10;
            int maxLockType = 0;
            List<Integer> transOwnReadLock = new ArrayList<Integer>();
            List<Integer> transOwnWriteLock = new ArrayList<Integer>();
            int indexOfLockInfo = -1;
            for (int i = 0; i < lockTables[targetSite - 1][variable - 1].size(); i++) {
                LockInfo lock = lockTables[targetSite - 1][variable - 1].get(i);//get a lock from a lock table, specified site and variable
                int curr = lock.getLockType();
                int trans = lock.getTransactionNum();
                if (trans == transaction) {//gets its own lock
                    indexOfLockInfo = i;
                }
                if (curr == 1) {
                    transOwnReadLock.add(trans);
                }
                if (curr == 2) {
                    transOwnWriteLock.add(trans);
                }
                if (curr > maxLockType) { //gets the max lock value for this variable
                    maxLockType = curr;
                }
            }
            
            if (WhetherVariableIsUpOnSite[targetSite - 1][variable - 1] != 0) { // 0: can neither read/write, 1: can read/write, 2: can write, cannot read
                if (maxLockType == 2 && transOwnWriteLock.size() == 1 && transOwnWriteLock.get(0).equals(transaction)) {
                    //meets its own write lock, so nothing changed
                    Current_Uncommitted_Database[targetSite - 1][variable - 1] = value;
                    
                    return true;
                }
                if (maxLockType == 1 && transOwnReadLock.size() == 1 && transOwnReadLock.get(0).equals(transaction)) {
                    //meets its own read lock, so update
                    lockTables[targetSite - 1][variable - 1].get(indexOfLockInfo).updateLockType(2);
                    Current_Uncommitted_Database[targetSite - 1][variable - 1] = value;
                    
                    return true;
                }
                if (maxLockType == 0) {//means no locks existing for this variable
                    
                    lockTables[targetSite - 1][variable - 1].add(new LockInfo(transaction, 2));//add write lock for this transaction
                    Current_Uncommitted_Database[targetSite - 1][variable - 1] = value;
                    
                    return true;
                }
                
                return false;
            }
            
            return false;
        }
        
        int acquiredLockCounter = 0;
        List<Integer> upSiteIndexes = new ArrayList<Integer>();
        for (int i = 0; i < 10; i++) {
            int maxLockType = 0;
            List<Integer> transOwnReadLock = new ArrayList<Integer>();
            List<Integer> transOwnWriteLock = new ArrayList<Integer>();
            int indexOfLockInfo = -1;
            for (int j = 0; j < lockTables[i][variable - 1].size(); j++) {
                LockInfo lock = lockTables[i][variable - 1].get(j);
                int curr = lock.getLockType();
                int trans = lock.getTransactionNum();
                if (trans == transaction) {
                    indexOfLockInfo = j;
                }
                if (curr == 1) {
                    transOwnReadLock.add(trans);
                }
                if (curr == 2) {
                    transOwnWriteLock.add(trans);
                }
                if (curr > maxLockType) {
                    maxLockType = curr;
                }
            }
            
            if (WhetherVariableIsUpOnSite[i][variable - 1] != 0) {
                
                upSiteIndexes.add(i);
                if (maxLockType == 2 && transOwnWriteLock.size() == 1 && transOwnWriteLock.get(0).equals(transaction)) {
                    acquiredLockCounter++;
                }
                if (maxLockType == 1 && transOwnReadLock.size() == 1 && transOwnReadLock.get(0).equals(transaction)) {
                    
                    lockTables[i][variable - 1].get(indexOfLockInfo).updateLockType(2);
                    acquiredLockCounter++;
                }
                if (maxLockType == 0) {
                    
                    lockTables[i][variable - 1].add(new LockInfo(transaction, 2));
                    acquiredLockCounter++;
                }
                
            }
        }
        if (upSiteIndexes.size() != 0 && upSiteIndexes.size() == acquiredLockCounter) {
            
            
            for (int i : upSiteIndexes) {
                Current_Uncommitted_Database[i][variable - 1] = value;
            }
            
            return true;
        }
        
        return false;
    }
    
    /**
     * Input: an integer represents site number; an integer represents variable number; an integer represents transaction number
     * Output: return the value of the variable
     * SideEffect: obtain read lock for the variable
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    int Read(int site,int variable,int transaction){

        
        int type=transactionTypeMap.get(transaction);
        
        if (type==0){
            
            int [][] temp=snapshotAtBeginOfRO.get(transaction);
            
            ArrayList<Integer> a=transactionAccessedHistory.get(transaction);
            
            if (variable%2==1){//odd variable, unique site
                int truesite=1 + variable%10;
                if (a.contains(truesite)==false){
                    a.add(truesite);
                    transactionAccessedHistory.put(transaction, a);
                }
                System.out.println("Read from site "+truesite);
                return temp[variable-1][0];
            }
            
            else{
                if (WhetherVariableIsUpOnSite[site-1][variable-1]!=1){
                    return 1000000;
                }
                else{
                    int truesite=site;
                    if (a.contains(truesite)==false){
                        a.add(truesite);
                        transactionAccessedHistory.put(transaction, a);
                    }
                    System.out.println("Read from site "+truesite);
                    return temp[variable-1][0];
                }
                
            }
            
        }
        else{
            
            //first check whether site is on
            if (variable%2==0){//replicated variables
                
                if (WhetherVariableIsUpOnSite[site-1][variable-1]!=1){
                    
                    
                    return 1000000;
                }
            }
            //then check locks
            int maxLock=0;
            for (int i=0;i<lockTables[site-1][variable-1].size();i++){
                if (lockTables[site-1][variable-1].get(i).getLockType()>maxLock){
                    maxLock=lockTables[site-1][variable-1].get(i).getLockType();
                }
            }
            if (maxLock<2){
                
                lockTables[site-1][variable-1].add(new LockInfo(transaction,1));
                
                System.out.println("Read from site "+site);
                return Current_Uncommitted_Database[site-1][variable-1];
            }
            else{
                
                // return 1000000 represents an unsuccessful read
                return 1000000;} 
            
        }
        
    }
    
    /**
     * Input: none
     * Output: none
     * SideEffect: print the committed values of all copies 
     *  of all variables at all sites
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    void dumpAll(){
        for (int i=0;i<10;i++){
            int site=i+1;
            String x="site "+site+":";
            for (int j=0;j<20;j++){
                int variable=j+1;
                if ((variable)%2==1){//odd variable case
                    if ((1 + variable%10)==site){//find the unique site for this variable
                        int value=Recent_Committed_Database[i][j];
                        x+="x"+variable+"="+value+" ";
                    }
                }
                else{//even variable case
                    int value=Recent_Committed_Database[i][j];
                    x+="x"+variable+"="+value+" ";
                }
            }
            System.out.println(x);
        }
    }

    /** 
     * Input: an integer represents site number
     * Output:none
     * SideEffect: print the committed values of all copies 
     * of all variables at site i
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    void dumpSite(int site){
        String x="site "+site+":";
        for (int j=0;j<20;j++){
            int variable=j+1;
            if ((variable)%2==1){//odd variable case
                if ((1 + variable%10)==site){//find the unique site for this variable
                    int value=Recent_Committed_Database[site-1][j];
                    x+="x"+variable+"="+value+" ";
                }
            }
            else{//even variable case
                int value=Recent_Committed_Database[site-1][j];
                x+="x"+variable+"="+value+" ";
            }
        }
        System.out.println(x);
    }
    
    /** 
     * Input: an integer represents variable number
     * Output:none
     * SideEffect: print the committed values of all copies 
     * of the input variable at at all sites
     * Author: Lingbo Li
     * Date: Dec 5, 2016
     */
    void dumpVariable(int variable){
 
        for (int i=0;i<10;i++){
            int site=i+1;
            String x="site "+site+":";
            if ((variable)%2==1){//odd variable case
                if ((1 + variable%10)==site){//find the unique site for this variable
                    int value=Recent_Committed_Database[i][variable-1];
                    x+="x"+variable+"="+value+" ";
                }
            }
            else{//even variable case
                int value=Recent_Committed_Database[i][variable-1];
                x+="x"+variable+"="+value+" ";
            }
            System.out.println(x);
        }
        
    }
    
    /** 
     * Input: a set contains transactions
     * Output: an integer represents the youngest transaction number
     * SideEffect: None
     * Author: Xiansha Jin
     * Date: Dec 5, 2016
     */
    int youngestTransaction(Set<Integer> transNumSet){
        
        int seqNum = -1;
        int candidate = -1;
        for (Integer transNum : transNumSet) {
            if (Begin_Time.indexOf(transNum) > seqNum) {
                seqNum = Begin_Time.indexOf(transNum);
                candidate = transNum;					
            }			
        }
        return candidate;
    }
}