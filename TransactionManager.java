import java.util.Scanner;
import java.util.Set;

import org.jgraph.graph.DefaultEdge;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class TransactionManager {
  int time = 0;
  List<String> transactionBufferContent;
  List<Integer> transactionBufferSequence;
  Set<Edge> edgeSet;
  Set<Integer> transNumSet;

  TransactionManager() {
    transactionBufferContent = new ArrayList<String>();
    transactionBufferSequence = new ArrayList<>();
    edgeSet = new HashSet<Edge>();
    transNumSet = new HashSet<Integer>();
  };

  /** 
   * Input: string
   * Output: true or false indicating whether the input string 
   * can be converted to an integer
   * SideEffect: None
   * Author: Lingbo Li
   * Date: Dec 3, 2016
   */
  public static boolean isNumber(String string) {
    try {
      Long.parseLong(string);
    } catch (Exception e) {
      return false;
    }
    return true;
  }
  
  /** 
   * Input: transaction, variable, lockTables, transNumSet, edgeSet
   * Output: None
   * SideEffect: transNumSet would add a new distinct vertex representing the specified RW "transaction" 
   * and the distinct vertices representing other transactions waiting for this "transaction". 
   * edgeSet would add new edges representing wait-for relations from the given "transaction" to
   * the transactions which it waits for.
   * Author: Xiansha Jin
   * Date: Dec 4, 2016
   */ 
  void CollectEdgesAndNodesForRW(int transaction, int variable, List<LockInfo>[][] lockTables, Set<Integer> transNumSet, Set<Edge> edgeSet) {
    transNumSet.add(transaction);
    for (int i = 0; i < 10; i++) {
      int candidateToWait = -1;
      for (int k = 0; k < lockTables[i][variable - 1].size(); k++) {
        int currLockType = lockTables[i][variable - 1].get(k).getLockType();
        int currTransNum = lockTables[i][variable - 1].get(k).getTransactionNum();
        if (currLockType == 2) {
          candidateToWait = currTransNum;
          break;
        }        
      }
      if (candidateToWait != - 1 && candidateToWait != transaction) {
        transNumSet.add(candidateToWait);
        edgeSet.add(new Edge(transaction, candidateToWait));
      }
    }
  }

  /** 
   * Input: transaction, variable, lockTables, transNumSet, edgeSet
   * Output: None
   * SideEffect: transNumSet would add a new distinct vertex representing the specified Write "transaction" 
   * and the distinct vertices representing other transactions waiting for this "transaction". 
   * edgeSet would add new edges representing wait-for relations from the given "transaction" to 
   * the transactions which it waits for.
   * Author: Xiansha Jin
   * Date: Dec 4, 2016
   */
  void CollectEdgesAndNodesForWrite(int transaction, int variable, List<LockInfo>[][] lockTables, Set<Integer> transNumSet, Set<Edge> edgeSet) {
    transNumSet.add(transaction);
    for (int i = 0; i < 10; i++) {
      boolean needToWait = true;
      List<Integer> candidatesToWait = new ArrayList<Integer>();
      for (int k = 0; k < lockTables[i][variable - 1].size(); k++) {
        if (lockTables[i][variable - 1].get(k).getTransactionNum() == transaction) {
          needToWait = false;
          break;
        }
        candidatesToWait.add(lockTables[i][variable - 1].get(k).getTransactionNum());
      }
      if (needToWait) {
        for (Integer candidate : candidatesToWait) {
          transNumSet.add(candidate);
          edgeSet.add(new Edge(transaction, candidate));
        }
      }
    }
  }
  
  /**
   * Input: transNumSet, edgeSet
   * Output: a set of integers representing all detected cycles
   * SideEffect: None
   * Author: Xiansha Jin
   * Date: Dec 4, 2016
   */ 
  Set<Integer> constructGraphAndDetectCycle(Set<Integer>transNumSet, Set<Edge> edgeSet) {
    DirectedGraph<Integer, DefaultEdge> directedGraph = new DefaultDirectedGraph<Integer, DefaultEdge>(DefaultEdge.class);
    for (Integer transNum : transNumSet) {
      directedGraph.addVertex(transNum);
    }
    for (Edge edge : edgeSet) {
      directedGraph.addEdge(edge.getFrom(), edge.getTo());
    }
    CycleDetector<Integer, DefaultEdge> cd = new CycleDetector<Integer, DefaultEdge>(directedGraph);
    return cd.findCycles();
  }
  
  /** 
   * The main component of the Transaction Manager. 
   * This method simulates the transactions to run. In each iteration, it handles appending transactions 
   * if any and translates coming request (e.g., read and write request on variables to read and write request on copies), 
   * as well as process each request by communicating and working cooperatively with the Data Manager by accessing the methods in 
   * the DataManager class.
   * 
   * Input: None
   * Output: None
   * SideEffect: Update the internal data structures: transactionBufferContent, transactionBufferSequence, edgeSet,
   * and transNumSet
   * Author: Lingbo Li and Xiansha Jin
   * Date: Dec 5, 2016
   */ 
  void Schedule() {
    DataManager D = new DataManager();
    Scanner scanner = new Scanner(System.in);

    while (scanner.hasNextLine()) {
      String myString = scanner.nextLine().trim();
        if (myString.equals("exit")){
            return;
        }
      String[] operations = myString.split(";");
     
      
      System.out.println("time is: " + time);
 
      List<Integer> TobeDelete=new ArrayList<>();
      int c=0;
      int f=transactionBufferContent.size();
      
      while (c<f){
 
        String line = transactionBufferContent.get(c);

        if ((line.length() >= 11) && (line.substring(0, 3).equals("W(T"))
            && (line.indexOf(')') != -1)) {

          String[] tuple = line.split(",");
          if (tuple.length == 3) {
            tuple[0] = tuple[0].trim();
            tuple[1] = tuple[1].trim();
            tuple[2] = tuple[2].trim();
            int transactionNumber = -1;
            int variable = -1;

            if (isNumber(tuple[0].substring(3)) == true) {

              transactionNumber = Integer.valueOf(tuple[0].substring(3));

            }

            if ((tuple[1].substring(0, 1).equals("x"))
                && (isNumber(tuple[1].substring(1)) == true)) {

              variable = Integer.valueOf(tuple[1].substring(1));
            }

            if (transactionNumber != -1 && variable != -1) {

              if (isNumber(tuple[2].substring(0, tuple[2].indexOf(")"))) == true) {
                int value = Integer.valueOf(tuple[2].substring(0, tuple[2].indexOf(")")));

                boolean result = D.Write(variable, value, transactionNumber);
                if (result == true) {
                	TobeDelete.add(c);
                  Set<Edge> copyOfEdgeSet = new HashSet<Edge>();
                  for (Edge e : edgeSet) {
                    if (e.getFrom() != transactionNumber) {
                      copyOfEdgeSet.add(e);
                                       
                    }
                  }
                  boolean okToRemoveTransNum = true;
                  for (Edge e : edgeSet) {
                	  if (e.getFrom() == transactionNumber || e.getTo() == transactionNumber) {
                		  okToRemoveTransNum = false;
                		  break;
                	  }
                  }
                  if (okToRemoveTransNum) {
                	  transNumSet.remove(transactionNumber);
                  }
                  edgeSet = new HashSet<Edge>(copyOfEdgeSet);
                }
              }
              
            }
          }
        } else if ((line.length() >= 7) && (line.substring(0, 5).equals("end(T"))
            && (line.indexOf(')') != -1)) {

          String number = line.substring(line.indexOf('T') + 1, line.indexOf(')'));
          if (isNumber(number) == true) {
            int trueNumber = Integer.valueOf(number);

            int result = D.Commit(trueNumber);

            if (result == 1) {// this is only for read-only transactions
            	System.out.println("T"+trueNumber+" is committed");
            	TobeDelete.add(c);
            }
        }
        }
        else if (line.startsWith("R")) {
          int start = line.indexOf("(");
          int comma = line.indexOf(",");
          int end = line.indexOf(")");
          String trans = line.substring(start + 1, comma).trim();

          String repli = line.substring(comma + 1, end).trim();

          String transNum = trans.substring(1).trim();

          String repliNum = repli.substring(1).trim();
          if (!transNum.matches("\\d+")) {
            continue;
          }
          int transInt = Integer.parseInt(transNum);
          if (!repliNum.matches("\\d+")) {
            continue;
          }
          int repliInt = Integer.parseInt(repliNum);

          if (repliInt % 2 == 1) {
            int result = D.Read(1 + repliInt % 10, repliInt, transInt);

            if (result != 1000000) {
            System.out.println("T"+transInt+" reads the value of x"+repliInt+"="+result);
              TobeDelete.add(c);
              Set<Edge> copyOfEdgeSet = new HashSet<Edge>();
              for (Edge e : edgeSet) {
                if (e.getFrom() != transInt) {
                  copyOfEdgeSet.add(e);                    
                }
              }
              boolean okToRemoveTransNum = true;
              for (Edge e : edgeSet) {
            	  if (e.getFrom() == transInt || e.getTo() == transInt) {
            		  okToRemoveTransNum = false;
            		  break;
            	  }
              }
              if (okToRemoveTransNum) {
            	  transNumSet.remove(transInt);
              }
              edgeSet = new HashSet<Edge>(copyOfEdgeSet);
            }
            else{
          
            	}
            
          } else {
            int result = 1000000;
            int site = 1;
            while (result == 1000000 && site < 11) {
              result = D.Read(site, repliInt, transInt);
              site += 1;
            }
            if (result != 1000000) {
            System.out.println("T"+transInt+" reads the value of x"+repliInt+"="+result);
          
              TobeDelete.add(c);
              Set<Edge> copyOfEdgeSet = new HashSet<Edge>();
              for (Edge e : edgeSet) {
                if (e.getFrom() != transInt) {
                	copyOfEdgeSet.add(e);
                                  
                }
              }
              boolean okToRemoveTransNum = true;
              for (Edge e : edgeSet) {
            	  if (e.getFrom() == transInt || e.getTo() == transInt) {
            		  okToRemoveTransNum = false;
            		  break;
            	  }
              }
              if (okToRemoveTransNum) {
            	  transNumSet.remove(transInt);
              }
              edgeSet = new HashSet<Edge>(copyOfEdgeSet);
            }
            else{
    	
            }
          }
        }
   
        c+=1;
      } 
      List<String> copyContent=new ArrayList<String>();
      List<Integer> copySequence=new ArrayList<>();
      
      
      for (int i=0;i<transactionBufferSequence.size();i++){
    	  
    	  int mode=0;
    	  for (int j=0;j<TobeDelete.size();j++){
    		  if (i==TobeDelete.get(j)){
    			  mode=1;
    			  break;
    		  }   		  
    	  }
    	  if (mode==0){
    		  copySequence.add(transactionBufferSequence.get(i));
    		  copyContent.add(transactionBufferContent.get(i));
    	  }
      }
      transactionBufferSequence=new ArrayList(copySequence);
      transactionBufferContent=new ArrayList(copyContent);

      for (int i = 0; i < operations.length; i++) {
        operations[i] = operations[i].trim();
        String line = operations[i];

        if ((operations[i].length() >= 9) && (operations[i].substring(0, 7).equals("begin(T"))
            && (operations[i].indexOf(')') != -1)) {

          String number = operations[i].substring(operations[i].indexOf('T') + 1,
              operations[i].indexOf(')'));
          if (isNumber(number) == true) {
            int trueNumber = Integer.valueOf(number);
            D.Start(trueNumber, 1);

          }
        } 
  
        
    
        
        else if (line.startsWith("dump(")) {
          if (line.equals("dump()")) {
        
            D.dumpAll();
          } else if (isNumber(line.substring(5, line.indexOf(")"))) == true) {
            int site = Integer.valueOf(line.substring(5, line.indexOf(")")));

            D.dumpSite(site);
          } else if (line.startsWith("dump(x")
              && isNumber(line.substring(6, line.indexOf(")"))) == true) {
            int variable = Integer.valueOf(line.substring(6, line.indexOf(")")));

            D.dumpVariable(variable);
          }
        } else if ((operations[i].length() >= 7) && (operations[i].substring(0, 5).equals("end(T"))
            && (operations[i].indexOf(')') != -1)) {

          String number = operations[i].substring(operations[i].indexOf('T') + 1,
              operations[i].indexOf(')'));
          if (isNumber(number) == true) {
            int trueNumber = Integer.valueOf(number);
            int result = D.Commit(trueNumber);
            
            if (result == 0) {
              D.Abort(trueNumber);
  
              System.out.println("T"+trueNumber+" is aborted because not all sites have been up since it accessed them.");
            } else if (result == 1) {
            	  System.out.println("T"+trueNumber+" is committed");
            
            } else {// this case is only for read-only transaction     
              transactionBufferContent.add(line);
              transactionBufferSequence.add(trueNumber);
            }
          }
        } else if ((operations[i].length() >= 11) && (operations[i].substring(0, 3).equals("W(T"))
            && (operations[i].indexOf(')') != -1)) {

          String[] tuple = operations[i].split(",");
          if (tuple.length == 3) {
            tuple[0] = tuple[0].trim();
            tuple[1] = tuple[1].trim();
            tuple[2] = tuple[2].trim();
            int transactionNumber = -1;
            int variable = -1;

            if (isNumber(tuple[0].substring(3)) == true) {

              transactionNumber = Integer.valueOf(tuple[0].substring(3));

            }

            if ((tuple[1].substring(0, 1).equals("x"))
                && (isNumber(tuple[1].substring(1)) == true)) {

              variable = Integer.valueOf(tuple[1].substring(1));
            }

            if (transactionNumber != -1 && variable != -1) {

              if (isNumber(tuple[2].substring(0, tuple[2].indexOf(")"))) == true) {
                int value = Integer.valueOf(tuple[2].substring(0, tuple[2].indexOf(")")));

                boolean result = D.Write(variable, value, transactionNumber);
                if (result == false) {
                  transactionBufferContent.add(line);
                  transactionBufferSequence.add(transactionNumber);
                  CollectEdgesAndNodesForWrite(transactionNumber, variable, D.lockTables, transNumSet, edgeSet);
                }
              }
            }
          }
        }

        else if (line.startsWith("beginRO")) {
          int start = line.indexOf("(");
          int end = line.indexOf(")");
          String trans = line.substring(start + 1, end).trim();
          String transNum = trans.substring(1).trim();
          if (!transNum.matches("\\d+")) {
            continue;
          }
          int transInt = Integer.parseInt(transNum);
          D.Start(transInt, 0);
     
        } else if (line.startsWith("R")) {
          int start = line.indexOf("(");
          int comma = line.indexOf(",");
          int end = line.indexOf(")");
          String trans = line.substring(start + 1, comma).trim();

          String repli = line.substring(comma + 1, end).trim();

          String transNum = trans.substring(1).trim();

          String repliNum = repli.substring(1).trim();
          if (!transNum.matches("\\d+")) {
            continue;
          }
          int transInt = Integer.parseInt(transNum);
          if (!repliNum.matches("\\d+")) {
            continue;
          }
          int repliInt = Integer.parseInt(repliNum);

          if (repliInt % 2 == 1) {
            int result = D.Read(1 + repliInt % 10, repliInt, transInt);
            if (result == 1000000) {
              transactionBufferContent.add(line);
              transactionBufferSequence.add(transInt);
              CollectEdgesAndNodesForWrite(transInt, repliInt, D.lockTables, transNumSet, edgeSet);

            } else {
            	  System.out.println("T"+transInt+" reads the value of x"+repliInt+"="+result);

            }
          } else {
            int result = 1000000;
            int site = 1;
            while (result == 1000000 && site < 11) {
              result = D.Read(site, repliInt, transInt);
              site += 1;
            }
            if (result == 1000000) {
              transactionBufferContent.add(line);
              transactionBufferSequence.add(transInt);
              CollectEdgesAndNodesForWrite(transInt, repliInt, D.lockTables, transNumSet, edgeSet);

            } else {
            	  System.out.println("T"+transInt+" reads the value of x"+repliInt+"="+result);
          
            }
          }
        } else if (line.startsWith("fail")) {
          int start = line.indexOf("(");
          int end = line.indexOf(")");
          String siteNum = line.substring(start + 1, end).trim();
          if (!siteNum.matches("\\d+")) {
            continue;
          }
          int siteInt = Integer.parseInt(siteNum);
         
          D.fail(siteInt);
        } else if (line.startsWith("recover")) {
          int start = line.indexOf("(");
          int end = line.indexOf(")");
          String siteNum = line.substring(start + 1, end).trim();

          if (!siteNum.matches("\\d+")) {
            continue;
          }
          int siteInt = Integer.parseInt(siteNum);
          D.recover(siteInt);
        }
      }
      Set<Integer> transInCycle = constructGraphAndDetectCycle(transNumSet, edgeSet);

      int candidate;
    
      while(transInCycle.size()>0){
    	  System.out.println("Detect dead lock: cycles are formed by " + transInCycle);
    	  candidate= D.youngestTransaction(transInCycle);
    	  D.Abort(candidate);
    	  System.out.println("transaction "+candidate+" is aborted because of deadlock.");
    	  transNumSet.remove(candidate);
    	  Set<Edge> copyOfEdgeSet = new HashSet<Edge>();
    	  for (Edge e : edgeSet) {
    		  if (e.getFrom() != candidate && e.getTo() != candidate) {
    			  copyOfEdgeSet.add(e);    			 
    		  }
    	  }
    	  for (int h=0;h<transactionBufferSequence.size();h++){
    		  if (transactionBufferSequence.get(h)==candidate){
    			  transactionBufferSequence.remove(h);
    			  transactionBufferContent.remove(h);
    			  break;
    		  }
    	  } 
    	  edgeSet = new HashSet<Edge>(copyOfEdgeSet);
    	  transInCycle = constructGraphAndDetectCycle(transNumSet, edgeSet);    	  
      } 
      time += 1;
    }
  };
}
