//Grant Edwards 5/16/2018

import java.util.*;
import java.lang.*;
import java.io.*;

public class JoinTester{
   public static void main(String[] args){
      
      //Take Params
      int arg0 = 2,arg1 = 2;
      if(args.length>1) {
         try {
            arg0 = Integer.parseInt(args[0]);
            arg1 = Integer.parseInt(args[1]);
         }
         catch(NumberFormatException nfe) {
            System.out.println("Arguments must be integers");
            throw(nfe);
         }
      }
      else
         throw(new IllegalArgumentException( "insufficient arguments found" ));
         
      //Find Files  
      Scanner takesscan=null,studentscan = null;         
      try {
         studentscan = new Scanner(new File("student.txt"));
         takesscan   = new Scanner(new File("takes.txt"));
      }
      catch(FileNotFoundException fnfe){
         System.out.println("File(s) not located");
      }
      
      //Build and Sort
      SortMergeJoin studentSMJ = new SortMergeJoin(arg0,arg1,"student");
      SortMergeJoin takesSMJ   = new SortMergeJoin(arg0,arg1,"takes");
      
      studentSMJ.sort(studentscan);
      takesSMJ.sort(takesscan);
      
      //Merge
      studentSMJ.merge(studentSMJ.getPartitions());
      takesSMJ.merge(takesSMJ.getPartitions());
      
      //Join
         //Find Files
      Scanner sortedTakesScan=null,sortedStudentScan = null;         
      try {
         sortedStudentScan = new Scanner(new File("sorted_student.txt"));
         sortedTakesScan   = new Scanner(new File("sorted_takes.txt"));
      }
      catch(FileNotFoundException fnfe){
         System.out.println("File(s) not located");
      }
         //Join
      SortMergeJoin joinSMJ = new SortMergeJoin(arg0,arg1,"join");
      joinSMJ.join(sortedStudentScan , sortedTakesScan);
   }
}