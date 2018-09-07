//Grant Edwards 5/16/2018

import java.util.*;
import java.lang.*;
import java.io.*;

public class JoinTester{
   public static void main(String[] args){
      
      int arg0 = 2,arg1 = 2;
      //Take Params
      /*
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
         throw(new IllegalArgumentException( "Insufficient arguments found. Please enter a pair of integers." ));*/
         
         
      System.out.println("This program takes a pair of preexisting files and partitions them into individual \nportions and sorts each of these using Java sorting functions. Each is saved as \nits own file in the directory \"output\". These files are then read back and read and \nsorted into one file for each original file. A natural join operation is then \nperformed on these two files, producing the final product, smj.txt. \nIn the starting files, each line represents a record, and the entries are whitespace \nseparated.\n");
              
      Scanner inputScanner = new Scanner(System.in);
      
      int temp;
      System.out.println("Please enter the block size. \n(An integer representing the number of records that can be stored in each block)");
      temp = inputScanner.nextInt();
      if(temp>0 && temp<5000)
         arg0 = temp;
         
      System.out.println("Please enter the memory size. \n(An integer representing the number of available memory blocks)");
      temp = inputScanner.nextInt();
      if(temp>0 && temp<5000)
         arg1 = temp;
         
      inputScanner.nextLine();
      
      System.out.println("Enter \"student.txt\" and \"takes.txt\" to use the example demonstration files.");
      
      String leftFile = getFilename(inputScanner);
      String rightFile = getFilename(inputScanner);
               
      //Find Files  
      System.out.println("Finding files\n");
      Scanner leftScan=null,rightScan = null;         
      try {
         leftScan    = new Scanner(new File(leftFile));
         rightScan   = new Scanner(new File(rightFile));
      }
      catch(FileNotFoundException fnfe){
         System.out.println("File(s) not located");
      }
      
      //Build and Sort
      System.out.println("Building objects\n");
      SortMergeJoin leftSMJ   = new SortMergeJoin(arg0,arg1,"left");
      SortMergeJoin rightSMJ  = new SortMergeJoin(arg0,arg1,"right");
      
      System.out.println("Sorting...\n");
      leftSMJ.sort(leftScan);
      rightSMJ.sort(rightScan);
      
      //Merge
      System.out.println("Partitioning...\n");
      leftSMJ.merge(leftSMJ.getPartitions());
      rightSMJ.merge(rightSMJ.getPartitions());
      
      //Join
         //Find Files
      Scanner sortedLeftScan=null,sortedRightScan = null;         
      try {
         sortedLeftScan = new Scanner(new File("sorted_"+leftFile));//student.txt"));
         sortedRightScan   = new Scanner(new File("sorted_"+rightFile));//takes.txt"));
      }
      catch(FileNotFoundException fnfe){
         System.out.println("File(s) not located");
      }
      //Join
      System.out.println("Joining...\n");
      SortMergeJoin joinSMJ = new SortMergeJoin(arg0,arg1,"join");
      joinSMJ.join(sortedLeftScan , sortedRightScan);
      
      System.out.println("Complete. smj.txt now contains sorted data.");
   }
   
   private static String getFilename(Scanner in){
      System.out.println("Please enter a filename");
      return in.nextLine();
   }
}
