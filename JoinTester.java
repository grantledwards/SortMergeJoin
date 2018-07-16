//Grant Edwards 5/16/2018

import java.util.*;

public class JoinTester{
   public static void main(String[] args){
      SortMergeJoin SMJ = new SortMergeJoin(3,7);
      
      String line = "";
      
      Random rand = new Random();
      
      for(int i=50;i>0;i--)
         line += rand.nextInt(100)+" \t....\n";
      Scanner fin = new Scanner(line);
      
      SMJ.sort(fin);
      
      SMJ.print();
   }
}