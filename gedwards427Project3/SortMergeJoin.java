//Grant Edwards 5/16/2018

import java.util.*;
import java.io.*;

public class SortMergeJoin {
   private ArrayList<Partition> partitions = null;
   private int _partitionSize;
   private int _blockSize;
   private String prefix;
   private Writer currentOut = null;
   private Writer joinOut = null;
   
   public SortMergeJoin(int block , int partition , String fileprefix) {
      _blockSize     = block;
      _partitionSize = partition;
      prefix = fileprefix;
   }
   
   /*
   * Sort
   */
   public void sort(Scanner fin) {
      //Build
      partitions = new ArrayList<Partition>();
      
      Partition buildTemp = new Partition(_blockSize , _partitionSize);
      partitions.add(buildTemp);
      
      String line;
      while(fin.hasNext()) {
         line = fin.nextLine();
         if(buildTemp.isFull()) {
            buildTemp = new Partition(_partitionSize , _blockSize);
            partitions.add(buildTemp);
         }
         buildTemp.add(line);
      }
      
      //Sort
      for(Partition sortTemp : partitions)
         sortTemp.sort();
         
      //Write
      print();
   }
   
   /*
   * Merge
   */
   public void merge(ArrayList<Partition> newPartitions) {
      partitions = newPartitions;
      
      //Merge
      ArrayList<Partition> merged = new ArrayList<Partition>();
      
      Partition mergeTemp = new Partition(_blockSize , _partitionSize);
      merged.add(mergeTemp);
   
      boolean hasNext = true;
      Partition savePoint = new Partition();//never accessed, replaced 
      while(hasNext)
      {
         hasNext = false;
         Entry min = new Entry("999999\tzz");
         for(Partition search : partitions)
         {
            if(search.topPeek()!=null)
            {
               hasNext = true;
               if(min.compareTo(search.topPeek()) > 0)
               {
                  min = search.topPeek();
                  savePoint = search;
               }
            }
         }
         if(mergeTemp.isFull()) {
            mergeTemp = new Partition(_partitionSize , _blockSize);
            merged.add(mergeTemp);
         }
         if(min.compareTo(new Entry("999999\tzz")) != 0)
            mergeTemp.add(savePoint.topTake());
      }
      
   /* for(Partition P : merged)
         for(Entry E : P.data)
            System.out.println(","+E.data);
   */
      mergePrint(merged);
   }
   
   /*
   * Join
   */
   public void join(Scanner L, Scanner R) {
      makeJoinFile();
   
      dataScan leftIn  = new dataScan(L);
      dataScan rightIn = new dataScan(R);
      
      boolean leftHasNext = true, rightHasNext = true;
      
      while(leftIn.hasNext() || rightIn.hasNext() && (leftHasNext || rightHasNext)) {
         Entry match = leftIn.get();
         if(leftIn.tryNext(match) || rightIn.tryNext(match)) {
            printLine(joinPair(leftIn.get() , rightIn.get()).data);
            if(leftIn.tryNext(match))
               leftIn.read();
            if(rightIn.tryNext(match))
               rightIn.read();
         }
         else {
            leftHasNext  = leftIn.read();
            rightHasNext = rightIn.read();
         }
      }
      
      closeCurrent();
   }
   
   /*
   *  Simple helper method for creating a new tuple by 
   *  joining two existing ones.
   */
   private Entry joinPair(Entry left, Entry right) {
      return new Entry(left,right);
   }
   
   private class dataScan {
      private Scanner myScanner;
      private Entry   line = new Entry("0\ta");
      private Entry   prev = new Entry("0\ta");
      
      dataScan(Scanner scan) {
         myScanner = scan;
         prev = new Entry(myScanner.nextLine());
         line = new Entry(myScanner.nextLine());
      }
      
      public Entry get() {
         return prev;
      }
      
      public boolean read() {
         if(!myScanner.hasNext())
            return false;
      
         prev = line;
         line = new Entry(myScanner.nextLine());
         return true;
      }
      
      public Entry peek() {
         return line;
      }
      
      public boolean hasNext() {
         return myScanner.hasNext();
      }
      
      public boolean tryNext(Entry match) {
         return(this.peek().matches(match));
      }
   }
      
   /*
   *Creates the smj_name_i.txt files, the currentOut Writer,
   *  and calls print on each partition.
   *This method is for use by the SORT portion
   */
   private void print() {
      int i = 0;
      for(Partition P : partitions) {
      
         try{currentOut = new BufferedWriter(
               new OutputStreamWriter(
               new FileOutputStream("./output/smj_" + this.prefix + "_" + i + ".txt"), "utf-8"));}
         catch(FileNotFoundException fe){}   
         catch(UnsupportedEncodingException uee){} 
      
         i++;
         P.print();
         
         try{currentOut.close();}
         catch(IOException ioe){}
      }
   }
   
   /*
   *Creates the sorted_name.txt files and writes
   *  the content of the passed in Partition to 
   *  them. 
   *This method is for use by the MERGE portion
   */
   private void mergePrint(ArrayList<Partition> merged)
   {
      Writer mergeOut = null;
      try{mergeOut = new BufferedWriter(
               new OutputStreamWriter(
               new FileOutputStream("sorted_" + this.prefix + ".txt"), "utf-8"));}
      catch(FileNotFoundException fe){}   
      catch(UnsupportedEncodingException uee){} 
      
      for(Partition P : merged)
         for(Entry E : P.data)
            try{mergeOut.write(E.data+"\n");}
            catch(IOException ioe){}
         
      try{mergeOut.close();}
      catch(IOException ioe){}
   }
   
   /*
   *Creates the sorted_name.txt files and writes
   *  the content of the passed in Partition to 
   *  them. 
   *This method is for use by the MERGE portion
   */
   private void makeJoinFile() {
      try{currentOut = new BufferedWriter(
               new OutputStreamWriter(
               new FileOutputStream("smj.txt"), "utf-8"));}
      catch(FileNotFoundException fe){}   
      catch(UnsupportedEncodingException uee){} 
   }
   
   private void closeCurrent() {
      try{currentOut.close();}
      catch(IOException ioe){}
   }
      
   //Prints to the currentOut Writer. Referenced extensively
   private void printLine(String line) {
      //System.out.println(line);
      try{currentOut.write(line+"\n");}
      catch(IOException ioe){}
   }
   
   //simple get method
   public ArrayList<Partition> getPartitions() {
      return this.partitions;
   }

   /*
   *Internal class Partition - represents 1 partition.
   *Stores maxEntries records on blockMaxSize blocks, where 
   *  maxEntries is the maximum number of recoords for the
   *  partition: block size * number of blocks
   */
   private class Partition {
      private int                blockMaxSize;
      private boolean            isFull         = false;
      private ArrayList<Entry>   data           = null;
      private ArrayList<Integer> divs           = null;
      private int                maxEntries;
      private int                top            = 0;
   
      private Partition(){}
   
      private Partition(int blockSize , int partitionSize) {
         if(partitionSize >= 1) {
            blockMaxSize = blockSize;
            maxEntries  = blockSize * partitionSize;
            data        = new ArrayList<Entry>();
            divs        = new ArrayList<Integer>();
            divs.add(0);
         }
      }
      
      public Entry topPeek()
      {
         if(top >= data.size())
            return null;
         return data.get(top);
      }
      
      public Entry topTake()
      {
         if(top >= data.size())
            return null;
         Entry ret = data.get(top);
         top++;
         return ret;
      }
      
      public boolean isFull() {
         return isFull;
      }
   
      public void print() {
         int i =0;
         for(Integer blockstart : divs) {
            int blockend = blockstart+blockMaxSize;
            if(blockend > data.size())
               blockend = data.size();
            for(Entry part : data.subList((int)blockstart,blockend))
               printLine(part.data);
         }
      }
      
      public void add(String adding)
      {
         this.add(new Entry(adding));
      }
      
      public void add(Entry adding) {
         if(data.size() >= maxEntries-1) {
            data.add(adding);
            this.isFull = true;
         }
         else {
            data.add(adding);
            if((data.size() - divs.get(divs.size()-1)) >= (blockMaxSize))
               divs.add(data.size());
         }
      }
      
      public void sort() {
         Collections.sort(data);
      }
   }
   
   private class Entry implements Comparator<Entry>, Comparable<Entry> {
      public String data;
      private int ID;
      
      Entry() {}
      
      public Entry(String in) {
         data = in;
         ID = Integer.parseInt(in.split("\t")[0]);
      }
      
      //Creates a new Entry by joining two old ones
      //Assumes join key is ID, first item
      public Entry(Entry left,Entry right) {
         ID = Integer.parseInt(left.data.split("\t")[0]);
         data = left.data;
         
         String[] rightItems = right.data.split("\t");
         int i =0;
         for(String col : rightItems){
            if(i>0)
               data += "\t"+col;
            i++;
         }
      }
      
      public int compareTo(Entry e) {
         return (this.ID)-(e.ID);
      }
   
      public int compare(Entry left, Entry right) {
         return left.ID - right.ID;
      }
      
      public boolean matches(Entry other) {
         return ID == other.getID();
      }
      
      public int getID() {
         return this.ID;
      }
   }
}