//Grant Edwards 5/16/2018

import java.util.*;

public class SortMergeJoin{
   private ArrayList<Partition> _partitions = null;
   private int _partitionSize;
   private int _blockSize;
   private int _numPartitions;
   
   public SortMergeJoin(int partition , int block){
      _partitionSize = partition;
      _blockSize     = block;
   }
   
   public void sort(Scanner fin){
   //build partitions
      _partitions = new ArrayList<Partition>();
   
      Partition temp;
      String line;
      temp = new Partition(_partitionSize , _blockSize);
      _partitions.add(temp);
      while(fin.hasNext()){
         line = fin.nextLine();
      //System.out.println(line);
         if(temp.isFull()){
            temp = new Partition(_partitionSize , _blockSize);
            _partitions.add(temp);
         }
         temp.add(line);
      }
   //sort partitions
      for(Partition p : _partitions)
         p.sort();
   }
   
   public void print()
   {
      int i = 0;
      for(Partition P : _partitions){
         System.out.println("p["+i+"]----------");
         i++;
         P.print();
      }
   }

   /*
   *Internal class Partition - represents 1 partition.
   *Stores _partitionMaxSize blocks, where _partitionMaxSize 
   *  is the perscribed number of blocks a partition should
   *  carry.
   */
   private class Partition{
      private ArrayList<Block> _blocks = null;
      private int _partitionMaxSize;
      private int _blockMaxSize;
      private boolean _isFull = false;
      private Block _temp;
   
      private Partition(int partitionSize , int blockSize){
         if(partitionSize >= 1){      
            _blocks           = new ArrayList<Block>(partitionSize);
            _partitionMaxSize = partitionSize;
            _blockMaxSize     = blockSize;
            _temp             = new Block(_blockMaxSize);
            _blocks.add(_temp);
         }
      }
   
      public boolean append(Block adding){
         if(_blocks.size() < _partitionMaxSize){
            _blocks.add(adding);
            return true;
         }
         else{
            _isFull = true;
            return false;
         }
      }
      
      public boolean isFull(){
         return _isFull;
      }
   
      public void print(){
         int i = 0;
         for(Block item : _blocks)
         {
            System.out.println("  b["+i+"]--------");
            i++;
            item.print();
         }
      }
      
      public void echo(String e)
      {
         System.out.println("p:"+e);
      }
      
      public void add(String adding){
      //    System.out.println(adding);
      //  if(_blocks.size() > _partitionMaxSize)
      //      return;
         if(_temp.isFull()){
            if(_blocks.size() < _partitionMaxSize){
               _temp = new Block(_blockMaxSize);
               _blocks.add(_temp);
            }
            else{
               this._isFull = true;
               return;
            }
         }
         _temp.add(adding);
      }
      
      public void sort()
      {
         ArrayList<String> full = new ArrayList<String>();
      
         for(Block b : _blocks)
            full.add(b._entries);
      }
   }
   
   /*
   *Internal class Block - represents 1 block.
   *Stores _blockMaxSize strings, each representing a line of 
   *  data where _blockMaxSize is the perscribed block size.
   */
   private class Block{
      public ArrayList<String> _entries = null;
      private int _blockMaxSize;
      private boolean _isFull = false;
   
      private Block(int blockSize){
         if(blockSize >= 1){      
            _entries       = new ArrayList<String>(blockSize);
            _blockMaxSize  = blockSize;
         }
      }
   
      public boolean append(String adding){
         if(_entries.size() < _blockMaxSize){
            _entries.add(adding);
            return true;
         }
         else
            return false;
      }
      
      public boolean isFull(){
         return _isFull;
      }
      
      public boolean add(String adding){
         if(_isFull)
            return false;
         else{
            _entries.add(adding);
            //System.out.println(_entries.size() +" , "+ _blockMaxSize);
            if(_entries.size() >= _blockMaxSize)
               _isFull = true;
            return true;
         }
      }
   
      public void print(){
         for(String item : _entries)
            System.out.println("    "+item);
            
      }
   }
}