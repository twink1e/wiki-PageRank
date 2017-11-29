import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

class DataProcessor {
  private static final String RAW = "indexbi.bin", LINK = "links.bin", OFFSET = "offset.bin", 
    SORT = "sorted.bin", FINAL = "final.bin", RANK_OFFSET = "rank_offset.txt";
  private static HashMap<Integer, Integer> off2idx;
  private static long start, end;
  private static int pageNum;

  private static int getLittleEndian(byte[] b) {
    return ((b[3]&0xff)<<24)+((b[2]&0xff)<<16)+((b[1]&0xff)<<8)+(b[0]&0xff);
  }

  private static void mapOffset2Idx() {
    try {
      FileChannel in = new FileInputStream(RAW).getChannel();
      MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());

      FileOutputStream fos = new FileOutputStream(OFFSET);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);

      byte[] buffer = new byte[4];
      mbb.position(4); //skip version number
      mbb.get(buffer);
      pageNum = getLittleEndian(buffer);
      int offset = 16; //first page
      int linkNum;

      for (int i=0; i<pageNum; i++) {
        off2idx.put(offset, i);
        dos.writeInt(offset);
        mbb.position(offset + 4); //number of links is next
        mbb.get(buffer); 
        linkNum = getLittleEndian(buffer);
        offset += (4 + linkNum) * 4; // 4 ints of meta data + links
      }
      if (offset == in.size()) System.out.println("Map offset success!"); // check if offset tallies
      else System.out.println("Map offset failed " + offset);
      in.close();
      fos.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private static void sortLinks() {
    try {
      FileChannel in = new FileInputStream(LINK).getChannel();
      MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());

      FileOutputStream fos = new FileOutputStream(SORT);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);
      
      int numPage = mbb.getInt();
      dos.writeInt(numPage);
      int numLink, currPage;
      int[] outLinks;
      for (int i=0; i<numPage; i++) {
        currPage = mbb.getInt();
        if (currPage != i) {
          System.out.println("Sort link failure i = " + i + " page = " +currPage);
          in.close();
          dos.close();
          System.exit(-1);
        }
        dos.writeInt(currPage);
        numLink = mbb.getInt();
        dos.writeInt(numLink);
        outLinks = new int[numLink];
        for (int j=0; j<numLink; j++) outLinks[j] = mbb.getInt();
        Arrays.sort(outLinks);
        for (int j=0; j<numLink; j++) dos.writeInt(outLinks[j]);
      }
      in.close();
      dos.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

private static void indexLinks() {
  try {
    FileChannel in = new FileInputStream(RAW).getChannel();
    MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());

    FileOutputStream fos = new FileOutputStream(LINK);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    DataOutputStream dos = new DataOutputStream(bos);

    dos.writeInt(pageNum);
      mbb.position(16); //start of first page
      byte[] buffer = new byte[4];
      int linkNum, offset, idx;

      for (int i=0; i<pageNum; i++) {
        dos.writeInt(i);
        mbb.position(mbb.position() + 4); // skip the 0
        mbb.get(buffer); 
        linkNum = getLittleEndian(buffer);
        dos.writeInt(linkNum);
        mbb.position(mbb.position() + 8); // skip B & M

        for (int j=0; j<linkNum; j++) {
          mbb.get(buffer); 
          offset = getLittleEndian(buffer);
          idx = off2idx.get(offset);
          dos.writeInt(idx);
        }
      }
      if (mbb.position() == in.size()) System.out.println("Index links success!");
      else System.out.println("Index links failed " + mbb.position());
      in.close();
      dos.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  /** For debug */
  private static void readBinaryMapped(String file, int num) {
    try {
      FileChannel in = new FileInputStream(file).getChannel();
      MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());
      for (int i=0; i<num; i++) mbb.getInt(4*i);
      in.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private static void blkLinks() {
    try {
      FileChannel in = new FileInputStream(SORT).getChannel();
      MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());

      FileOutputStream[] fos = new FileOutputStream[9];
      BufferedOutputStream[] bos = new BufferedOutputStream[9];
      DataOutputStream[] dos = new DataOutputStream[9];
      ArrayList<Integer>[] outLinks = new ArrayList[9];

      for (int i=0; i<9; i++) {
        fos[i] = new FileOutputStream(i + ".bin");
        bos[i] = new BufferedOutputStream(fos[i]);
        dos[i] = new DataOutputStream(bos[i]);
      }
      
      int numPage = mbb.getInt();
      for (int i=0; i<9; i++) {
        dos[i].writeInt(numPage);
      }
      int numLink, currPage;

      for (int i=0; i<numPage; i++) {
        currPage = mbb.getInt();
        if (currPage != i) {
          System.out.println("blk link failure i = " + i + " page = " +currPage);
          closeAndExit(in, dos);
        }
        for (int j=0; j<9; j++) {
          dos[j].writeInt(currPage);
        }
        numLink = mbb.getInt();
        int dstPage;
        int fileIdx = 0, accLink = 0;
        for (int j=0; j<9; j++) outLinks[j] = new ArrayList<Integer>();

        for (int j=0; j<numLink; j++) {
          dstPage = mbb.getInt();
          outLinks[dstPage/1000000].add(dstPage);
        }

        for (int j=0; j<9; j++) {
          accLink += outLinks[j].size();
          dos[j].writeInt(outLinks[j].size());
          for (int l : outLinks[j]) dos[j].writeInt(l);
        }

        if (accLink != numLink) {
          System.out.println("blk link failure currPage = " + currPage + " numLink = " + numLink + " accLink " + accLink);
          closeAndExit(in, dos);
        }
      }
      in.close();
      for (int i=0; i<9; i++) {
        dos[i].close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void closeAndExit(FileChannel in, DataOutputStream[] dos) {
    try {
      in.close();
      for (int i=0; i<9; i++) {
        dos[i].close();
      }
      System.exit(-1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static class Pair implements Comparable<Pair> {
    int p;
    double s;
    public Pair(int p, double s) {
      this.p = p;
      this.s = s;
    }
    @Override
    public int compareTo(Pair o) {
      if (s > o.s) return -1;
      if (s == o.s) return 0;
      else return 1;
    }
  }
  private static void getTop() {
    try {
      FileChannel in = new FileInputStream(FINAL).getChannel();
      MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());
      int numPage = (int)in.size() / 8;
      Pair[] scores = new Pair[numPage];
      int maxPage = -1, page = -1;
      double maxScore = 0, score;
      for (int i=0; i<numPage; i++) {
        score = mbb.getDouble();
        scores[i] = new Pair(i, score);
        if (score > maxScore) {
          maxScore = score;
          maxPage = i;
        }
      }
      in.close();
      Arrays.sort(scores);
      if (maxScore != scores[0].s) {
        System.out.println("max score " + maxScore + "scores " + scores[0].s);
        System.exit(-1);
      }

      in = new FileInputStream(OFFSET).getChannel();
      mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());
      FileWriter fw = new FileWriter(RANK_OFFSET);

      for (int i=0; i<1000; i++) 
        fw.write("SELECT title from pages where offset = " + mbb.getInt(scores[i].p * 4) + ";\n");
      in.close();
      fw.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main (String[] args) {
    /*off2idx = new HashMap<Integer, Integer>();
    start = System.currentTimeMillis();
    mapOffset2Idx();
    end = System.currentTimeMillis();
    System.out.println("Map offset took "+ (end-start) + " ms");
    start = System.currentTimeMillis();
    indexLinks();
    end = System.currentTimeMillis();
    System.out.println("Index links took "+ (end-start) + " ms");
    sortLinks();*/
    start = System.currentTimeMillis();
    blkLinks();
    end = System.currentTimeMillis();
    System.out.println("Blk links took "+ (end-start) + " ms");

    //getTop();
  }
}