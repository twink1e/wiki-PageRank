import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

class DataProcessor {
  private static final String RAW = "indexbi.bin", LINK = "links.bin", OFFSET = "offset.bin";
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
      fos.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private static void readBinaryBuffered(String file, int num) {
    try {
      FileInputStream in = new FileInputStream(file);
      BufferedInputStream fis = new BufferedInputStream(in);
      DataInputStream dis = new DataInputStream(fis);
      for (int i=0; i<num; i++) dis.readInt(); //System.out.println(dis.readInt());
      dis.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }
  private static void readBinaryMapped(String file, int num) {
    try {
      FileChannel in = new FileInputStream(file).getChannel();
      MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, in.size());
      for (int i=0; i<num; i++) mbb.getInt(4*i); //System.out.println(dis.readInt());
      in.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public static void main (String[] args) {
    off2idx = new HashMap<Integer, Integer>();
    start = System.currentTimeMillis();
    mapOffset2Idx();
    end = System.currentTimeMillis();
    System.out.println("Map offset took "+ (end-start) + " ms");
    start = System.currentTimeMillis();
    indexLinks();
    end = System.currentTimeMillis();
    System.out.println("Index links took "+ (end-start) + " ms");
  }
}